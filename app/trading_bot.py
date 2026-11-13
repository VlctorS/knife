import asyncio
import datetime
import logging
import time
import json

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine, Settings
from .state import state, Alert, classify_event, moving_average
from .config import settings

log = logging.getLogger(__name__)

MIN_POS_SIZE = 6.0
VOLUME_LOOKBACK = 20


def _get_upsert(model, values, pk_col):
    is_pg = engine.url.drivername.startswith('postgresql')
    if is_pg:
        return pg_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})
    return sqlite_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})


def _calc_avg_volume(candles, lookback=20):
    if len(candles) < lookback + 1:
        return 0.0
    prev = candles[-(lookback + 1):-1]
    vols = [float(k[5]) for k in prev if float(k[5]) > 0]
    return sum(vols) / len(vols) if vols else 0.0


def _pick(cfg_val, global_val, fallback):
    """Приоритет настройки: конфиг символа -> общие настройки -> дефолт."""
    if cfg_val and cfg_val > 0:
        return cfg_val
    if global_val and global_val > 0:
        return global_val
    return fallback


class TradingBot:
    def __init__(self, rest):
        self.rest = rest
        self._last_scan_ts = 0
        self._seen_candles = set()

    async def run_history_scan(self):
        log.info('Starting history scan for %d symbols...', len(state.symbols))
        ok = 0
        async with SessionLocal() as session:
            for sym, st in state.symbols.items():
                try:
                    kl = await self.rest.get_kline_1h(sym, limit=720)
                    if len(kl) < VOLUME_LOOKBACK + 2:
                        continue
                    
                    all_vols = [float(k[5]) for k in kl if float(k[5]) > 0]
                    if not all_vols:
                        continue
                    global_avg_vol = sum(all_vols) / len(all_vols)
                    
                    prostrels = []
                    for k in kl:
                        high = float(k[2])
                        low = float(k[3])
                        vol = float(k[5])
                        if low <= 0 or vol <= 0:
                            continue
                        if vol < global_avg_vol:
                            continue
                        prostrels.append((high - low) / low * 100)
                    
                    if not prostrels:
                        continue
                    
                    avg_p = sum(prostrels) / len(prostrels)
                    auto_entry = max(3.0, avg_p * 0.85)
                    
                    values = dict(
                        symbol=sym, max_leverage=st.max_leverage,
                        avg_prostrel_pct=avg_p, max_prostrel_pct=max(prostrels),
                        freq_per_day=len(prostrels) / 30.0,
                        entry_long_pct=auto_entry, entry_short_pct=auto_entry,
                        updated_at=datetime.datetime.utcnow(),
                    )
                    stmt = _get_upsert(SymbolStat, values, 'symbol')
                    await session.execute(stmt)
                    
                    res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                    if not res.scalars().first():
                        session.add(SymbolConfig(symbol=sym))
                        
                    ok += 1
                    if ok % 20 == 0:
                        await session.commit()
                        log.info('History scan progress: %d / %d', ok, len(state.symbols))
                        await asyncio.sleep(1.0)
                    else:
                        await asyncio.sleep(0.3)
                except Exception as e:
                    log.warning('History scan failed for %s: %r', sym, e)
            await session.commit()
        log.info('History scan complete. Symbols in DB: %d', ok)

    async def check_for_entries(self):
        now = time.time()
        if now - self._last_scan_ts < 60:
            return
        self._last_scan_ts = now

        async with SessionLocal() as session:
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            open_syms = {t.symbol for t in result.scalars().all()}

            symbols_to_check = [s for s in state.symbols.keys() if s not in open_syms]
            if not symbols_to_check:
                return

            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()

            intervals = ['240', 'D']  # ловим ножи на 4ч и 1д
            
            for sym in symbols_to_check:
                try:
                    cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                    cfg = cfg_res.scalars().first()
                    stat = await session.get(SymbolStat, sym)
                    st = state.symbols.get(sym)
                    if not st or st.ma is None or st.last_price <= 0:
                        continue

                    if cfg and cfg.entry_pct > 0:
                        threshold = cfg.entry_pct
                    elif gs.entry_pct > 0:
                        threshold = gs.entry_pct
                    else:
                        threshold = (stat.avg_prostrel_pct * 0.85) if stat and stat.avg_prostrel_pct > 0 else settings.min_knife_move_pct
                        threshold = max(threshold, settings.min_knife_move_pct)

                    current_price = st.last_price

                    # Сначала младший ТФ (4h), затем якорь (1d). Вход только если
                    # нож подтверждён, а якорный ТФ не продолжает движение.
                    kline_cache = {}
                    for tf in intervals:
                        kl = await self.rest.get_kline(sym, interval=tf, limit=22)
                        kline_cache[tf] = kl
                        if len(kl) < VOLUME_LOOKBACK + 2:
                            continue

                        last_closed = kl[-2]
                        open_price = float(last_closed[1])
                        close_price_candle = float(last_closed[4])
                        volume = float(last_closed[5])

                        if open_price <= 0 or volume <= 0:
                            continue

                        avg_vol = _calc_avg_volume(kl, VOLUME_LOOKBACK)
                        if avg_vol <= 0 or volume < avg_vol:
                            continue

                        change_pct = (close_price_candle - open_price) / open_price * 100
                        if abs(change_pct) < threshold:
                            continue

                        candle_id = f"{sym}_{tf}_{last_closed[0]}"
                        if candle_id in self._seen_candles:
                            continue

                        knife_tf = kline_cache.get(tf) or kl
                        anchor_tf = kline_cache.get('D') if tf != 'D' else None
                        if tf == 'D':
                            # Для дневного ножа якорем служит недельный контекст из 4h-окна
                            anchor_tf = kline_cache.get('240')

                        ma_knife = moving_average(knife_tf, min(settings.ma_period, 20))
                        ma_anchor = moving_average(anchor_tf, min(settings.ma_period, 20)) if anchor_tf else None

                        ev = classify_event(knife_tf, anchor_tf, current_price, ma_knife, ma_anchor)
                        min_conf = _pick(cfg.min_confidence if cfg else 0, gs.min_confidence if gs else 0, 0.4)
                        tf_label = {'60': '1h', '240': '4h', 'D': '1d'}.get(tf, tf)
                        
                        # Вспомогательная функция для определения направления для алерта
                        side_guess = 'LONG' if (ev.get('direction') == 'long' or 'down' in ev.get('reason', '')) else 'SHORT'
                        kind_guess = 'DUMP' if side_guess == 'LONG' else 'PUMP'

                        if not ev['is_spike']:
                            # Разделили всплеск с трендом: это продолжение — не входим
                            log.debug('SKIP %s %s: %s', sym, tf, ev['reason'])
                            self._seen_candles.add(candle_id)
                            
                            # Переводим причину на русский язык
                            reason_ru = ev['reason']
                            if 'not a rejection candle' in reason_ru:
                                reason_ru = reason_ru.replace('not a rejection candle', 'Не нож (нет длинной тени)')
                            elif 'trend continuation' in reason_ru:
                                reason_ru = reason_ru.replace('trend continuation:', 'Продолжение тренда на старшем ТФ. Сила:')
                            elif 'price still at knife low' in reason_ru:
                                reason_ru = 'Цена все еще на минимуме ножа (падающий нож)'
                            elif 'price still at knife high' in reason_ru:
                                reason_ru = 'Цена все еще на максимуме ножа (летящий нож)'
                            elif 'unknown rejection direction' in reason_ru:
                                reason_ru = 'Неизвестное направление ножа'

                            # Добавляем алерт с причиной отказа
                            state.add_alert(Alert(
                                ts=time.time(), symbol=sym, kind=kind_guess,
                                price_now=current_price, ref_price=open_price, ma=st.ma,
                                change_pct=change_pct, timeframe=f'{tf_label} SKIP',
                                reason=reason_ru
                            ))
                            continue
                            
                        if ev['confidence'] < min_conf:
                            log.debug('SKIP %s %s: confidence %.2f < %.2f (%s)', sym, tf, ev['confidence'], min_conf, ev['reason'])
                            self._seen_candles.add(candle_id)
                            
                            # Добавляем алерт с причиной отказа по уверенности
                            state.add_alert(Alert(
                                ts=time.time(), symbol=sym, kind=kind_guess,
                                price_now=current_price, ref_price=open_price, ma=st.ma,
                                change_pct=change_pct, timeframe=f'{tf_label} SKIP',
                                reason=f'Низкая уверенность ({ev["confidence"]:.2f} < {min_conf:.2f}). {ev["reason"]}'
                            ))
                            continue

                        side = 'LONG' if ev['direction'] == 'long' else 'SHORT'
                        kind = 'DUMP' if side == 'LONG' else 'PUMP'

                        state.add_alert(Alert(
                            ts=time.time(), symbol=sym, kind=kind,
                            price_now=current_price, ref_price=open_price, ma=st.ma,
                            change_pct=change_pct, timeframe=f'{tf_label} knife conf={ev["confidence"]:.2f}',
                            reason='Успешный вход (открытие сделки)'
                        ))

                        # --- СБОР СНИМКА ИНДИКАТОРОВ ---
                        # Выносим analysis в переменную, чтобы Pylance не ругался на Optional
                        analysis = ev.get('analysis')
                        anchor_trend = analysis.trend_direction if analysis else 'unknown'
                        anchor_strength = round(analysis.trend_strength, 2) if analysis else 0.0

                        entry_details = {
                            'timeframe': tf_label,
                            'confidence': round(ev['confidence'], 2),
                            'min_confidence': min_conf,
                            'reason': ev['reason'],
                            'change_pct': round(change_pct, 2),
                            'volume': volume,
                            'avg_volume': avg_vol,
                            'open_price': open_price,
                            'close_price': close_price_candle,
                            'high_price': float(last_closed[2]),
                            'low_price': float(last_closed[3]),
                            'knife_low': float(knife_tf[-2][3]) if len(knife_tf) >= 2 else 0,
                            'knife_high': float(knife_tf[-2][2]) if len(knife_tf) >= 2 else 0,
                            'current_price': current_price,
                            'ma_knife': ma_knife,
                            'ma_anchor': ma_anchor,
                            'anchor_trend': anchor_trend,
                            'anchor_strength': anchor_strength,
                        }
                        # --------------------------------
                        await self._open_trade(
                            session, sym, side, current_price, st.max_leverage, 
                            cfg, gs, change_pct, 
                            fee_pct=gs.fee_pct if gs and gs.fee_pct > 0 else 0.055,
                            details=entry_details
                        )

                        log.info('SIGNAL %s %s (%s) %.2f%% -> OPEN %s @ %.6f | %s', kind, sym, tf_label, change_pct, side, current_price, ev['reason'])

                        open_syms.add(sym)
                        break

                except Exception as e:
                    log.warning(f'Check entries failed for {sym}: {e}')
            await session.commit()

    async def _open_trade(self, session, sym, side, price, max_lev, cfg, gs, change_pct=None, fee_pct=0.055, details=None):
        if cfg and cfg.position_size_entry > 0:
            pos_size = cfg.position_size_entry
        else:
            pos_size = gs.position_size_entry if gs else MIN_POS_SIZE
        
        pos_size = max(MIN_POS_SIZE, pos_size)
        margin = pos_size / max_lev
        qty = pos_size / price
        # Bybit берёт taker-комиссию с каждой стороны по факту исполнения
        entry_fee = pos_size * (fee_pct / 100.0)

        if cfg and cfg.max_steps > 0:
            max_steps = cfg.max_steps
        else:
            max_steps = gs.max_steps if gs else 10

        trade = Trade(
            symbol=sym, side=side, entry_price=price, avg_entry_price=price,
            total_qty=qty, total_margin=margin, initial_margin=margin, status='OPEN',
            grid_step=0, max_steps=max_steps,
            leverage=max_lev, current_price=price, exit_step=0,
            entry_fee=entry_fee, pnl=-entry_fee,
        )
        session.add(trade)
        await session.flush()
        
        notes = f'{side} OPEN (size: {pos_size:.2f} USDT, fee {entry_fee:.4f})'
        if change_pct is not None:
            notes += f', change: {change_pct:.2f}%'
            
        history = TradeHistory(
            trade_id=trade.id, action='OPEN', price=price, qty=qty,
            margin=margin, pnl=-entry_fee, notes=notes,
            details=json.dumps(details) if details else None
        )
        session.add(history)
        log.info('OPEN %s %s @ %.6f (size: %.2f USDT, qty: %.2f, lev: %.0fx, fee: %.4f)', side, sym, price, pos_size, qty, max_lev, entry_fee)

    async def update_open_trades(self):
        async with SessionLocal() as session:
            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()
            
            fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055
            
            result = await session.execute(
                select(Trade).where(Trade.status == 'OPEN').where(Trade.total_qty > 0.0001)
            )
            for t in result.scalars().all():
                st = state.symbols.get(t.symbol)
                if not st or st.last_price <= 0 or st.ma is None: 
                    continue
                
                price = st.last_price
                ma = st.ma
                t.current_price = price
                
                cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == t.symbol))
                cfg = cfg_res.scalars().first()
                
                # --- 1. ЛОГИКА УСРЕДНЕНИЯ (своя прогрессия: grid_step_pct * grid_step_multiplier^n) ---
                if t.grid_step < t.max_steps:
                    start_pct = _pick(cfg.grid_step_pct if cfg else 0, gs.grid_step_pct if gs else 0, 5.0)
                    mult = _pick(cfg.grid_step_multiplier if cfg else 0, gs.grid_step_multiplier if gs else 0, 2.5)
                    step_pct = start_pct * (mult ** t.grid_step)
                    
                    can_grid = False
                    if t.side == 'LONG' and price <= t.avg_entry_price * (1 - step_pct / 100) and price < ma:
                        can_grid = True
                    elif t.side == 'SHORT' and price >= t.avg_entry_price * (1 + step_pct / 100) and price > ma:
                        can_grid = True
                    
                    if can_grid:
                        add_pos_size = _pick(cfg.position_size_grid if cfg else 0, gs.position_size_grid if gs else 0, MIN_POS_SIZE)
                        
                        # Применяем множитель размера для усреднений
                        size_mult = _pick(cfg.grid_size_multiplier if cfg else 0, gs.grid_size_multiplier if gs else 0, 1.0)
                        if size_mult > 1.0:
                            add_pos_size = add_pos_size * (size_mult ** t.grid_step)
                            
                        add_pos_size = max(MIN_POS_SIZE, add_pos_size)
                        
                        add_margin = add_pos_size / t.leverage
                        add_qty = add_pos_size / price
                        
                        old_qty = t.total_qty
                        t.grid_step += 1
                        t.total_qty += add_qty
                        t.total_margin += add_margin
                        t.avg_entry_price = (t.avg_entry_price * old_qty + price * add_qty) / t.total_qty
                        
                        add_commission = add_pos_size * (fee_pct / 100.0)
                        t.entry_fee += add_commission
                        
                        history = TradeHistory(
                            trade_id=t.id, action='GRID_ADD', price=price, qty=add_qty,
                            margin=add_margin, pnl=-add_commission, notes=f'BUY {add_qty:.2f} (step {t.grid_step}, left: {t.total_qty:.2f}, fee {add_commission:.4f})'
                        )
                        session.add(history)
                        await session.commit() 

                # --- 2. РАСЧЕТ PnL: нереализованный = чистая цена минус комиссии обеих сторон ---
                if t.side == 'LONG':
                    change_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    change_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100

                gross = t.total_qty * (price - t.avg_entry_price) if t.side == 'LONG' \
                    else t.total_qty * (t.avg_entry_price - price)
                # Комиссия входа уже уплачена (entry_fee), на выход — по текущей цене
                open_exit_fee = t.total_qty * price * (fee_pct / 100.0)
                t.pnl = t.realized_pnl + gross - t.entry_fee - open_exit_fee
                
                # --- 3. СТУПЕНЧАТЫЙ ВЫХОД (своя прогрессия: initial_exit_pct * exit_step_multiplier^n) ---
                start_exit = _pick(cfg.initial_exit_pct if cfg else 0, gs.initial_exit_pct if gs else 0, 15.0)
                exit_mult = _pick(cfg.exit_step_multiplier if cfg else 0, gs.exit_step_multiplier if gs else 0, 1.6)
                close_pct = _pick(cfg.exit_contract_pct if cfg else 0, gs.exit_contract_pct if gs else 0, 50.0)
                
                current_exit_step = t.exit_step if t.exit_step is not None else 0
                target_exit_pct = start_exit * (exit_mult ** current_exit_step)
                
                if change_pct >= target_exit_pct:
                    # Если остаток слишком мал (меньше 1 USDT), закрываем всё
                    if t.total_qty * (close_pct / 100.0) * price < 1.0 or t.total_qty * (1 - close_pct / 100.0) < 0.001:
                        closed_qty = t.total_qty
                        closed_frac = 1.0
                        action = 'FULL_CLOSE'
                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()
                    else:
                        closed_qty = t.total_qty * (close_pct / 100.0)
                        closed_frac = close_pct / 100.0
                        action = 'PARTIAL_CLOSE'
                        t.exit_step = current_exit_step + 1

                    # Доля комиссии входа, приходящаяся на закрываемую часть
                    entry_fee_part = t.entry_fee * closed_frac

                    # Старая средняя ОБЯЗАТЕЛЬНО фиксируется до переприсваиваний:
                    # PnL закрытой части считается именно от неё.
                    old_avg = t.avg_entry_price

                    if t.side == 'LONG':
                        closed_pnl = (closed_qty * price) - (closed_qty * old_avg)
                    else:
                        closed_pnl = (closed_qty * old_avg) - (closed_qty * price)

                    close_commission = (closed_qty * price) * (fee_pct / 100.0)
                    t.exit_fee += close_commission
                    # Реализуем PnL части: цена минус обе комиссии (вход и выход)
                    closed_pnl -= close_commission + entry_fee_part
                    t.realized_pnl += closed_pnl

                    t.entry_fee -= entry_fee_part
                    t.total_qty -= closed_qty
                    t.total_margin -= (closed_qty * old_avg / t.leverage)

                    # Фиксируем новую среднюю при частичном закрытии: остаток
                    # позиции считаем открытым по цене закрытия части.
                    if action == 'PARTIAL_CLOSE':
                        t.avg_entry_price = price

                    if t.status == 'CLOSED' or t.total_qty <= 0.0001:
                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()
                        t.total_qty = 0.0
                        t.total_margin = 0.0
                        t.entry_fee = 0.0

                    # Плавающий PnL = реализованное + gross остатка от НОВОЙ средней
                    # минус неучтённые комиссии (остаток входа и ожидаемая комиссия выхода).
                    if t.total_qty > 0.0001:
                        open_gross = t.total_qty * (price - t.avg_entry_price) if t.side == 'LONG' \
                            else t.total_qty * (t.avg_entry_price - price)
                        open_exit_fee = t.total_qty * price * (fee_pct / 100.0)
                        t.pnl = t.realized_pnl + open_gross - t.entry_fee - open_exit_fee
                    else:
                        t.pnl = t.realized_pnl

                    history = TradeHistory(
                        trade_id=t.id, action=action, price=price, qty=closed_qty,
                        margin=t.total_margin, pnl=closed_pnl, 
                        notes=f'SELL {close_pct}% (qty: {closed_qty:.2f}, left: {t.total_qty:.2f}, target {target_exit_pct:.1f}%, actual {change_pct:.2f}%, out fee {close_commission:.4f}, in fee {entry_fee_part:.4f})'
                    )
                    session.add(history)
                    await session.commit()
            
            await session.commit()

trading_bot = None