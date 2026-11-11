import asyncio
import datetime
import logging
import time

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine, Settings
from .state import state, Alert
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

            intervals = ['60', '240', 'D']
            
            for sym in symbols_to_check:
                try:
                    cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                    cfg = cfg_res.scalars().first()
                    stat = await session.get(SymbolStat, sym)
                    st = state.symbols.get(sym)
                    if not st or st.ma is None:
                        continue
                    
                    # Логика порога
                    if cfg and cfg.entry_pct > 0:
                        threshold = cfg.entry_pct
                    elif gs.entry_pct > 0:
                        threshold = gs.entry_pct
                    else:
                        threshold = (stat.avg_prostrel_pct * 0.85) if stat and stat.avg_prostrel_pct > 0 else 5.0
                        threshold = max(threshold, 5.0)
                    
                    for tf in intervals:
                        kl = await self.rest.get_kline(sym, interval=tf, limit=22)
                        if len(kl) < VOLUME_LOOKBACK + 2:
                            continue
                        
                        last_closed = kl[-2]
                        open_price = float(last_closed[1])
                        close_price = float(last_closed[4])
                        high_price = float(last_closed[2])
                        low_price = float(last_closed[3])
                        volume = float(last_closed[5])
                        
                        if open_price <= 0 or volume <= 0:
                            continue
                        
                        avg_vol = _calc_avg_volume(kl, VOLUME_LOOKBACK)
                        if avg_vol <= 0 or volume < avg_vol:
                            continue
                        
                        change_pct = (close_price - open_price) / open_price * 100
                        
                        if abs(change_pct) >= threshold:
                            candle_id = f"{sym}_{tf}_{last_closed[0]}"
                            if candle_id in self._seen_candles:
                                continue
                            self._seen_candles.add(candle_id)
                            
                            kind = 'PUMP' if change_pct > 0 else 'DUMP'
                            side = 'SHORT' if change_pct > 0 else 'LONG'
                            
                            # Проверка: не входим внизу/сверху всплеска
                            spike_center = (high_price + low_price) / 2
                            
                            if side == 'LONG':
                                if st.ma is None or close_price >= st.ma:
                                    log.info(f'SKIP LONG {sym}: price {close_price} >= MA {st.ma}')
                                    continue
                                if close_price < spike_center:
                                    log.info(f'SKIP LONG {sym}: price {close_price} < spike center {spike_center}')
                                    continue
                            else:  # SHORT
                                if st.ma is None or close_price <= st.ma:
                                    log.info(f'SKIP SHORT {sym}: price {close_price} <= MA {st.ma}')
                                    continue
                                if close_price > spike_center:
                                    log.info(f'SKIP SHORT {sym}: price {close_price} > spike center {spike_center}')
                                    continue
                            
                            tf_label = {'60': '1h', '240': '4h', 'D': '1d'}.get(tf, tf)
                            
                            state.add_alert(Alert(
                                ts=time.time(), symbol=sym, kind=kind,
                                price_now=close_price, ref_price=open_price, ma=st.ma,
                                change_pct=change_pct, timeframe=tf_label
                            ))
                            
                            await self._open_trade(session, sym, side, close_price, st.max_leverage, cfg, gs, change_pct)
                            
                            log.info(f'SIGNAL {kind} {sym} ({tf_label}) {change_pct:.2f}% (vol {volume:.0f} > avg {avg_vol:.0f}) -> OPEN {side}')
                            
                            open_syms.add(sym)
                            break
                            
                except Exception as e:
                    log.warning(f'Check entries failed for {sym}: {e}')
            await session.commit()

    async def _open_trade(self, session, sym, side, price, max_lev, cfg, gs, change_pct=None):
        if cfg and cfg.position_size_entry > 0:
            pos_size = cfg.position_size_entry
        else:
            pos_size = gs.position_size_entry if gs else MIN_POS_SIZE
        
        pos_size = max(MIN_POS_SIZE, pos_size)
                
        margin = pos_size / max_lev
        qty = pos_size / price

        if cfg and cfg.max_steps > 0:
            max_steps = cfg.max_steps
        else:
            max_steps = gs.max_steps if gs else 10

        trade = Trade(
            symbol=sym, side=side, entry_price=price, avg_entry_price=price,
            total_qty=qty, total_margin=margin, initial_margin=margin, status='OPEN',
            grid_step=0, max_steps=max_steps,
            leverage=max_lev, current_price=price,
        )
        session.add(trade)
        await session.flush()
        
        notes = f'{side} open (size: {pos_size:.2f} USDT)'
        if change_pct is not None:
            notes += f', change: {change_pct:.2f}%'
            
        history = TradeHistory(
            trade_id=trade.id, action='OPEN', price=price, qty=qty,
            margin=margin, pnl=0.0, notes=notes
        )
        session.add(history)
        log.info('OPEN %s %s @ %.6f (size: %.2f USDT, margin: %.2f, lev: %.0fx)', side, sym, price, pos_size, margin, max_lev)

        async def update_open_trades(self):
        async with SessionLocal() as session:
            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()
            
            fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055
            
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            for t in result.scalars().all():
                st = state.symbols.get(t.symbol)
                if not st or st.last_price <= 0 or st.ma is None: 
                    continue
                
                price = st.last_price
                ma = st.ma
                t.current_price = price
                
                cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == t.symbol))
                cfg = cfg_res.scalars().first()
                
                # --- 1. ЛОГИКА УСРЕДНЕНИЯ ---
                if t.grid_step < t.max_steps:
                    start_pct = (cfg.grid_step_pct if cfg and cfg.grid_step_pct > 0 else (gs.grid_step_pct if gs else 5.0))
                    mult = (cfg.grid_step_multiplier if cfg and cfg.grid_step_multiplier > 0 else (gs.grid_step_multiplier if gs else 2.5))
                    step_pct = start_pct * (mult ** t.grid_step)
                    
                    can_grid = False
                    if t.side == 'LONG' and price <= t.avg_entry_price * (1 - step_pct / 100) and price < ma:
                        can_grid = True
                    elif t.side == 'SHORT' and price >= t.avg_entry_price * (1 + step_pct / 100) and price > ma:
                        can_grid = True
                    
                    if can_grid:
                        add_pos_size = (cfg.position_size_grid if cfg and cfg.position_size_grid > 0 else (gs.position_size_grid if gs else MIN_POS_SIZE))
                        add_pos_size = max(MIN_POS_SIZE, add_pos_size)
                        
                        add_margin = add_pos_size / t.leverage
                        add_qty = add_pos_size / price
                        
                        old_qty = t.total_qty
                        t.grid_step += 1
                        t.total_qty += add_qty
                        t.total_margin += add_margin
                        t.avg_entry_price = (t.avg_entry_price * old_qty + price * add_qty) / t.total_qty
                        
                        add_commission = add_pos_size * (fee_pct / 100.0)
                        
                        history = TradeHistory(
                            trade_id=t.id, action='GRID_ADD', price=price, qty=add_qty,
                            margin=add_margin, pnl=-add_commission, notes=f'step {t.grid_step} (size: {add_pos_size:.2f} USDT, fee: {add_commission:.4f})'
                        )
                        session.add(history)
                        log.info('GRID ADD %s %s @ %.6f (step %d, fee %.4f)', t.side, t.symbol, price, t.grid_step, add_commission)
                        await session.commit() # Коммитим усреднение сразу

                # --- 2. РАСЧЕТ PnL ---
                if t.side == 'LONG':
                    change_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    change_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100
                
                position_size = t.total_margin * t.leverage
                total_commission = position_size * (fee_pct / 100.0) * 2
                
                t.pnl = (t.total_margin * (change_pct / 100.0) * t.leverage) - total_commission
                
                # --- 3. ЛОГИКА ВЫХОДА (ИСПРАВЛЕННАЯ) ---
                exit_pct = (cfg.initial_exit_pct if cfg and cfg.initial_exit_pct > 0 else (gs.initial_exit_pct if gs and gs.initial_exit_pct > 0 else 15.0))
                
                if t.total_qty < 0.001:
                    t.status = 'CLOSED'
                    t.closed_at = datetime.datetime.utcnow()
                    await session.commit()
                    continue
                
                # Проверяем историю, чтобы не спамить закрытиями, но ставим задержку 5 секунд (а не 5 минут)
                last_history = await session.execute(
                    select(TradeHistory).where(TradeHistory.trade_id == t.id).order_by(TradeHistory.ts.desc()).limit(1)
                )
                last_h = last_history.scalars().first()
                
                # Если последнее действие было PARTIAL_CLOSE и прошло меньше 5 секунд — пропускаем
                # Это защита от того, чтобы бот не закрывал по 50% каждые 2 секунды
                already_partial = (
                    last_h and last_h.action == 'PARTIAL_CLOSE' and 
                    (time.time() - last_h.ts.timestamp()) < 5
                )
                
                # Если цена достигла цели и это не флаг "already_partial"
                if change_pct >= exit_pct and not already_partial:
                    close_pct = (cfg.exit_contract_pct if cfg and cfg.exit_contract_pct > 0 else (gs.exit_contract_pct if gs and gs.exit_contract_pct > 0 else 50.0))
                    closed_qty = t.total_qty * (close_pct / 100.0)
                    
                    if t.side == 'LONG':
                        closed_pnl = (closed_qty * price) - (closed_qty * t.avg_entry_price)
                    else:
                        closed_pnl = (closed_qty * t.avg_entry_price) - (closed_qty * price)
                    
                    # Вычитаем комиссию за выход
                    closed_position_size = closed_qty * price
                    close_commission = closed_position_size * (fee_pct / 100.0)
                    closed_pnl -= close_commission
                    
                    t.total_qty -= closed_qty
                    t.total_margin -= (closed_qty * t.avg_entry_price / t.leverage)
                    
                    # Накапливаем PnL
                    t.pnl = (t.pnl or 0) + closed_pnl
                    
                    if t.total_qty < 0.001 or close_pct >= 100:
                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()
                        action = 'FULL_CLOSE'
                        log.info('CLOSE FULL %s %s @ %.6f PnL: %.2f USDT', t.side, t.symbol, price, closed_pnl)
                    else:
                        t.grid_step = 0  # Сбрасываем сетку
                        action = 'PARTIAL_CLOSE'
                        log.info('CLOSE PARTIAL %s %s @ %.6f (%.0f%%) PnL: %.2f USDT, grid reset', t.side, t.symbol, price, close_pct, closed_pnl)
                    
                    history = TradeHistory(
                        trade_id=t.id, action=action, price=price, qty=closed_qty,
                        margin=t.total_margin, pnl=closed_pnl, notes=f'close {close_pct}% (change {change_pct:.2f}%, fee {close_commission:.4f})'
                    )
                    session.add(history)
                    await session.commit() # Коммитим закрытие сразу
            
            await session.commit() # Финальный коммит

trading_bot = None