import asyncio
import datetime
import logging
import time
import json

from sqlalchemy import select, delete, func as sa_func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine, Settings
from .state import state, Alert, classify_event, moving_average
from .config import settings
from .validity import evaluate_validity

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
    # Кеш kline по (symbol, interval): защищает от штормов REST и от зависания
    # сделки при недоступности Bybit (возвращаем прошлый кеш).
    KLINE_TTL_SEC = 25
    REST_TIMEOUT_SEC = 8
    # Если открытая сделка не обновлялась столько секунд, а тик свежий —
    # считаем её «зависшей» и пишем SNAPSHOT-восстановление.
    STALE_TRADE_SEC = 180
    # Храним seen-свечи 48 часов (хватает для 4h/D интервалов с запасом).
    SEEN_RETENTION_MS = 48 * 3600 * 1000

    def __init__(self, rest):
        self.rest = rest
        self._last_scan_ts = 0
        self._seen_candles = set()
        self._kline_cache = {}
        self._last_prune_ts = 0

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
            result = await session.execute(
                select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL']))
            )
            open_trades = result.scalars().all()
            open_syms = {t.symbol for t in open_trades}

            # --- ЛИМИТ ОДНОВРЕМЕННО ОТКРЫТЫХ СДЕЛОК ---
            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()

            max_open = gs.max_open_trades if gs and gs.max_open_trades > 0 else 5
            active_count = len({t.symbol for t in open_trades if t.status in ('OPEN', 'PARTIAL')})
            if active_count >= max_open:
                return

            symbols_to_check = [s for s in state.symbols.keys() if s not in open_syms]
            if not symbols_to_check:
                return

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
                        kl = await self._fetch_klines_safe(sym, tf, 22)
                        if not kl:
                            continue
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
                        change_pct = (close_price_candle - open_price) / open_price * 100
                        if abs(change_pct) < threshold:
                            continue

                        # Объёмный фильтр — в classify_event, чтобы единая причина отказа
                        candle_id = f"{sym}_{tf}_{last_closed[0]}"
                        if candle_id in self._seen_candles:
                            continue

                        knife_tf = kline_cache.get(tf) or kl
                        anchor_tf = kline_cache.get('D') if tf != 'D' else None
                        if tf == 'D':
                            # Для дневного ножа якорем служит контекст 4h-окна
                            anchor_tf = kline_cache.get('240')

                        ma_knife = moving_average(knife_tf, min(settings.ma_period, 20))
                        ma_anchor = moving_average(anchor_tf, min(settings.ma_period, 20)) if anchor_tf else None

                        volume_ratio = volume / avg_vol if avg_vol > 0 else None
                        ev = classify_event(knife_tf, anchor_tf, current_price, ma_knife, ma_anchor,
                                            volume_ratio=volume_ratio)
                        min_conf = _pick(cfg.min_confidence if cfg else 0, gs.min_confidence if gs else 0, 0.4)
                        tf_label = {'60': '1h', '240': '4h', 'D': '1d'}.get(tf, tf)
                        
                        # Направление для алерта берём из анализа свечи: direction есть
                        # только при подтверждённом спайке, поэтому используем trend_direction
                        # ножа (down -> ищем LONG на отскоке, up -> SHORT).
                        an_obj = ev.get('analysis')
                        is_long_guess = bool(an_obj and an_obj.trend_direction == 'down')
                        side_guess = 'LONG' if is_long_guess else 'SHORT'
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
                            elif 'thin move, volume' in reason_ru:
                                reason_ru = reason_ru.replace('thin move, volume', 'Тонкое движение без объёма:')
                            elif 'price still at knife low' in reason_ru:
                                reason_ru = 'Цена все еще на минимуме ножа (падающий нож, слабого отскока недостаточно)'
                            elif 'price still at knife high' in reason_ru:
                                reason_ru = 'Цена все еще на максимуме ножа (летящий нож, слабого отката недостаточно)'
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
                            'volume_ratio': round(volume_ratio, 2) if volume_ratio else None,
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
                            details=entry_details,
                            tf=tf  # <--- ПЕРЕДАЕМ ТФ ('240' или 'D')
                        )


                        log.info('SIGNAL %s %s (%s) %.2f%% -> OPEN %s @ %.6f | %s', kind, sym, tf_label, change_pct, side, current_price, ev['reason'])

                        open_syms.add(sym)
                        active_count += 1
                        if active_count >= max_open:
                            break
                        break

                except Exception as e:
                    log.warning(f'Check entries failed for {sym}: {e}')
            await session.commit()

    def _calc_grid_exit_plan(self, t, cfg, gs):
        """Полный план сетки усреднений и ступенчатых выходов (для истории/снимков)."""
        plan = {}
        try:
            start_pct = _pick(cfg.grid_step_pct if cfg else 0, gs.grid_step_pct if gs else 0, 5.0)
            mult = _pick(cfg.grid_step_multiplier if cfg else 0, gs.grid_step_multiplier if gs else 0, 2.5)
            size_mult = _pick(cfg.grid_size_multiplier if cfg else 0, gs.grid_size_multiplier if gs else 0, 1.0)
            base_size = _pick(cfg.position_size_grid if cfg else 0, gs.position_size_grid if gs else 0, MIN_POS_SIZE)

            step_pct = start_pct * (mult ** t.grid_step)
            grid_size = max(MIN_POS_SIZE, base_size * ((size_mult ** t.grid_step) if size_mult > 1.0 else 1.0))
            if t.side == 'LONG':
                grid_price = t.avg_entry_price * (1 - step_pct / 100)
            else:
                grid_price = t.avg_entry_price * (1 + step_pct / 100)
            plan['next_grid'] = {
                'step': t.grid_step + 1,
                'pct': round(step_pct, 2),
                'price': round(grid_price, 8),
                'size_usdt': round(grid_size, 2),
                'remaining_steps': max(0, t.max_steps - t.grid_step),
            }

            start_exit = _pick(cfg.initial_exit_pct if cfg else 0, gs.initial_exit_pct if gs else 0, 15.0)
            exit_mult = _pick(cfg.exit_step_multiplier if cfg else 0, gs.exit_step_multiplier if gs else 0, 1.6)
            close_pct = _pick(cfg.exit_contract_pct if cfg else 0, gs.exit_contract_pct if gs else 0, 50.0)
            exit_step = t.exit_step or 0
            exit_pct = start_exit * (exit_mult ** exit_step)
            if t.side == 'LONG':
                exit_price = t.avg_entry_price * (1 + exit_pct / 100)
            else:
                exit_price = t.avg_entry_price * (1 - exit_pct / 100)
            exit_qty = t.total_qty * (close_pct / 100.0)
            plan['next_exit'] = {
                'step': exit_step + 1,
                'pct': round(exit_pct, 2),
                'price': round(exit_price, 8),
                'close_pct': close_pct,
                'qty': round(exit_qty, 6),
            }
        except Exception as e:
            log.debug('Grid/exit plan failed for %s: %r', getattr(t, 'symbol', '?'), e)
        return plan

    async def _fetch_klines_safe(self, symbol, interval, limit):
        """Kline c кешем по границе свечи. При сбое REST возвращаем прошлый кеш —
        это защищает от зависания сделки при недоступности Bybit."""
        now = time.time()
        key = (symbol, interval)
        entry = self._kline_cache.get(key)
        if entry and now - entry[0] < self.KLINE_TTL_SEC:
            return entry[1]
        try:
            kl = await asyncio.wait_for(
                self.rest.get_kline(symbol, interval=interval, limit=limit),
                timeout=self.REST_TIMEOUT_SEC
            )
        except Exception as e:
            log.warning('get_kline %s %s failed: %r%s', symbol, interval, e,
                        ' (using cached)' if entry else '')
            return entry[1] if entry else None
        self._kline_cache[key] = (now, kl)
        return kl

    def _prune_caches(self):
        """Очищаем кеш свечей и seen-свечей, чтобы не расти бесконечно."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.SEEN_RETENTION_MS
        self._seen_candles = {cid for cid in self._seen_candles
                              if int(cid.rsplit('_', 1)[1]) >= cutoff}

    async def _recover_stale_trades(self, session, open_trades, stale_sec=None):
        """Лечит «зависшие» сделки: если в потоке тиков есть свежая цена, а сделка
        не обновлялась дольше STALE_TRADE_SEC — форсируем update_open_trades для неё.

        Возвращает True, если была обработана хотя бы одна зависшая запись.
        """
        if stale_sec is None:
            stale_sec = self.STALE_TRADE_SEC
        recovered = False
        now = time.time()
        for t in open_trades:
            st = state.symbols.get(t.symbol)
            if not st or st.last_price <= 0:
                continue
            # last_tick_ts сравнима с UTC; приводим оба к unix-sec
            tick_age = abs(now - st.last_price_ts)
            # Если тик свежее самой записи в истории больше чем на stale_sec — лечим
            hist_res = await session.execute(
                select(TradeHistory.ts).where(TradeHistory.trade_id == t.id)
                .order_by(TradeHistory.ts.desc()).limit(1)
            )
            last_hist_ts = hist_res.scalar()
            if not last_hist_ts:
                continue
            db_age = (datetime.datetime.utcnow() - last_hist_ts).total_seconds()
            if db_age > stale_sec and tick_age < stale_sec:
                log.warning('RECOVER stale trade %s #%s: history=%.0fs ago, tick=%.0fs ago -> forcing update',
                            t.symbol, t.id, db_age, tick_age)
                # Помечаем в логе и сбрасываем timestamp, чтобы следующий цикл пересчитал PnL
                snapshot = await self._get_market_snapshot(t, session=session)
                session.add(TradeHistory(
                    trade_id=t.id, action='SNAPSHOT', price=st.last_price, qty=t.total_qty,
                    margin=t.total_margin, pnl=t.pnl,
                    notes=f'Авто-восстановление после зависания ({db_age:.0f}s)',
                    details=json.dumps(snapshot)
                ))
                await session.commit()
                recovered = True
        return recovered

    async def _get_market_snapshot(self, t, session=None):
        """Собирает подробный снимок текущего состояния для истории."""
        try:
            kl_1h = await self._fetch_klines_safe(t.symbol, '60', 30)
            kl_4h = await self._fetch_klines_safe(t.symbol, '240', 30)
            if not kl_1h or not kl_4h:
                return {}
            st = state.symbols.get(t.symbol)
            
            curr_price = st.last_price if st else t.current_price
            ma_1h = moving_average(kl_1h, min(20, len(kl_1h) - 1)) if len(kl_1h) > 5 else None
            ma_4h = moving_average(kl_4h, min(20, len(kl_4h) - 1)) if len(kl_4h) > 5 else None
            
            # Текущая и прошлая свеча на 1ч
            c_curr = kl_1h[-1] if kl_1h else [0]*6
            c_prev = kl_1h[-2] if len(kl_1h) > 1 else [0]*6

            age_hours = None
            ref_time = getattr(t, 'last_action_at', None) or t.opened_at
            if ref_time:
                age_hours = (datetime.datetime.utcnow() - ref_time).total_seconds() / 3600.0

            # Получаем актуальность разворота
            v = evaluate_validity(
                symbol=t.symbol, side=t.side,
                current_price=curr_price, avg_entry_price=t.avg_entry_price,
                klines_1h=kl_1h, klines_4h=kl_4h,
                ma_1h=ma_1h, ma_4h=ma_4h,
                age_hours=age_hours,
            )

            snapshot = {
                "current_price": curr_price,
                "avg_price": t.avg_entry_price,
                "pnl": t.pnl,
                "realized_pnl": t.realized_pnl,
                "position_size_usdt": t.total_qty * curr_price,
                "total_qty": t.total_qty,
                "grid_step": t.grid_step,
                "exit_step": t.exit_step,
                "max_steps": t.max_steps,
                "exit_max_steps": getattr(t, 'exit_max_steps', 0),
                "trade_age_hours": round(age_hours, 2) if age_hours is not None else None,
                "validity_status": v.status,
                "validity_score": v.score,
                "validity_reasons": list(v.reasons or []),
                "validity_metrics": v.metrics,
                "validity_verdict": v.verdict,
                "ma_1h": ma_1h,
                "ma_4h": ma_4h,
                "curr_candle": {"open": float(c_curr[1]), "high": float(c_curr[2]), "low": float(c_curr[3]), "close": float(c_curr[4]), "vol": float(c_curr[5])},
                "prev_candle": {"open": float(c_prev[1]), "high": float(c_prev[2]), "low": float(c_prev[3]), "close": float(c_prev[4]), "vol": float(c_prev[5])}
            }

            # Баланс на момент события (бумажный + реализованный + нереализованный)
            # и план сетки/выходов. Используем переданную сессию, если есть —
            # иначе открываем свою (вложенная сессия в SQLite может блокироваться).
            try:
                if session is not None:
                    gs_row = await session.get(Settings, 1)
                    paper_base = float(gs_row.paper_balance) if gs_row and gs_row.paper_balance else 100.0
                    realized_res = await session.execute(
                        select(sa_func.sum(TradeHistory.pnl))
                        .where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
                    )
                    realized_total = realized_res.scalar() or 0.0
                    open_res = await session.execute(
                        select(sa_func.sum(Trade.pnl))
                        .where(Trade.status.in_(['OPEN', 'PARTIAL']))
                    )
                    open_total = open_res.scalar() or 0.0
                else:
                    async with SessionLocal() as s_sess:
                        gs_row = await s_sess.get(Settings, 1)
                        paper_base = float(gs_row.paper_balance) if gs_row and gs_row.paper_balance else 100.0
                        realized_res = await s_sess.execute(
                            select(sa_func.sum(TradeHistory.pnl))
                            .where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
                        )
                        realized_total = realized_res.scalar() or 0.0
                        open_res = await s_sess.execute(
                            select(sa_func.sum(Trade.pnl))
                            .where(Trade.status.in_(['OPEN', 'PARTIAL']))
                        )
                        open_total = open_res.scalar() or 0.0
                snapshot["balance_realized"] = round(paper_base + realized_total, 4)
                snapshot["balance_live"] = round(paper_base + realized_total + open_total, 4)
            except Exception as bal_err:
                log.debug('Balance in snapshot failed for %s: %r', t.symbol, bal_err)

            # План сетки/выходов (следующий шаг) — если есть cfg/gs в контексте
            try:
                if session is not None:
                    cfg_res = await session.execute(
                        select(SymbolConfig).where(SymbolConfig.symbol == t.symbol)
                    )
                    snap_cfg = cfg_res.scalars().first()
                    snap_gs = await session.get(Settings, 1)
                    plan = self._calc_grid_exit_plan(t, snap_cfg, snap_gs)
                    if plan:
                        snapshot.update(plan)
            except Exception as plan_err:
                log.debug('Plan in snapshot failed for %s: %r', t.symbol, plan_err)

            return snapshot
        except Exception as e:
            log.warning(f"Failed to get snapshot for {t.symbol}: {e}")
            return {}

    async def _open_trade(self, session, sym, side, price, max_lev, cfg, gs, change_pct=None, fee_pct=0.055, details=None, tf='60'):

        # СТРОГАЯ ЛОГИКА: если в конфиге 0 - берем глобальное, иначе - конфиг
        if cfg and cfg.position_size_entry > 0:
            pos_size = cfg.position_size_entry
        else:
            pos_size = gs.position_size_entry if (gs and gs.position_size_entry > 0) else MIN_POS_SIZE
        
        pos_size = max(MIN_POS_SIZE, pos_size)
        margin = pos_size / max_lev
        qty = pos_size / price
        # Bybit берёт taker-комиссию с каждой стороны по факту исполнения
        entry_fee = pos_size * (fee_pct / 100.0)

        if cfg and cfg.max_steps > 0:
            max_steps = cfg.max_steps
        else:
            max_steps = gs.max_steps if (gs and gs.max_steps > 0) else 10

        # Максимум шагов выхода (0 = без ограничения)
        exit_max_steps = gs.exit_max_steps if gs and gs.exit_max_steps > 0 else 0

        trade = Trade(
            symbol=sym, side=side, entry_price=price, avg_entry_price=price,
            total_qty=qty, total_margin=margin, initial_margin=margin, status='OPEN',
            grid_step=0, max_steps=max_steps,
            leverage=max_lev, current_price=price, exit_step=0,
            entry_fee=entry_fee, pnl=-entry_fee,
            tf=tf, # <--- СОХРАНЯЕМ
            validity='green',
            exit_max_steps=exit_max_steps,
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

    async def _check_trade_validity(self, t):
        """
        Оценивает актуальность разворота по открытой сделке.

        Возвращает ValidityResult или None если данных недостаточно.
        Побочный эффект: обновляет t.validity и t.validity_reasons (JSON).
        Красный цвет разрешён только после cooldown-периода с момента входа/усреднения.
        """
        try:
            kl_1h = await self._fetch_klines_safe(t.symbol, '60', 30)
            kl_4h = await self._fetch_klines_safe(t.symbol, '240', 30)
            if not kl_1h or not kl_4h:
                return None

            ma_1h = moving_average(kl_1h, min(20, len(kl_1h) - 1)) if len(kl_1h) > 5 else None
            ma_4h = moving_average(kl_4h, min(20, len(kl_4h) - 1)) if len(kl_4h) > 5 else None

            st = state.symbols.get(t.symbol)
            current_price = st.last_price if st and st.last_price > 0 else t.current_price

            # Возраст сделки в часах — для cooldown индикатора разворота
            age_hours = None
            ref_time = getattr(t, 'last_action_at', None) or t.opened_at
            if ref_time:
                age_hours = (datetime.datetime.utcnow() - ref_time).total_seconds() / 3600.0

            v = evaluate_validity(
                symbol=t.symbol,
                side=t.side,
                current_price=current_price,
                avg_entry_price=t.avg_entry_price,
                klines_1h=kl_1h,
                klines_4h=kl_4h,
                ma_1h=ma_1h,
                ma_4h=ma_4h,
                age_hours=age_hours,
            )
            if v:
                t.validity = v.status
                try:
                    t.validity_reasons = json.dumps(list(v.reasons or []), ensure_ascii=False)
                except Exception:
                    t.validity_reasons = None
                try:
                    t._validity_ts = time.time()
                except Exception:
                    pass
            return v
        except Exception as e:
            log.warning('Validity check failed for %s: %r', t.symbol, e)
            return None


    async def _close_position(self, session, t, price, reason='', close_pct=None):
        """
        Закрывает позицию полностью с фиксацией PnL и записью в историю.
        Используется когда разворот потерян — вместо усреднения выходим.
        """
        gs = await session.get(Settings, 1)
        fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055

        closed_qty = t.total_qty
        if t.side == 'LONG':
            closed_pnl = closed_qty * (price - t.avg_entry_price)
        else:
            closed_pnl = closed_qty * (t.avg_entry_price - price)

        close_commission = closed_qty * price * (fee_pct / 100.0)
        realized_part = closed_pnl - close_commission - t.entry_fee

        t.realized_pnl += realized_part
        t.exit_fee += close_commission
        t.pnl = t.realized_pnl
        t.status = 'CLOSED'
        t.closed_at = datetime.datetime.utcnow()
        t.current_price = price
        t.total_qty = 0.0
        t.total_margin = 0.0
        t.entry_fee = 0.0

        # СОБИРАЕМ СНИМОК (отдельная сессия — _close_position свой коммит уже сделал)
        snapshot = await self._get_market_snapshot(t, session=session)

        notes = f'VALIDITY EXIT: {reason} (qty: {closed_qty:.2f}, pnl: {realized_part:.4f})' if reason \
            else f'FULL CLOSE at validity exit (qty: {closed_qty:.2f}, pnl: {realized_part:.4f})'

        history = TradeHistory(
            trade_id=t.id, action='FULL_CLOSE', price=price, qty=closed_qty,
            margin=0.0, pnl=realized_part, notes=notes,
            details=json.dumps(snapshot)
        )
        session.add(history)
        await session.commit()
        log.info('VALIDITY EXIT %s %s @ %.6f | PnL: %.4f | reason: %s',
                 t.side, t.symbol, price, realized_part, reason)

    async def update_open_trades(self):
        # Периодическая очистка кеша seen-свечей (раз в час)
        now_ts = time.time()
        if now_ts - self._last_prune_ts > 3600:
            self._prune_caches()
            self._last_prune_ts = now_ts

        async with SessionLocal() as session:
            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()
            
            fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055
            check_validity = bool(getattr(gs, 'check_validity_before_grid', False))
            
            result = await session.execute(
                select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL'])).where(Trade.total_qty > 0.0001)
            )
            open_trades = list(result.scalars().all())

            # --- ЛЕЧЕНИЕ «ЗАВИСШИХ» СДЕЛОК ---
            # Если история не обновлялась дольше STALE_TRADE_SEC, но тик свежий — пишем SNAPSHOT
            try:
                await self._recover_stale_trades(session, open_trades)
            except Exception as e:
                log.warning('Recovery of stale trades failed: %r', e)
            
            for t in open_trades:
                st = state.symbols.get(t.symbol)
                if not st or st.last_price <= 0:
                    continue
                
                price = st.last_price
                ma = st.ma
                t.current_price = price
                
                cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == t.symbol))
                cfg = cfg_res.scalars().first()

                # --- 0. ВАЛИДАЦИЯ РАЗВОРОТА И ЛОГИРОВАНИЕ СМЕНЫ ЦВЕТА ---
                need_check = True
                hist_res = await session.execute(
                    select(TradeHistory.ts, TradeHistory.action)
                    .where(TradeHistory.trade_id == t.id)
                    .order_by(TradeHistory.ts.desc())
                    .limit(1)
                )
                last_hist = hist_res.first()
                if last_hist:
                    last_hist_ts, last_action = last_hist
                    delta = (datetime.datetime.utcnow() - last_hist_ts).total_seconds()
                    # После VALIDITY_CHANGE / SNAPSHOT — cooldown 60 сек
                    if last_action in ('VALIDITY_CHANGE', 'SNAPSHOT') and delta < 60:
                        need_check = False
                    elif last_action not in ('VALIDITY_CHANGE', 'SNAPSHOT') and delta < 15:
                        # После открытия/усреднения/выхода ждём 15 сек перед проверкой
                        need_check = False

                validity = None

                if need_check:
                    v = await self._check_trade_validity(t)
                    if v:
                        validity = v
                        old_status = t.validity or 'green'
                        
                        # Всегда обновляем статус в БД
                        t.validity = v.status
                        try:
                            t.validity_reasons = json.dumps(list(v.reasons or []), ensure_ascii=False)
                        except Exception:
                            t.validity_reasons = ""
                        
                        # Обновляем last_action_at — для cooldown индикатора
                        t.last_action_at = datetime.datetime.utcnow()
                        
                        # Принудительно сохраняем статус в БД, чтобы он не сбросился
                        await session.commit()
                        
                        if old_status != v.status:
                            # Собираем снимок индикаторов
                            snapshot = await self._get_market_snapshot(t)
                            
                            # Формируем подробное обоснование смены цвета
                            verdict_text = ''
                            if v.verdict:
                                verdict_text = v.verdict.get('explanation', '')
                                verdict_note = v.verdict.get('note', '')
                                if verdict_note:
                                    verdict_text = verdict_note + '. ' + verdict_text if verdict_text else verdict_note
                            
                            hist_notes = f"Смена статуса разворота: {old_status} -> {v.status}. Оценка: {v.score:.2f}"
                            if verdict_text:
                                hist_notes += f"\nОбоснование: {verdict_text}"

                            snapshot = await self._get_market_snapshot(t, session=session)

                            history = TradeHistory(
                                trade_id=t.id, action='VALIDITY_CHANGE', price=price, qty=t.total_qty,
                                margin=t.total_margin, pnl=t.pnl,
                                notes=hist_notes,
                                details=json.dumps(snapshot)
                            )
                            session.add(history)
                            await session.commit()
                            log.info(f"VALIDITY CHANGE {t.symbol}: {old_status} -> {v.status}. {verdict_text}")


                # --- 1. ЛОГИКА УСРЕДНЕНИЯ ---
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
                        if check_validity:
                            v = validity if validity is not None else await self._check_trade_validity(t)
                            if v and v.status == 'red':
                                await self._close_position(
                                    session, t, price,
                                    reason='; '.join(v.reasons[:4]) or 'разворот неактуален'
                                )
                                continue
                        
                        add_pos_size = _pick(cfg.position_size_grid if cfg else 0, gs.position_size_grid if gs else 0, MIN_POS_SIZE)
                        
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

                        grid_exit_reset = ''
                        if t.grid_step >= 3:
                            t.exit_step = 0
                            grid_exit_reset = ', exits reset'

                        # СОБИРАЕМ СНИМОК ПРИ УСРЕДНЕНИИ
                        snapshot = await self._get_market_snapshot(t, session=session)

                        history = TradeHistory(
                            trade_id=t.id, action='GRID_ADD', price=price, qty=add_qty,
                            margin=add_margin, pnl=-add_commission, 
                            notes=f'Усреднение {t.grid_step} (left: {t.total_qty:.2f}, fee {add_commission:.4f}{grid_exit_reset})',
                            details=json.dumps(snapshot)
                        )
                        session.add(history)
                        await session.commit()

                # --- 2. РАСЧЕТ PnL ---
                if t.side == 'LONG':
                    change_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    change_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100

                gross = t.total_qty * (price - t.avg_entry_price) if t.side == 'LONG' \
                    else t.total_qty * (t.avg_entry_price - price)
                open_exit_fee = t.total_qty * price * (fee_pct / 100.0)
                t.pnl = t.realized_pnl + gross - t.entry_fee - open_exit_fee
                
                # --- 3. СТУПЕНЧАТЫЙ ВЫХОД ---
                start_exit = _pick(cfg.initial_exit_pct if cfg else 0, gs.initial_exit_pct if gs else 0, 15.0)
                exit_mult = _pick(cfg.exit_step_multiplier if cfg else 0, gs.exit_step_multiplier if gs else 0, 1.6)
                close_pct = _pick(cfg.exit_contract_pct if cfg else 0, gs.exit_contract_pct if gs else 0, 50.0)
                
                current_exit_step = t.exit_step if t.exit_step is not None else 0
                target_exit_pct = start_exit * (exit_mult ** current_exit_step)

                exit_max = t.exit_max_steps if getattr(t, 'exit_max_steps', 0) else (gs.exit_max_steps if gs and gs.exit_max_steps > 0 else 0)
                force_full_close = exit_max > 0 and (current_exit_step + 1) >= exit_max

                if change_pct >= target_exit_pct:
                    if (force_full_close
                            or t.total_qty * (close_pct / 100.0) * price < 1.0
                            or t.total_qty * (1 - close_pct / 100.0) < 0.001):
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
                        t.grid_step = 0

                    entry_fee_part = t.entry_fee * closed_frac
                    old_avg = t.avg_entry_price

                    if t.side == 'LONG':
                        closed_pnl = (closed_qty * price) - (closed_qty * old_avg)
                    else:
                        closed_pnl = (closed_qty * old_avg) - (closed_qty * price)

                    close_commission = (closed_qty * price) * (fee_pct / 100.0)
                    t.exit_fee += close_commission
                    closed_pnl -= close_commission + entry_fee_part
                    t.realized_pnl += closed_pnl

                    t.entry_fee -= entry_fee_part
                    t.total_qty -= closed_qty
                    t.total_margin -= (closed_qty * old_avg / t.leverage)

                    if action == 'PARTIAL_CLOSE':
                        t.avg_entry_price = price

                    if t.status == 'CLOSED' or t.total_qty <= 0.0001:
                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()
                        t.total_qty = 0.0
                        t.total_margin = 0.0
                        t.entry_fee = 0.0

                    if t.total_qty > 0.0001:
                        open_gross = t.total_qty * (price - t.avg_entry_price) if t.side == 'LONG' \
                            else t.total_qty * (t.avg_entry_price - price)
                        open_exit_fee = t.total_qty * price * (fee_pct / 100.0)
                        t.pnl = t.realized_pnl + open_gross - t.entry_fee - open_exit_fee
                    else:
                        t.pnl = t.realized_pnl

                    # СОБИРАЕМ СНИМОК ПРИ ВЫХОДЕ
                    snapshot = await self._get_market_snapshot(t, session=session)

                    history = TradeHistory(
                        trade_id=t.id, action=action, price=price, qty=closed_qty,
                        margin=t.total_margin, pnl=closed_pnl, 
                        notes=f'Выход {t.exit_step} ({close_pct}%): закрыто {closed_qty:.2f}, осталось {t.total_qty:.2f}. Цель: {target_exit_pct:.1f}%, факт: {change_pct:.2f}%',
                        details=json.dumps(snapshot)
                    )
                    session.add(history)
                    await session.commit()
            
            # Финальный коммит для сохранения t.validity и t.pnl / current_price
            await session.commit()

trading_bot = None