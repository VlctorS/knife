import asyncio
import datetime
import logging
import time
import json
import sqlite3 as _sqlite3

from typing import Optional

from sqlalchemy import select, delete, func as sa_func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
import uuid
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine, Settings, CLOSE_ACTIONS
from .state import (
    state, Alert, classify_event, moving_average,
    trend_by_closes, find_pins_with_confirm, PinContext, _candle_sec,
)
from .indicators import atr as calc_atr, find_levels, nearest_level
from .modes import get_mode, resolve_distance, ModeParams, interval_sec as _interval_sec
from .bybit import normalize_qty, min_order_notional
from .config import settings
from .validity import evaluate_validity

log = logging.getLogger(__name__)

MIN_POS_SIZE = 6.0
VOLUME_LOOKBACK = 20


class InsufficientBalance(Exception):
    """Недостаточно свободно маржи для входа. НЕ блокирует сигнал: сделка
    не открывается, но пин остаётся живым и сигнал виден вUI до проверки
    баланса (см. 'insufficient balance' в обработчике ошибок входа)."""


def _get_upsert(model, values, pk_col):
    is_pg = engine.url.drivername.startswith('postgresql')
    if is_pg:
        return pg_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})
    return sqlite_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})


# --- Валидация разворота по ЖИВОМУ ТФ сделки (а не по захардкоженным 1H/4H) ---
# Раньше validity скальпа-на-5m проверялась по закрытию 1H и 4H свечей:
# для hold_max_hours=6 это абсурд — «красный» мог появиться только после
# закрытия 4H бара. Теперь ТФ валидации = рабочий ТФ режима, cooldown и
# подтверждение слома — в барах этого ТФ.
_VALID_TF_SEC = {'5': 300, '15': 900, '240': 14400}


def _validity_tf(mode) -> str:
    """ТФ для оценки актуальности разворота = рабочий ТФ режима."""
    return mode.work_tf


def _validity_struct_params(mode) -> tuple:
    """(min_hold_sec, confirm_candles) для структурного красного на ТФ режима.

    Скальп 5m: минимум 45 минут удержания + 2 подтверждённые 5m свечи.
    Интрадэй 15m: 3 часа + 2 свечи 15m. Свинг 4H: 6 часов + 2 свечи 4H
    (не дольше, чем раньше по 1h-логике, но на живом графике сделки).
    """
    tf_sec = _VALID_TF_SEC.get(str(mode.work_tf), 3600)
    if mode.id == 'scalp':
        min_age_sec = 45 * 60
    elif mode.id == 'intraday':
        min_age_sec = 3 * 3600
    else:
        min_age_sec = 6 * 3600
    return min_age_sec, max(2, int(2 * 3600 / tf_sec)) if mode.id == 'swing' else 2


def _calc_avg_volume(candles, lookback=20):
    if len(candles) < lookback + 1:
        return 0.0
    prev = candles[-(lookback + 1):-1]
    vols = [float(k[5]) for k in prev if float(k[5]) > 0]
    return sum(vols) / len(vols) if vols else 0.0


def _pick(cfg_val, global_val, fallback):
    """Приорет настройки: конфиг символа -> общие настройки -> дефолт."""
    if cfg_val and cfg_val > 0:
        return cfg_val
    if global_val and global_val > 0:
        return global_val
    return fallback


def _trade_age_hours(t) -> Optional[float]:
    """Возраст сделки (часы) от момента входа (opened_at).

    Cooldown красного цвета должен считаться от входа, а не от
    last_action_at: усреднения и смена цвета обнуляли бы его, и сделка
    на сетке никогда не получила бы red.
    """
    ref = getattr(t, 'opened_at', None)
    if not ref:
        return None
    return (datetime.datetime.utcnow() - ref).total_seconds() / 3600.0


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
    # Максимум свечей, которое отдаёт Bybit за один kline-запрос.
    KLINES_PER_REQUEST = 1000
    # Сколько последних пин-свечей помним как «уже проверенные без сигнала»
    PENDING_TTL_SEC = 3 * 3600
    # Лимиты по одновременным запросам kline (Bybit rate limit: ~10 req/s, берём с запасом)
    MAX_CONCURRENT_KLINE_REQUESTS = 5
    # Кэш загруженных свечей для открытых сделок (symbol, interval) -> (ts, klines)
    TRADE_KLINE_CACHE_TTL_SEC = 30

    def __init__(self, rest):
        self.rest = rest
        self._last_scan_ts = 0
        self._seen_candles = set()
        self._kline_cache = {}
        self._trade_kline_cache = {}  # symbol -> {interval: (ts, klines)}
        self._last_prune_ts = 0
        self._kline_sem = asyncio.Semaphore(self.MAX_CONCURRENT_KLINE_REQUESTS)
        # {(symbol, mode_id): {pin_ts: ts_seen}} — пины без подтверждения ещё
        # живы; если подтверждение появится позже — сигнал пропустится.
        self._pending_pins = {}
        # Индикаторный кеш режима: symbol -> dict(atr_work, levels_work, ...)      
        self._mode_cache = {}
        # {(symbol, mode_id): (open_ts_sec, deadline_sec)} — какую свечу рабочего
        # ТФ ждёт бот по последнему найденному пин-сигналу. Живёт до смены бара
        # или выхода пина из окна подтверждения; UI рисует обратный отсчёт.
        self._wait_tf = {}

    async def reset_signal_state(self):
        """Сброс состояния поиска сигналов (при смене режима в настройках):
        сигналы сбрасываются, поиск начинается сначала. Старые сделки не
        затрагиваются — они дорабатывают свой режим."""
        self._seen_candles.clear()
        self._pending_pins.clear()
        self._mode_cache.clear()
        self._kline_cache.clear()
        self._trade_kline_cache.clear()
        self._wait_tf.clear()
        self._last_scan_ts = 0
        state.alerts.clear()
        log.info('Signal search reset (new mode); open trades keep their own mode')

    async def sync_open_positions(self):
        """
        Синхронизирует открытые позиции с биржей.

        При запуске бота загружает реальные позиции с Bybit и создаёт
        записи в БД если их нет. Также обновляет существующие записи
        данными с биржи (qty, avg_entry_price, etc).

        Возвращает количество синхронизированных позиций.
        """
        for attempt in range(3):
            try:
                return await self._do_sync_positions()
            except Exception as e:
                err = str(e)
                if 'database is locked' in err and attempt < 2:
                    log.warning('[SYNC] БД заблокирована, попытка %d/3 через 2 сек...', attempt + 1)
                    await asyncio.sleep(2)
                else:
                    raise
    
    async def _do_sync_positions(self):
        log.info('[SYNC] Начальная синхронизация позиций с Bybit...')
        positions = await self.rest.get_positions()
        if not positions:
            log.info('[SYNC] Нет открытых позиций на Bybit')
        
        synced = 0
        async with SessionLocal() as session:
            # Загружаем существующие открытые сделки из БД
            result = await session.execute(
                select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL']))
            )
            existing = {}
            for t in result.scalars().all():
                existing[t.symbol] = t

            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()

            fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055

            for pos in positions:
                symbol = pos.get('symbol', '')
                size = float(pos.get('size', 0))
                if size == 0:
                    continue

                side_raw = pos.get('side', '')
                side = 'LONG' if side_raw == 'Buy' else 'SHORT'
                
                avg_entry = float(pos.get('avgEntryPrice') or 0)
                entry_price = float(pos.get('entryPrice') or avg_entry)
                leverage = float(pos.get('leverage') or 10)
                unrealisedPnl = float(pos.get('unrealisedPnl') or 0)

                # Fallback: если avg_entry = 0, ищем в истории ордеров
                if avg_entry <= 0:
                    try:
                        order_hist = await self.rest.get_order_history(symbol, limit=50)
                        for o in reversed(order_hist):
                            if o.get('orderStatus') == 'Filled' and o.get('cumExecQty', '0') > '0':
                                avg_entry = float(o.get('avgPrice') or 0)
                                if avg_entry > 0:
                                    log.info('[SYNC] Найдена entry цена из истории: %s %.6f', symbol, avg_entry)
                                    break
                    except Exception as e:
                        log.warning('[SYNC] Не удалось найти entry цену для %s: %s', symbol, e)
                    # entry_price тоже обновляем, если тоже 0
                    if entry_price <= 0:
                        entry_price = avg_entry
                positionValue = float(pos.get('positionValue') or 0)
                marginCo = float(pos.get('marginCo') or 0)
                
                # Калькулируем qty в контрактах
                # Bybit: 1 контракт = 0.0001 BTC (зависит от символа)
                # Но size — это количество контрактов
                qty = size  # size уже в контрактах
                
                # Калькулируем маржу
                margin = marginCo if marginCo > 0 else (positionValue / max(leverage, 1))
                
                # Калькулируем entry fee на основе размера позиции
                entry_fee = positionValue * (fee_pct / 100.0)
                
                # Текущая цена: из WS, fallback на avg_entry
                st = state.symbols.get(symbol)
                ws_price = st.last_price if st and st.last_price > 0 else 0
                current_price = ws_price if ws_price > 0 else avg_entry if avg_entry > 0 else entry_price
                
                # Пытаемся определить grid_step по истории ордеров
                grid_step = 0
                try:
                    order_hist = await self.rest.get_order_history(symbol, limit=100)
                    # Считаем количество GRID_ADD ордеров (с префиксом knife_grid)
                    for order in order_hist:
                        olid = order.get('orderLinkId', '')
                        if 'knife_grid' in olid:
                            grid_step += 1
                except Exception:
                    pass  # Если нет истории — grid_step = 0
                
                # Определяем mode: если позиция уже была в БД — берём её режим;
                # для новой позиции пытаемся угадать по размеру сетки/объёму, иначе intraday.
                if symbol in existing:
                    mode_id = getattr(existing[symbol], 'mode', None) or 'intraday'
                else:
                    # Новая позиция вне бота: если есть история усреднений — считаем grid_step
                    guessed_mode = 'intraday'
                    try:
                        pos_size_usdt = positionValue if positionValue > 0 else qty * avg_entry
                        # Очень маленькие позиции чаще скальперские
                        if 0 < pos_size_usdt <= 15:
                            guessed_mode = 'scalp'
                        elif pos_size_usdt >= 200:
                            guessed_mode = 'swing'
                    except Exception:
                        pass
                    mode_id = guessed_mode
                
                # Рассчитываем PnL
                if side == 'LONG':
                    gross_pnl = qty * (current_price - avg_entry)
                else:
                    gross_pnl = qty * (avg_entry - current_price)
                
                exit_fee_est = qty * current_price * (fee_pct / 100.0)
                pnl = unrealisedPnl if unrealisedPnl != 0 else (gross_pnl - entry_fee - exit_fee_est)
                
                if symbol in existing:
                    # Обновляем существующую запись: только рыночные данные,
                    # НЕ перезаписываем режим, tf, открытую дату, grid_step
                    # и validty — они могут быть установлены ботом или вручную.
                    t = existing[symbol]
                    t.side = side
                    t.entry_price = entry_price
                    t.avg_entry_price = avg_entry
                    t.total_qty = qty
                    t.total_margin = margin
                    t.initial_margin = margin
                    t.current_price = current_price
                    t.leverage = leverage
                    t.entry_fee = entry_fee
                    t.pnl = pnl
                    t.status = 'OPEN'
                    # grid_step обновляем ТОЛЬКО если новый больше (внешнее усреднение)
                    if grid_step > t.grid_step:
                        t.grid_step = grid_step
                    t.bybit_position_id = pos.get('positionIdx')
                    
                    log.info('[SYNC] Обновлена сделка %s: %s qty=%.4f avg=%.6f pnl=%.2f',
                             symbol, side, qty, avg_entry, pnl)
                else:
                    # Создаём новую запись — позиция есть на бирже но нет в БД
                    mode = get_mode(mode_id)
                    max_steps = gs.max_steps if gs and gs.max_steps > 0 else mode.max_steps
                    exit_max_steps = gs.exit_max_steps if gs and gs.exit_max_steps > 0 else 0
                    
                    trade = Trade(
                        symbol=symbol, side=side, entry_price=entry_price,
                        avg_entry_price=avg_entry, total_qty=qty,
                        total_margin=margin, initial_margin=margin,
                        status='OPEN', grid_step=grid_step, max_steps=max_steps,
                        leverage=leverage, current_price=current_price,
                        exit_step=0, entry_fee=entry_fee, pnl=pnl,
                        tf=mode.work_tf, mode=mode_id, validity='green',
                        exit_max_steps=exit_max_steps,
                        bybit_order_id=None,
                        bybit_position_id=pos.get('positionIdx'),
                    )
                    session.add(trade)
                    await session.flush()
                    
                    # Записываем в историю как OPEN
                    notes = f'{side} SYNCED from Bybit (qty: {qty:.4f}, avg: {avg_entry:.6f}, lev: {leverage:.0f}x)'
                    history = TradeHistory(
                        trade_id=trade.id, action='SYNCED', price=entry_price,
                        qty=qty, margin=margin, pnl=0, notes=notes
                    )
                    session.add(history)
                    
                    log.info('[SYNC] Создана сделка из Bybit: %s %s qty=%.4f avg=%.6f',
                             symbol, side, qty, avg_entry)
                    synced += 1

                    # Визуализируем подхваченную вручную сделку в сигналах.
                    self._alert_trade_opened(symbol, side, current_price,
                                             mode.label, qty * current_price,
                                             qty, leverage, source='sync')
            
            await session.commit()

            # Удаляем позиции, которых нет на бирже
            bybit_symbols = set(pos.get('symbol', '') for pos in positions)
            stale = [t for t in existing.values() if t.symbol not in bybit_symbols]
            log.info('[SYNC] Статус синхронизации: существующих=%d, на бирже=%d, будет удалено=%d',
                     len(existing), len(bybit_symbols), len(stale))
            if stale:
                for t in stale:
                    log.info('[SYNC] Закрываем позицию в БД (нет на бирже): %s (статус: %s, qty: %.4f)',
                             t.symbol, t.status, t.total_qty)

                    # Пытаемся взять РЕАЛЬНУЮ цену закрытия и PnL из closed-pnl.
                    # Fallback — последняя сохранённая цена в БД.
                    exit_price = t.current_price or t.avg_entry_price or 0.0
                    closed_pnl = float(t.pnl or 0.0)
                    closed_by_sync_pnl = None
                    try:
                        records = await self.rest.get_closed_pnl(t.symbol, limit=50)
                        if records:
                            # Берём самую свежую запись по updatedTime/createdTime
                            def _ts(r):
                                return int(r.get('updatedTime') or r.get('createdTime') or 0)
                            rec = max(records, key=_ts)
                            rec_avg_exit = float(rec.get('avgExitPrice') or 0)
                            if rec_avg_exit > 0:
                                exit_price = rec_avg_exit
                            closed_by_sync_pnl = float(rec.get('closedPnl') or 0)
                    except Exception as e:
                        log.warning('[SYNC] closed-pnl недоступен для %s: %r', t.symbol, e)

                    if closed_by_sync_pnl is not None:
                        # Реальный суммарный PnL закрытого цикла от Bybit.
                        # Заменяем ранее накопленный realized, чтобы не суммировать
                        # частичные выходы бота дважды.
                        realized_part = closed_by_sync_pnl
                        total_closed_pnl = closed_by_sync_pnl
                    else:
                        # Без closed-pnl — считаем по реальной цене закрытия.
                        avg = float(t.avg_entry_price or 0.0)
                        q = float(t.total_qty or 0.0)
                        if avg > 0 and q > 0 and exit_price > 0:
                            gross = q * (exit_price - avg) if t.side == 'LONG' \
                                else q * (avg - exit_price)
                            exit_fee = q * exit_price * (fee_pct / 100.0)
                            realized_part = gross - exit_fee - float(t.entry_fee or 0.0)
                        else:
                            realized_part = float(t.pnl or 0.0)
                        total_closed_pnl = float(t.realized_pnl or 0.0) + realized_part

                    history = TradeHistory(
                        trade_id=t.id, action='CLOSED_BY_SYNC', price=exit_price,
                        qty=float(t.total_qty or 0.0), margin=0, pnl=realized_part,
                        notes=(f'Позиция закрыта на бирже вне бота @ {exit_price:.6f}; '
                               f'PnL {realized_part:.4f} USDT'
                               + ('' if closed_by_sync_pnl is not None else ' (расчёт по цене)'))
                    )
                    session.add(history)

                    t.realized_pnl = total_closed_pnl
                    t.status = 'CLOSED'
                    t.closed_at = datetime.datetime.utcnow()
                    # Не обнуляем avg_entry_price / entry_price / total_qty —
                    # сохраняем историю сделки (UI показывает их в закрытых).
                    t.current_price = exit_price
                    t.pnl = total_closed_pnl
                    synced += 1
                await session.commit()

        log.info('[SYNC] Синхронизация завершена. Обработано позиций: %d, удалено: %d',
                 synced + len(existing), len(stale))
        return synced

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

        async with SessionLocal() as session:
            gs = await session.get(Settings, 1)
            if not gs:
                gs = Settings(id=1)
                session.add(gs)
                await session.commit()

            # --- 3. Определяем режим для НОВЫХ сигналов ---
            mode_id = getattr(gs, 'trade_mode', 'intraday') or 'intraday'
            mode = get_mode(mode_id)

            # Минимальный период прохода задаётся режимом (у скальпа — короче).
            scan_period = mode.scan_period_sec if mode.scan_period_sec > 0 else 60
            if now - self._last_scan_ts < scan_period:
                return
            self._last_scan_ts = now

            # --- 1. Собираем открытые сделки и их символы ---
            result = await session.execute(
                select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL']))
            )
            open_trades = list(result.scalars().all())
            open_syms = {t.symbol for t in open_trades}

            # --- 2. Фильтр max_open_trades ---
            max_open = gs.max_open_trades if gs and gs.max_open_trades > 0 else 5
            active_count = len({t.symbol for t in open_trades if t.status in ('OPEN', 'PARTIAL')})
            if active_count >= max_open:
                return

            symbols_to_check = [s for s in state.symbols.keys() if s not in open_syms]

            # --- 3c. Ожидаемые закрытия свечей по радар-сигналам ---
            # Обновляем и чистим «wait»-информацию в алертах монитора: UI
            # показывает, какой свечи ждёт бот, когда она закроется и сколько
            # осталось. Работает даже если ниже есть открытые сделки (return).
            try:
                self._refresh_wait_info(symbols_to_check, mode_id)
            except Exception as e:
                log.debug('[WAIT-TF] refresh failed: %r', e)

            if not symbols_to_check:
                return

            # --- 3b. Конфиги монет (min_confidence и пр.) — один запрос на проход ---
            cfg_map = {}
            try:
                cfg_res = await session.execute(select(SymbolConfig))
                cfg_map = {c.symbol: c for c in cfg_res.scalars().all()}
            except Exception as e:
                log.warning('[SCAN] SymbolConfig load failed: %r', e)

            # --- 4. Проход по символам с ограничением частоты ---
            scanned = 0
            for sym in symbols_to_check:
                cfg = cfg_map.get(sym)
                st = state.symbols.get(sym)
                if not st or st.last_price <= 0:
                    continue

                # --- 5. Получаем индикаторы режима (кеш + калл с лимитом) ---
                cache_key = (sym, mode.id)
                cached = self._mode_cache.get(cache_key)
                if cached and (now - cached['ts']) < 120:
                    atr_work = cached['atr']
                    levels_work = cached['levels']
                    kl_work = cached.get('kl')  # восстанавливаем для find_pins_with_confirm
                else:
                    try:
                        async with self._kline_sem:
                            kl_work = await self._fetch_klines_safe(sym, mode.work_tf, self.KLINES_PER_REQUEST)
                    except Exception:
                        kl_work = None
                    if kl_work and len(kl_work) >= 30:
                        atr_work = calc_atr(kl_work, period=14)
                        levels_work = find_levels(kl_work, pivot_window=3)
                    else:
                        atr_work = None
                        levels_work = []
                    self._mode_cache[cache_key] = {'atr': atr_work, 'levels': levels_work, 'kl': kl_work, 'ts': now}

                if not atr_work or atr_work <= 0:
                    log.debug('[SKIP] %s mode=%s: no ATR', sym, mode.id)
                    continue
                if not levels_work:
                    # Без уровней нет контекста
                    log.debug('[SKIP] %s mode=%s: no SR levels (need levels for entry)', sym, mode.id)
                    continue
                if not kl_work or len(kl_work) < 3:
                    # find_pins_with_confirm требует хотя бы несколько свечей
                    log.debug('[SKIP] %s mode=%s: not enough klines (%d)', sym, mode.id, len(kl_work) if kl_work else 0)
                    continue

                # --- 6. Получаем якорный ТФ для тренда и пространства ---
                try:
                    async with self._kline_sem:
                        kl_anchor = await self._fetch_klines_safe(sym, mode.anchor_tf, self.KLINES_PER_REQUEST)
                except Exception:
                    kl_anchor = None
                if not kl_anchor or len(kl_anchor) < 30:
                    kl_anchor = None
                # use_open=True: незакрытая свеча якоря участвует в тренде —
                # не ждём закрытия 1H/D бара (иначе окно подтверждения короче
                # жизни свечи якоря и сигнал протухает).
                anchor_dir, anchor_strength = trend_by_closes(kl_anchor, use_open=True) if kl_anchor else ('flat', 0.0)

                # --- 7. Пин-бары с подтверждением ---
                pins = find_pins_with_confirm(
                    kl_work, levels_work, atr_work, st.last_price,
                    mode.pin_shadow_frac, mode.move_atr_mult, mode.move_pct_min,
                    lookback_candles=36, max_pin_age_candles=6,
                    level_tol_atr=mode.shadow_lookback_atr,
                )
                if not pins:
                    log.debug('[PINS] %s mode=%s: 0 pins found (shadow=%.2f move=%.2f%% atr=%.6f price=%.6f)',
                              sym, mode.id, mode.pin_shadow_frac, mode.move_pct_min, atr_work, st.last_price)
                    self._wait_tf.pop((sym, mode.id), None)
                else:
                    # Какую свечу рабочего ТФ ждёт бот: подтверждение пина —
                    # закрытие бара после свечи-кандидата. ts/confirm_ts пина —
                    # в МС; переводим в секунды. Дедлайн — конец окна подтверждения.
                    dur_sec = _candle_sec(kl_work) / 1000.0 if kl_work else 0.0
                    newest = max(pins, key=lambda p: p.get('confirm_ts') or p['ts'])
                    exp_open_ms = (newest.get('confirm_ts') or newest['ts'])
                    exp_open = int(exp_open_ms / 1000.0)
                    deadline = (newest.get('confirm_ts') or newest['ts']) / 1000.0 + mode.confirm_window_sec
                    if dur_sec > 0 and deadline > now:
                        self._wait_tf[(sym, mode.id)] = (exp_open, deadline)
                    else:
                        self._wait_tf.pop((sym, mode.id), None)

                for pin in pins:
                    side = pin['side']
                    level_price = pin['level_price']
                    extreme = pin['extreme']
                    pin_ts = pin['ts']

                    # Дедуп: этот пин уже проверялся в предыдущих проходах —
                    # не переоткрываем по нему сигнал каждый цикл.
                    seen_key = f"{sym}_{mode.id}_{int(pin_ts)}"
                    if seen_key in self._seen_candles:
                        continue

                    # Проверяем тренд якоря
                    if anchor_dir == 'flat':
                        # Флэт на старшем — можно, но понижаем уверенность
                        pass
                    elif anchor_dir == 'down' and side != 'LONG':
                        # Тренд вниз, ищем SHORT, но тренд против — пропускаем
                        continue
                    elif anchor_dir == 'up' and side != 'SHORT':
                        continue

                    # --- 8. Пространство до цели ---
                    if side == 'LONG':
                        target = nearest_level(levels_work, st.last_price, 'above', within_pct=15)
                    else:
                        target = nearest_level(levels_work, st.last_price, 'below', within_pct=15)

                    if target and target['price'] > 0:
                        if side == 'LONG':
                            dist_to_target = (target['price'] - st.last_price) / st.last_price * 100
                        else:
                            dist_to_target = (st.last_price - target['price']) / st.last_price * 100
                        # ATR-защита: если уровень слишком близко к entry (меньше 1 ATR),
                        # сигнал ненадёжен — RR будет ложно высокий из-за близкой цели
                        atr_pct_val = atr_work / st.last_price * 100 if st.last_price > 0 else 0
                        if atr_pct_val > 0 and dist_to_target < atr_pct_val * 0.8:
                            dist_to_target = atr_pct_val * 1.2  # растягиваем до 1.2 ATR
                    else:
                        # Нет уровня — ориентируемся на ATR (3..5 ATR для разных режимов)
                        atr_mult = 3.0 if mode.id == 'scalp' else 5.0
                        dist_to_target = atr_mult * atr_work / st.last_price * 100 if st.last_price > 0 else 10.0

                    # Стоп: за экстремумом тени пина
                    if side == 'LONG':
                        stop_distance = st.last_price - extreme
                    else:
                        stop_distance = extreme - st.last_price
                    stop_pct = stop_distance / st.last_price * 100 if st.last_price > 0 else 0.0

                    # RR = (dist_to_target) / (stop_pct)
                    rr = dist_to_target / stop_pct if stop_pct > 0 else 0.0
                    if rr < mode.space_rr_min:
                        log.debug('[SKIP-RR] %s mode=%s rr=%.2f < min=%.1f', sym, mode.id, rr, mode.space_rr_min)
                        continue

                    # --- 9. Уверенность ---
                    confidence = self._calc_pin_confidence(
                        pin, atr_work, anchor_dir, anchor_strength,
                        dist_to_target, stop_pct, rr, level_price, st.last_price,
                        mode,
                    )
                    # Порог: конфиг монеты > глобальные настройки > режим.
                    # (Раньше min_confidence из UI игнорировался.)
                    conf_min = _pick(cfg.min_confidence if cfg else 0,
                                     gs.min_confidence if gs else 0,
                                     mode.min_confidence) or mode.min_confidence
                    if confidence < conf_min:
                        log.debug('[SKIP-CONF] %s mode=%s conf=%.3f < min=%.2f', sym, mode.id, confidence, conf_min)
                        continue

                    # --- 10. Confirm window ---
                    # Окно отсчитываем от момента ПОДТВЕРЖДЕНИЯ (закрытие
                    # свечи-кандидата), а не от открытия пин-свечи: иначе
                    # retest/BOS на 4-6 свечах никогда бы не прошёл фильтр.
                    confirm_ts = pin.get('confirm_ts') or pin_ts
                    pin_age_sec = now - confirm_ts
                    if pin_age_sec > mode.confirm_window_sec:
                        # Время подтверждения вышло — не входим
                        continue

                    # --- 11. Открываем сделку ---
                    try:
                        await self._open_mode_trade(
                            session, sym, side, st.last_price, st.max_leverage,
                            cfg=cfg, gs=gs, change_pct=None,
                            fee_pct=gs.fee_pct if gs and gs.fee_pct > 0 else 0.055,
                            mode=mode,
                            details={
                                'mode': mode.id,
                                'work_tf': mode.work_label,
                                'anchor_tf': mode.anchor_label,
                                'confidence': round(confidence, 2),
                                'min_confidence': conf_min,
                                'anchor_trend': anchor_dir,
                                'anchor_strength': round(anchor_strength, 3),
                                'level_price': round(level_price, 8),
                                'extreme': round(extreme, 8),
                                'confirm_type': pin['confirm']['type'],
                                'rr': round(rr, 2),
                                'dist_target_pct': round(dist_to_target, 2),
                                'stop_pct': round(stop_pct, 2),
                                'atr_work': round(atr_work, 8),
                                'current_price': st.last_price,
                                'pin_index': pin['index'],
                                'pin_range_pct': pin.get('range_pct', 0),
                                'vol_ratio': pin.get('vol_ratio'),
                            },
                        )
                        log.info(
                            'SIGNAL %s %s | %s @ %.6f conf=%.2f rr=%.1f | '
                            'anchor=%s/%.2f | level=%.6f | confirm=%s',
                            side, sym, mode.label, st.last_price,
                            confidence, rr, anchor_dir, anchor_strength,
                            level_price, pin['confirm']['type']
                        )
                        active_count += 1
                        if active_count >= max_open:
                            break
                        break  # один сигнал на символ

                    except Exception as e:
                        log.warning('Open trade failed for %s %s: %r', sym, mode.id, e)
                        # Временные ошибки (сеть/лимиты/недостаток баланса):
                        # НЕ сжигаем пин — попробуем снова в следующем проходе.
                        msg = str(e).lower()
                        if isinstance(e, InsufficientBalance) or 'insufficient balance' in msg:
                            # Сигнал существует и ждёт входа, но баланс не
                            # позволяет открыть сделку. Регистрируем его в
                            # мониторе как радар-сигнал (до проверки баланса),
                            # чтобы UI показал сигнал + ожидание закрытия свечи.
                            self._signal_no_funds(sym, side, st.last_price, mode, pin, now)
                            continue
                        if any(t in msg for t in ('rate', 'timeout', 'temp')):
                            continue
                        seen_key = f"{sym}_{mode.id}_{int(pin['ts'])}"
                        self._seen_candles.add(seen_key)
                        break  # неустранимая ошибка — переходим к следующему пину

                # --- 12. Дедуп: помечаем пин как проверенный ---
                # Пин сжигается, только если он действительно протух
                # (подтверждение старше confirm_window) — тогда он не сможет
                # войти и в следующем проходе. Пины, отброшенные фильтрами,
                # которые зависят от ЖИВОГО рынка (тренд, RR, уверенность,
                # уровень), оставляем живыми: рынок меняется, сигнал может
                # стать валидным, пока пин не вышел из окна подтверждения.
                for pin in pins:
                    confirm_ts = pin.get('confirm_ts') or pin['ts']
                    if now - confirm_ts > mode.confirm_window_sec:
                        seen_key = f"{sym}_{mode.id}_{int(pin['ts'])}"
                        self._seen_candles.add(seen_key)

                scanned += 1

            log.debug('[SCAN] mode=%s: %d symbols scanned, %d open, %d max', mode.id, scanned, active_count, max_open)
            await session.commit()

    def _calc_pin_confidence(self, pin, atr_work, anchor_dir, anchor_strength,
                             dist_to_target, stop_pct, rr, level_price,
                             current_price, mode: ModeParams) -> float:
        """Комплексная уверенность сигнала: пин-бар + контекст + пространство.

        Максимум пересчитан: тень 0.30, объём 0.15, уровень 0.15, тренд 0.25,
        RR 0.15, подтверждение 0.10 = 1.00 (раньше максимум был ~0.80).

        Дополнительно: волатильность (ATR) влияет на уверенность. Для
        скальпа оптимальна умеренная волатильность — слишком тихий рынок
        не даёт пробежать цель, слишком бурный выбивает по стопу.
        """
        score = 0.0
        side = pin['side']
        if side == 'LONG':
            shadow_ratio = (pin['pin_low'] - min(pin['pin_open'], pin['pin_close'])) / max(pin['pin_high'] - pin['pin_low'], 0.0001)
        else:
            shadow_ratio = (max(pin['pin_open'], pin['pin_close']) - pin['pin_high']) / max(pin['pin_high'] - pin['pin_low'], 0.0001)
        shadow_score = min(shadow_ratio / 0.85, 1.0) * 0.30  # было 0.25
        score += shadow_score

        # 2. Объём (0..0.15)
        vol = pin.get('vol_ratio')
        if vol and vol > 0:
            vol_score = min(vol / 2.0, 1.0) * 0.15
            score += vol_score

        # 3. Сила уровня SR (0..0.15)
        score += min(pin.get('level_strength', 0.5) / 1.0, 1.0) * 0.15

        # 4. Тренд якоря (0..0.25)
        if anchor_dir == 'flat':
            score += 0.10
        elif anchor_dir == side:
            score += 0.15 + min(anchor_strength, 1.0) * 0.10

        # 5. Пространство RR (0..0.15)
        rr_score = min(rr / (mode.space_rr_min * 2.0), 1.0) * 0.15
        score += rr_score

        # 6. Подтверждение входа (0..0.10)
        confirm = pin.get('confirm', {})
        ct = confirm.get('type', '')
        if ct == 'close':
            score += 0.10
        elif ct == 'bos':
            score += 0.10
        elif ct == 'retest':
            score += 0.07

        # 7. Волатильность (ATR) — штраф/бонус за режим (±0.10)
        # ATR в % от цены: optimal band для скальпа 0.15%..1.2%.
        if atr_work and current_price > 0:
            atr_pct = atr_work / current_price * 100.0
            if mode.id == 'scalp':
                if atr_pct < 0.08:
                    score -= 0.08   # тишина — цель не пробегает, комиссии съедят
                elif atr_pct <= 1.2:
                    score += 0.05   # оптимальная полоса скальпа
                elif atr_pct > 2.5:
                    score -= 0.05   # слишком бурно — стоп выбивается шумом
            elif mode.id == 'intraday':
                if atr_pct < 0.2:
                    score -= 0.06
                elif 0.4 <= atr_pct <= 2.5:
                    score += 0.04
            else:  # swing
                if atr_pct >= 0.8:
                    score += 0.04

        score = max(0.0, min(score, 1.0))
        return round(score, 3)

    async def _open_mode_trade(self, session, sym, side, price, max_lev,
                               cfg, gs, change_pct, fee_pct, mode, details):
        """Открывает сделку в режиме (scalp/intraday/swing) через Bybit API."""
        # --- Проверка баланса перед выставлением ордера ---
        try:
            bal = await self.rest.get_balance()
            avail = float(bal.get('available_balance') or 0)
        except Exception as e:
            log.warning('[ENTRY] Balance check failed for %s: %r', sym, e)
            avail = 0.0

        pos_size = _pick(cfg.position_size_entry if cfg else 0, 
                         gs.position_size_entry if gs else 0, MIN_POS_SIZE)
        pos_size = max(MIN_POS_SIZE, pos_size)

        # Уважаем контрактный минимум биржи: у BTC minQty=0.001 -> при цене
        # ~60000 это 60 USDT. Если плановый размер меньше minQty*price (или
        # minNotional), поднимаем позицию до минимума, иначе ордер отклонят
        # либо (хуже) он окажется в разы больше задуманного.
        min_notional = min_order_notional(sym, price)
        if pos_size < min_notional:
            log.warning('[ENTRY] %s: размер %.2f USDT < контрактного минимума %.2f USDT — поднимаю до минимума',
                        sym, pos_size, min_notional)
            pos_size = min_notional

        margin_needed = pos_size / max(max_lev, 1)
        if avail < margin_needed * 1.2:
            raise InsufficientBalance(
                f"Insufficient balance: available={avail:.2f} USDT, "
                f"need ~{margin_needed:.2f} USDT (+20% буфер) для {sym}"
            )

        # qty считаем от итогового размера — не меньше minQty контракта
        qty = normalize_qty(sym, pos_size / price) if price > 0 else 0.0
        if qty <= 0:
            raise Exception(f'Не удалось вычислить объём для {sym} (price={price})')
        pos_size = qty * price  # фактическая стоимость после округления лота
        margin = pos_size / max_lev
        entry_fee = pos_size * (fee_pct / 100.0)

        if cfg and cfg.max_steps > 0:
            max_steps = cfg.max_steps
        else:
            max_steps = gs.max_steps if (gs and gs.max_steps > 0) else mode.max_steps

        exit_max_steps = gs.exit_max_steps if gs and gs.exit_max_steps > 0 else 0

        # --- Отправляем реальный ордер на Bybit ---
        order_link_id = f"knife_{mode.id}_{uuid.uuid4().hex[:12]}"
        try:
            # Устанавливаем плечо (кэшируется на бирже)
            await self.rest.set_leverage(sym, max_lev, 'cross')

            # Market-ордер на вход
            bybit_side = 'Buy' if side == 'LONG' else 'Sell'
            order_result = await self.rest.create_order(
                symbol=sym,
                side=bybit_side,
                order_type='Market',
                qty=qty,
                order_link_id=order_link_id
            )

            # Получаем ID ордера для отслеживания
            bybit_order_id = (order_result.get('result', {})
                             .get('orderId', ''))
            
        except Exception as e:
            log.error('REAL ORDER failed for %s %s: %r', sym, side, e)
            raise

        trade = Trade(
            symbol=sym, side=side, entry_price=price, avg_entry_price=price,
            total_qty=qty, total_margin=margin, initial_margin=margin, status='OPEN',
            grid_step=0, max_steps=max_steps,
            leverage=max_lev, current_price=price, exit_step=0,
            entry_fee=entry_fee, pnl=-entry_fee,
            tf=mode.work_tf,
            mode=mode.id,
            validity='green',
            exit_max_steps=exit_max_steps,
            bybit_order_id=bybit_order_id,
            bybit_position_id=None,
        )
        session.add(trade)
        await session.flush()

        notes = f'{side} OPEN (mode={mode.label}, size: {pos_size:.2f} USDT, fee {entry_fee:.4f})'
        if change_pct is not None:
            notes += f', change: {change_pct:.2f}%'

        history = TradeHistory(
            trade_id=trade.id, action='OPEN', price=price, qty=qty,
            margin=margin, pnl=-entry_fee, notes=notes,
            details=json.dumps(details) if details else None
        )
        session.add(history)
        log.info('OPEN %s %s @ %.6f (mode: %s, size: %.2f USDT, qty: %.2f, lev: %.0fx, fee: %.4f, order: %s)',
                 side, sym, price, mode.label, pos_size, qty, max_lev, entry_fee,
                 bybit_order_id)

        # Показываем успешный вход во вкладке «Сигналы» (не только ошибки).
        self._alert_trade_opened(sym, side, price, mode.label, pos_size,
                                 qty, max_lev, source='bot')

    def _calc_grid_exit_plan(self, t, cfg, gs):
        """Полный план сетки усреднений и ступенчатых выходов (для истории/снимков)."""
        plan = {}
        try:
            mode = get_mode(getattr(t, 'mode', None))
            base_size = _pick(cfg.position_size_grid if cfg else 0, gs.position_size_grid if gs else 0, MIN_POS_SIZE)

            step_pct = self._grid_step_pct(t, cfg, gs, mode)
            size_mult = _pick(cfg.grid_size_multiplier if cfg else 0, gs.grid_size_multiplier if gs else 0, 1.0)
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

            exit_pct = self._exit_target_pct(t, cfg, gs, mode)
            close_pct = _pick(cfg.exit_contract_pct if cfg else 0, gs.exit_contract_pct if gs else 0, 50.0)
            exit_qty = t.total_qty * (close_pct / 100.0)
            if t.side == 'LONG':
                exit_price = t.avg_entry_price * (1 + exit_pct / 100)
            else:
                exit_price = t.avg_entry_price * (1 - exit_pct / 100)
            plan['next_exit'] = {
                'step': (t.exit_step or 0) + 1,
                'pct': round(exit_pct, 2),
                'price': round(exit_price, 8),
                'close_pct': close_pct,
                'qty': round(exit_qty, 6),
            }
        except Exception as e:
            log.debug('Grid/exit plan failed for %s: %r', getattr(t, 'symbol', '?'), e)
        return plan

    # --- Кеш ATR для управления сделкой: symbol -> {'atr': float, 'ts': now} ---
    _ATR_REFRESH_SEC = 300

    async def _trade_atr_pct(self, t) -> float:
        """Текущий ATR(14) рабочего ТФ режима сделки в % от цены."""
        mode = get_mode(getattr(t, 'mode', None))
        st = state.symbols.get(t.symbol)
        price = st.last_price if st and st.last_price > 0 else t.current_price
        if price <= 0:
            return 0.0
        kl = await self._fetch_klines_safe(t.symbol, mode.work_tf, 120)
        if not kl or len(kl) < 30:
            return 0.0
        a = calc_atr(kl, period=14)
        if not a:
            return 0.0
        return a / price * 100.0

    def _grid_step_pct(self, t, cfg, gs, mode=None) -> float:
        """Шаг сетки: режим задаёт базу (% или ATR-привязку), конфиг монеты
        и глобальные настройки могут переопределить фиксированный %."""
        if mode is None:
            mode = get_mode(getattr(t, 'mode', None))
        pct_override = _pick(cfg.grid_step_pct if cfg else 0,
                             gs.grid_step_pct if gs else 0, 0.0)
        atr_pct = getattr(t, '_atr_pct', 0.0) or 0.0
        if mode.grid_step_atr and atr_pct > 0:
            # В режиме расстояние = max(базовый %, N*ATR); override конфига — нижняя граница
            dist = resolve_distance(pct_override or mode.grid_step_pct,
                                    atr_pct, mode.grid_step_atr, 100.0)
            return dist
        return pct_override or mode.grid_step_pct or 5.0

    def _exit_target_pct(self, t, cfg, gs, mode=None) -> float:
        """Цель выхода текущей ступени: база режима (+шаг к цели из деталей входа),
        прогрессия по exit_step. Конфиги переопределяют базовый %."""
        if mode is None:
            mode = get_mode(getattr(t, 'mode', None))
        start_pct = _pick(cfg.initial_exit_pct if cfg else 0,
                          gs.initial_exit_pct if gs else 0, 0.0)
        mult = _pick(cfg.exit_step_multiplier if cfg else 0,
                     gs.exit_step_multiplier if gs else 0, 0.0) or mode.exit_step_multiplier
        exit_step = t.exit_step or 0
        base = start_pct or mode.initial_exit_pct or 15.0
        target = base * (mult ** exit_step)
        if mode.exit_step_atr:
            atr_pct = getattr(t, '_atr_pct', 0.0) or 0.0
            if atr_pct > 0:
                target = max(target, atr_pct * mode.exit_step_atr * (1 + exit_step * 0.35))
        # Если при входе было измерено пространство до уровня цели — выходим там,
        # но не раньше минимальной ступени (защита от слишком близких уровней).
        planned = getattr(t, '_planned_exit_pct', 0.0) or 0.0
        if planned > 0:
            target = min(max(planned, target), planned * 1.25)
        return target

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

    async def _fetch_trade_klines(self, symbol, interval, limit=30):
        """Приоритетная загрузка свечей для мониторинга открытых сделок.
        
        Использует отдельный кэш и семафор для контроля rate-лимитов.
        Приоритет: открытые сделки > новые сигналы.
        """
        now = time.time()
        cache_key = symbol
        interval_entry = self._trade_kline_cache.get(cache_key, {})
        
        # Проверяем кэш для данного интервала
        cached = interval_entry.get(interval)
        if cached and (now - cached[0]) < self.TRADE_KLINE_CACHE_TTL_SEC:
            return cached[1]
        
        async with self._kline_sem:
            try:
                kl = await asyncio.wait_for(
                    self.rest.get_kline(symbol, interval=interval, limit=limit),
                    timeout=self.REST_TIMEOUT_SEC
                )
                # Сохраняем в кэш
                if cache_key not in self._trade_kline_cache:
                    self._trade_kline_cache[cache_key] = {}
                self._trade_kline_cache[cache_key][interval] = (now, kl)
                
                # Также обновляем общий кэш (он используется для сигналов)
                self._kline_cache[(symbol, interval)] = (now, kl)
                return kl
            except Exception as e:
                log.warning('get_kline trade %s %s failed: %r, trying fallback',
                            symbol, interval, e)
                # Фоллбек на общий кэш
                fallback = self._kline_cache.get((symbol, interval))
                if fallback:
                    return fallback[1]
                raise

    async def _prune_trade_kline_cache(self):
        """Очищает устаревший кэш свечей для открытых сделок."""
        now = time.time()
        cutoff = now - self.TRADE_KLINE_CACHE_TTL_SEC
        symbols_to_remove = []
        
        for symbol, intervals in self._trade_kline_cache.items():
            intervals_to_remove = []
            for interval, (ts, _) in intervals.items():
                if ts < cutoff:
                    intervals_to_remove.append(interval)
            for interval in intervals_to_remove:
                del intervals[interval]
            if not intervals:
                symbols_to_remove.append(symbol)
        
        for symbol in symbols_to_remove:
            del self._trade_kline_cache[symbol]

    def _prune_caches(self):
        """Очищаем кеш свечей, seen-свечей и индикаторный кеш режимов.

        Формат ключа: '<symbol>_<mode_id>_<pin_start_ts_ms>'. Symbol может
        содержать '_' (например '1000PEPEUSDT'), поэтому ищем последний
        числовой хвост — это ts. Пустой хвост или нечисловой — удаляем.
        """
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.SEEN_RETENTION_MS
        pruned = set()
        for cid in self._seen_candles:
            tail = cid.rsplit('_', 1)[-1] if '_' in cid else ''
            try:
                ts_val = int(tail)
            except ValueError:
                continue
            if ts_val >= cutoff:
                pruned.add(cid)
        self._seen_candles = pruned
        # Индикаторный кеш режимов чистим по времени жизни записей
        stale_keys = [k for k, v in self._mode_cache.items()
                      if isinstance(v, dict) and time.time() - v.get('ts', 0) > 900]
        for k in stale_keys:
            del self._mode_cache[k]

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
            mode_v = get_mode(getattr(t, 'mode', None))
            v_tf = _validity_tf(mode_v)
            v_sec, v_confirm = _validity_struct_params(mode_v)
            kl_1h = await self._fetch_klines_safe(t.symbol, v_tf, 60)
            anchor_map = {'5': '60', '15': '240', '240': 'D'}
            kl_4h = await self._fetch_klines_safe(t.symbol, anchor_map.get(v_tf, '60'), 30)
            if not kl_1h or not kl_4h:
                return {}
            st = state.symbols.get(t.symbol)

            curr_price = st.last_price if st else t.current_price
            ma_1h = moving_average(kl_1h, min(20, len(kl_1h) - 1)) if len(kl_1h) > 5 else None
            ma_4h = moving_average(kl_4h, min(20, len(kl_4h) - 1)) if len(kl_4h) > 5 else None

            # Текущая и прошлая свеча на ТФ валидации
            c_curr = kl_1h[-1] if kl_1h else [0]*6
            c_prev = kl_1h[-2] if len(kl_1h) > 1 else [0]*6

            age_hours = _trade_age_hours(t)

            # Получаем актуальность разворота (с ATR-порогами режима)
            v = evaluate_validity(
                symbol=t.symbol, side=t.side,
                current_price=curr_price, avg_entry_price=t.avg_entry_price,
                klines_1h=kl_1h, klines_4h=kl_4h,
                ma_1h=ma_1h, ma_4h=ma_4h,
                age_hours=age_hours,
                mode_id=getattr(t, 'mode', None),
                atr_pct=getattr(t, '_atr_pct', 0.0) or 0.0,
                tf_sec=v_sec,
                min_hold_sec=_validity_struct_params(mode_v)[0],
                confirm_candles=v_confirm,
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

            # Реальный баланс с Bybit
            try:
                real_bal = await self.rest.get_balance()
                live_from_bybit = real_bal.get('wallet_value', 0) or real_bal.get('total_equity', 0)
                
                if session is not None:
                    realized_res = await session.execute(
                        select(sa_func.sum(TradeHistory.pnl))
                        .where(TradeHistory.action.in_(CLOSE_ACTIONS))
                    )
                    realized_total = realized_res.scalar() or 0.0
                    open_res = await session.execute(
                        select(sa_func.sum(Trade.pnl))
                        .where(Trade.status.in_(['OPEN', 'PARTIAL']))
                    )
                    open_total = open_res.scalar() or 0.0
                else:
                    async with SessionLocal() as s_sess:
                        realized_res = await s_sess.execute(
                            select(sa_func.sum(TradeHistory.pnl))
                            .where(TradeHistory.action.in_(CLOSE_ACTIONS))
                        )
                        realized_total = realized_res.scalar() or 0.0
                        open_res = await s_sess.execute(
                            select(sa_func.sum(Trade.pnl))
                            .where(Trade.status.in_(['OPEN', 'PARTIAL']))
                        )
                        open_total = open_res.scalar() or 0.0

                # Если есть данные с Bybit — используем их
                if live_from_bybit > 0:
                    snapshot["balance_realized"] = round(live_from_bybit, 4)
                    snapshot["balance_live"] = round(live_from_bybit, 4)
                else:
                    # Fallback: считаем от стартового 100 USDT (если API недоступен)
                    snapshot["balance_realized"] = round(100.0 + realized_total, 4)
                    snapshot["balance_live"] = round(100.0 + realized_total + open_total, 4)
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

        ТФ валидации = рабочий ТФ режима (не захардкоженные 1H/4H).
        Скальп 5m проверяется по 5m свечам, интрадэй 15m по 15m, свинг 4H по 4H.
        """
        try:
            mode = get_mode(getattr(t, 'mode', None))
            valid_tf = _validity_tf(mode)
            min_age_sec, confirm_candles = _validity_struct_params(mode)

            kl_work = await self._fetch_klines_safe(t.symbol, valid_tf, 60)
            # Старший якорь — на шаг выше: 1H для скальпа, 4H для интрадэй, D для свинг.
            anchor_tf_map = {'5': '60', '15': '240', '240': 'D'}
            kl_anchor = await self._fetch_klines_safe(t.symbol, anchor_tf_map.get(valid_tf, '60'), 30)

            if not kl_work or not kl_anchor:
                return None

            st = state.symbols.get(t.symbol)
            current_price = st.last_price if st and st.last_price > 0 else t.current_price

            # Возраст сделки в часах — от входа (не от последнего действия)
            age_hours = _trade_age_hours(t)

            v = evaluate_validity(
                symbol=t.symbol,
                side=t.side,
                current_price=current_price,
                avg_entry_price=t.avg_entry_price,
                klines_1h=kl_work,
                klines_4h=kl_anchor,
                ma_1h=moving_average(kl_work, min(20, len(kl_work) - 1)) if len(kl_work) > 5 else None,
                ma_4h=moving_average(kl_anchor, min(20, len(kl_anchor) - 1)) if len(kl_anchor) > 5 else None,
                age_hours=age_hours,
                mode_id=getattr(t, 'mode', None),
                atr_pct=getattr(t, '_atr_pct', 0.0) or 0.0,
                tf_sec=_VALID_TF_SEC.get(str(valid_tf), 3600),
                min_hold_sec=min_age_sec,
                confirm_candles=confirm_candles,
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
        Закрывает позицию полностью через Bybit API с фиксацией PnL и записью в историю.
        """
        gs = await session.get(Settings, 1)
        fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055

        closed_qty = t.total_qty

        # --- Отправляем реальный ордер на закрытие ---
        close_order_link_id = f"knife_validity_{t.id}_{uuid.uuid4().hex[:8]}"
        try:
            close_bybit_side = 'Sell' if t.side == 'LONG' else 'Buy'
            close_result = await self.rest.create_order(
                symbol=t.symbol,
                side=close_bybit_side,
                order_type='Market',
                qty=closed_qty,
                order_link_id=close_order_link_id,
                reduce_only=True
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            log.error('VALIDITY CLOSE order failed for %s #%s: %r', t.symbol, t.id, e)

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

    async def _periodic_position_sync(self, session):
        """Лёгкая периодическая синхронизация позиций с биржей (раз в 60 сек).

        Подхватывает сделки, открытые/закрытые/усреднённые вне бота:
        - новая позиция на бирже без записи в БД -> создаёт Trade;
        - внешнее усреднение -> обновляет qty/avg_entry/grid_step;
        - закрытая на бирже позиция -> закрывает сделку и фиксирует PnL.
        Вызывается из update_open_trades, чтобы не ждать рестарта.
        """
        if not hasattr(self, '_last_pos_sync'):
            self._last_pos_sync = time.time()
        if time.time() - self._last_pos_sync < 60:
            return
        self._last_pos_sync = time.time()
        try:
            await self.sync_open_positions()
        except Exception as e:
            log.warning('[SYNC-PERIODIC] failed: %r', e)

    def _alert_trade_opened(self, sym: str, side: str, price: float,
                            mode_label: str, size_usdt: float,
                            qty: float, leverage: float, source: str = 'bot') -> None:
        """Регистрирует сигнал «успешный вход» во вкладке «Сигналы».

        Алерты в UI не должны оставаться пустыми, когда сделка открывается:
        раньше сюда попадали только PUMP/DUMP от монитора и сигналы без
        средств, а реальные входы бота/подхваты синка — нет.
        Дедупликация: заменяем свежий (10 мин) радар-алерт по этому символу,
        чтобы не дублировать, а не добавлять новый.
        """
        now = time.time()
        reason = (f'✅ Вход {side} {mode_label} @ {price:.6f} '
                  f'• {size_usdt:.2f} USDT • {qty:g} контр. • {leverage:.0f}x'
                  + ('' if source == 'bot' else ' (подхвачено с биржи)'))
        for a in state.alerts:
            if a.symbol == sym and now - a.ts < 600:
                a.reason = reason
                a.price_now = price
                a.ts = now
                a.wait_tf_label = ''
                a.wait_candle_open_ts = 0
                a.wait_deadline_ts = 0
                a.tf_sec = 0
                return
        ref_price = state.price_n_minutes_ago(sym, settings.window_minutes) or price
        state.add_alert(Alert(
            ts=now,
            symbol=sym,
            kind='PUMP' if side == 'LONG' else 'DUMP',
            price_now=price,
            ref_price=ref_price,
            ma=None,
            change_pct=(price / ref_price - 1.0) * 100 if ref_price else 0.0,
            timeframe='',
            reason=reason,
            wait_tf_label='',
            wait_candle_open_ts=0,
            wait_deadline_ts=0,
            tf_sec=0,
        ))

    def _signal_no_funds(self, sym: str, side: str, price: float,
                         mode, pin, now: float) -> None:
        """Сигнал валиден, но маржи не хватает — регистрируем его как радар.

        Сделки нет, поэтому сигнал должен быть виден во вкладке «Сигналы»
        ДО проверки баланса: добавляем/обновляем алерт с причиной и
        ожиданием закрытия свечи рабочего ТФ (UI рисует обратный отсчёт).
        Дедупликация: не создаём дубликаты по символу каждые 10 минут.
        """
        dur = _interval_sec(mode.work_tf)
        confirm_ts = (pin.get('confirm_ts') or pin['ts']) / 1000.0
        deadline = confirm_ts + mode.confirm_window_sec
        if deadline <= now:
            return
        # Живой пин — обновляем окно ожидания всегда
        self._wait_tf[(sym, mode.id)] = (int(confirm_ts), deadline)
        reason = f'⚠ Сигнал {side} {mode.work_label}: мало маржи, вход отложен (пополните баланс)'
        # Не плодим алерты: если по символу уже есть свежий сигнал — только
        # обновляем окна и причину.
        for a in state.alerts:
            if a.symbol == sym and now - a.ts < 600:
                a.reason = reason
                a.wait_tf_label = mode.work_label
                a.wait_candle_open_ts = int(confirm_ts)
                a.wait_deadline_ts = deadline
                a.tf_sec = dur
                return
        ref_price = state.price_n_minutes_ago(sym, settings.window_minutes) or price
        state.add_alert(Alert(
            ts=now,
            symbol=sym,
            kind='PUMP' if side == 'LONG' else 'DUMP',
            price_now=price,
            ref_price=ref_price,
            ma=None,
            change_pct=(price / ref_price - 1.0) * 100 if ref_price else 0.0,
            timeframe=f'{settings.window_minutes}m',
            reason=reason,
            wait_tf_label=mode.work_label,
            wait_candle_open_ts=int(confirm_ts),
            wait_deadline_ts=deadline,
            tf_sec=dur,
        ))

    def _refresh_wait_info(self, symbols_to_check, mode_id: str) -> None:
        """Актуализирует в радар-алертах инфо об ожидаемой свече рабочего ТФ.

        Для алертов монитора (PUMP/DUMP) по символам без открытых сделок:
        если бот уже нашёл пин — ставим точные границы ожидаемой свечи и
        дедлайн окна подтверждения; если пин протух или его нет — снимаем
        ожидание (UI перестанет показывать отсчёт). Работает поверх кешей,
        сетевых запросов не делает.
        """
        now = time.time()
        checkable = set(symbols_to_check)
        # Живые пины: только те символы, что ещё сканируются (без позиции)
        for key in list(self._wait_tf.keys()):
            sym, mid = key
            if sym not in checkable or mid != mode_id:
                continue
            open_ts, deadline = self._wait_tf[key]
            if now > deadline:
                del self._wait_tf[key]
        for a in state.alerts:
            if a.wait_deadline_ts == 0.0 and a.symbol not in checkable:
                continue
            wait = self._wait_tf.get((a.symbol, mode_id))
            if wait is None:
                # Пина нет / он протух / есть открытая сделка — снимаем отсчёт,
                # но только для «сырых» алертов без причины входа.
                if not a.reason or 'Успешный вход' not in a.reason:
                    a.wait_deadline_ts = 0.0
                    a.wait_candle_open_ts = 0.0
                    a.wait_tf_label = ''
                    a.tf_sec = 0
                continue
            open_ts, deadline = float(wait[0]), float(wait[1])
            if now > deadline:
                del self._wait_tf[(a.symbol, mode_id)]
                if not a.reason or 'Успешный вход' not in a.reason:
                    a.wait_deadline_ts = 0.0
                    a.wait_candle_open_ts = 0.0
                    a.wait_tf_label = ''
                    a.tf_sec = 0
                continue
            a.wait_candle_open_ts, a.wait_deadline_ts = open_ts, deadline
            mode = get_mode(mode_id)
            if not a.wait_tf_label:
                a.wait_tf_label = mode.work_label
            a.tf_sec = _interval_sec(mode.work_tf)

    async def update_open_trades(self):
        # Периодическая очистка кеша seen-свечей (раз в час)
        now_ts = time.time()
        if now_ts - self._last_prune_ts > 3600:
            self._prune_caches()
            self._last_prune_ts = now_ts
            await self._prune_trade_kline_cache()

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

            # --- ПРИОРИТЕТНАЯ ЗАГРУЗКА СВЕЧЕЙ ДЛЯ ОТКРЫТЫХ СДЕЛОК ---
            # Загружаем 4h и D свечи для мониторинга до любых других операций
            # Это гарантирует что validity check и grid-логика работают на актуальных данных
            if open_trades:
                trade_symbols = {}
                for t in open_trades:
                    m = get_mode(getattr(t, 'mode', None))
                    v_tf = _validity_tf(m)
                    anchor_map = {'5': '60', '15': '240', '240': 'D'}
                    a_tf = anchor_map.get(v_tf, '60')
                    trade_symbols.setdefault(t.symbol, set()).update({v_tf, a_tf})
                log.debug('[PRIORITY] Loading live mode TF candles for %d open trades...', len(trade_symbols))
                for sym, tfs in trade_symbols.items():
                    for tf in sorted(tfs):
                        try:
                            await self._fetch_trade_klines(sym, tf, limit=60)
                            await asyncio.sleep(0.03)  # очередь с минимальной задержкой
                        except Exception as e:
                            log.warning('Failed to load %s klines for %s: %r', tf, sym, e)

            # --- ПЕРИОДИЧЕСКАЯ СИНХРОНИЗАЦИЯ ПОЗИЦИЙ С БИРЖОЙ ---
            # Подхват сделок, открытых/усреднённых/закрытых вне бота
            try:
                await self._periodic_position_sync(session)
            except Exception as e:
                log.warning('Periodic position sync failed: %r', e)
            
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

                # --- РЕЖИМ СДЕЛКИ (старые сделки дорабатывают свой режим) ---
                mode = get_mode(getattr(t, 'mode', None))
                # ATR рабочего ТФ режима (кеш 5 мин) — для ATR-привязки сетки/целей
                atr_key = f'atr_{t.symbol}_{mode.id}'
                atr_cached = self._mode_cache.get(atr_key)
                if atr_cached and (now_ts - atr_cached['ts']) < self._ATR_REFRESH_SEC:
                    t._atr_pct = atr_cached['atr']
                else:
                    try:
                        t._atr_pct = await self._trade_atr_pct(t)
                    except Exception:
                        t._atr_pct = 0.0
                    self._mode_cache[atr_key] = {'atr': t._atr_pct, 'ts': now_ts}
                # План выхода по уровню SR/ATR, измеренный при входе
                t._planned_exit_pct = 0.0
                try:
                    oh_hist = await session.execute(
                        select(TradeHistory.details)
                        .where(TradeHistory.trade_id == t.id)
                        .where(TradeHistory.action == 'OPEN')
                        .order_by(TradeHistory.ts.asc()).limit(1)
                    )
                    oh_details = oh_hist.scalar()
                    if oh_details:
                        oh = json.loads(oh_details)
                        dist_t = oh.get('dist_target_pct') or 0.0
                        if dist_t > 0:
                            t._planned_exit_pct = float(dist_t)
                except Exception:
                    pass

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


                # --- 1. ЛОГИКА УСРЕДНЕНИЯ (параметры режима) ---
                if t.grid_step < t.max_steps:
                    step_pct = self._grid_step_pct(t, cfg, gs, mode)
                    mult = _pick(cfg.grid_step_multiplier if cfg else 0,
                                 gs.grid_step_multiplier if gs else 0, 0.0) or mode.grid_step_multiplier

                    can_grid = False
                    if t.side == 'LONG' and price <= t.avg_entry_price * (1 - step_pct / 100):
                        can_grid = True
                    elif t.side == 'SHORT' and price >= t.avg_entry_price * (1 + step_pct / 100):
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

                        # --- Отправляем реальный ордер на усреднение ---
                        grid_order_link_id = f"knife_grid_{t.id}_{t.grid_step + 1}_{uuid.uuid4().hex[:8]}"
                        try:
                            bybit_side = 'Buy' if t.side == 'LONG' else 'Sell'
                            grid_result = await self.rest.create_order(
                                symbol=t.symbol,
                                side=bybit_side,
                                order_type='Market',
                                qty=add_qty,
                                order_link_id=grid_order_link_id
                            )
                            await asyncio.sleep(0.5)  # Даем ордеру исполниться
                            
                            # Проверяем позицию
                            positions = await self.rest.get_positions(t.symbol)
                            bybit_position_id = None
                            for pos in positions:
                                if pos.get('symbol') == t.symbol:
                                    bybit_position_id = pos.get('positionIdx')
                                    break
                            
                        except Exception as e:
                            log.error('GRID ORDER failed for %s #%s step %d: %r', 
                                     t.symbol, t.id, t.grid_step + 1, e)
                            continue

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

                # --- 1b. HARD SL: исчерпанные усреднения + пробитие сетки ---
                # Не «паника на лёгком испуге», а фиксация ПРОИГРАННОГО тезиса.
                # Закрываем только когда совпали ВСЕ условия:
                #   1) все max_steps усреднений уже сделаны;
                #   2) цена прошла ЗА ПОСЛЕДНИМ уровнем сетки ещё один полный
                #      шаг (структурное пробитие, а не шум у последнего уровня);
                #   3) разворот структурно краснеет ИЛИ сделка живёт дольше
                #      1.5 × штатного hold_max_hours (время вышло, а возврата нет).
                # На микро-балансе это отсекает хвостовые потери (висящие сутки
                # ноги), но нормально переживает просадки внутри сетки.
                age_h = _trade_age_hours(t) or 0.0
                if t.grid_step >= t.max_steps and t.max_steps > 0:
                    step_pct = self._grid_step_pct(t, cfg, gs, mode)
                    # Уровень пробития = последний уровень сетки минус ещё один шаг.
                    last_level = t.avg_entry_price * (1 - step_pct / 100.0) if t.side == 'LONG' \
                        else t.avg_entry_price * (1 + step_pct / 100.0)
                    breached = (t.side == 'LONG' and price <= last_level * (1 - step_pct / 100.0)) or \
                               (t.side == 'SHORT' and price >= last_level * (1 + step_pct / 100.0))
                    if breached:
                        hard_deadline_h = mode.hold_max_hours * 1.5 if mode.hold_max_hours else 0
                        overdue = hard_deadline_h > 0 and age_h >= hard_deadline_h
                        v_hard = validity if validity is not None else await self._check_trade_validity(t)
                        thesis_dead = (v_hard is not None and v_hard.status == 'red') or overdue
                        if thesis_dead:
                            reason_bits = []
                            if v_hard is not None and v_hard.status == 'red':
                                reason_bits.append('разворот мёртв: ' + ('; '.join(v_hard.reasons[:3]) or 'red'))
                            if overdue:
                                reason_bits.append(f'просрочка {age_h:.1f}ч > {hard_deadline_h:.0f}ч лимита')
                            log.warning('HARD SL %s #%s @ %.6f | шаги %d/%d исчерпаны, сетка пробита | %s',
                                        t.symbol, t.id, price, t.grid_step, t.max_steps,
                                        ' + '.join(reason_bits))
                            await self._close_position(
                                session, t, price,
                                reason='HARD SL: ' + ' + '.join(reason_bits)
                            )
                            continue

                # --- 2. РАСЧЕТ PnL ---
                if t.side == 'LONG':
                    change_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    change_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100

                gross = t.total_qty * (price - t.avg_entry_price) if t.side == 'LONG' \
                    else t.total_qty * (t.avg_entry_price - price)
                open_exit_fee = t.total_qty * price * (fee_pct / 100.0)
                t.pnl = t.realized_pnl + gross - t.entry_fee - open_exit_fee
                
                # --- 3. СТУПЕНЧАТЫЙ ВЫХОД (параметры режима + план по SR-цели) ---
                close_pct = _pick(cfg.exit_contract_pct if cfg else 0, gs.exit_contract_pct if gs else 0, 50.0)

                current_exit_step = t.exit_step if t.exit_step is not None else 0
                target_exit_pct = self._exit_target_pct(t, cfg, gs, mode)

                # --- 3b. ВЫХОД ПО ВРЕМЕНИ РЕЖИМА (макс. время удержания) ---
                time_exit = mode.hold_max_hours and age_h >= mode.hold_max_hours

                exit_max = t.exit_max_steps if getattr(t, 'exit_max_steps', 0) else (gs.exit_max_steps if gs and gs.exit_max_steps > 0 else 0)
                force_full_close = (exit_max > 0 and (current_exit_step + 1) >= exit_max) or time_exit

                if change_pct >= target_exit_pct or time_exit:
                    if (force_full_close
                            or t.total_qty * (close_pct / 100.0) * price < 1.0
                            or t.total_qty * (1 - close_pct / 100.0) < 0.001):
                        # --- FULL CLOSE через Bybit API ---
                        closed_qty = t.total_qty
                        closed_frac = 1.0
                        action = 'FULL_CLOSE'
                        
                        close_order_link_id = f"knife_close_full_{t.id}_{uuid.uuid4().hex[:8]}"
                        try:
                            close_bybit_side = 'Sell' if t.side == 'LONG' else 'Buy'
                            close_result = await self.rest.create_order(
                                symbol=t.symbol,
                                side=close_bybit_side,
                                order_type='Market',
                                qty=closed_qty,
                                order_link_id=close_order_link_id,
                                reduce_only=True
                            )
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            log.error('FULL CLOSE order failed for %s #%s: %r', t.symbol, t.id, e)

                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()

                    else:
                        # --- PARTIAL CLOSE через Bybit API ---
                        closed_qty = t.total_qty * (close_pct / 100.0)
                        closed_frac = close_pct / 100.0
                        action = 'PARTIAL_CLOSE'
                        t.exit_step = current_exit_step + 1
                        t.grid_step = 0

                        close_order_link_id = f"knife_close_part_{t.id}_{t.exit_step}_{uuid.uuid4().hex[:8]}"
                        try:
                            close_bybit_side = 'Sell' if t.side == 'LONG' else 'Buy'
                            close_result = await self.rest.create_order(
                                symbol=t.symbol,
                                side=close_bybit_side,
                                order_type='Market',
                                qty=closed_qty,
                                order_link_id=close_order_link_id,
                                reduce_only=True
                            )
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            log.error('PARTIAL CLOSE order failed for %s #%s: %r', t.symbol, t.id, e)

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