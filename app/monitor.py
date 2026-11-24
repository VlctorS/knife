import asyncio
import logging
import time
from typing import List

import aiohttp

from .bybit import BybitREST, BybitTickerWS, chunked
from .config import settings
from .state import Alert, state
from .db import SessionLocal, Settings

log = logging.getLogger(__name__)


class Monitor:
    def __init__(self) -> None:
        self._session = None
        self._rest = None
        self._tasks = []
        self._ws_tasks = []
        self._stop = asyncio.Event()
        self._loop = None
        # (ts, ModeParams|None) — кеш текущего режима для _attach_wait
        self._mode_cache = (0.0, None)

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._session = aiohttp.ClientSession()
        self._rest = BybitREST(self._session)
        self._tasks.append(asyncio.create_task(self._symbols_loop(), name='symbols'))
        self._tasks.append(asyncio.create_task(self._check_loop(), name='check'))
        log.info('Monitor started')

    async def stop(self) -> None:
        self._stop.set()
        for t in self._ws_tasks + self._tasks:
            t.cancel()
        if self._session:
            await self._session.close()

    async def _symbols_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._refresh_symbols()
            except Exception:
                log.exception('symbols refresh failed')
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=settings.symbols_refresh_sec
                )
            except asyncio.TimeoutError:
                pass

    async def _refresh_symbols(self) -> None:
        assert self._rest is not None
        instr = await self._rest.get_instruments()
        lev_map = await self._rest.get_max_leverage_map()
        
        tickers = await self._rest.get_tickers()
        turnover_map = {}
        for t in tickers:
            sym = t.get('symbol')
            try:
                turnover_map[sym] = float(t.get('turnover24h') or 0)
            except ValueError:
                pass

        log.info('instruments=%d, risk-limit symbols=%d, tickers=%d',
                 len(instr), len(lev_map), len(turnover_map))

        db_min_lev = settings.min_max_leverage
        db_min_turnover = 5.0
        async with SessionLocal() as session:
            s = await session.get(Settings, 1)
            if s:
                db_min_lev = s.min_leverage
                db_min_turnover = s.min_turnover

        filtered = []
        for it in instr:
            sym = it['symbol']
            ml = lev_map.get(sym)
            if ml is None:
                continue
            if ml < db_min_lev:
                continue
                
            turnover = turnover_map.get(sym, 0) / 1_000_000
            if turnover < db_min_turnover:
                continue
                
            filtered.append({'symbol': sym, 'maxLeverage': ml})
            
        filtered.sort(key=lambda x: x['symbol'])

        if not filtered:
            log.warning('No symbols after filter (minLev=%s, minTurnover=%sM)', db_min_lev, db_min_turnover)
            return

        state.set_symbols(filtered)
        state.last_symbols_refresh = time.time()
        log.info('Symbols loaded: %d (filter maxLeverage>=%s)', len(filtered), db_min_lev)

        await self._rebuild_ws([d['symbol'] for d in filtered])
        await self._load_ma()

    async def _rebuild_ws(self, symbols: List[str]) -> None:
        for t in self._ws_tasks:
            t.cancel()
        self._ws_tasks.clear()
        chunks = chunked(symbols, 100)
        for idx, grp in enumerate(chunks):
            ws = BybitTickerWS(grp, batch_size=10)
            self._ws_tasks.append(
                asyncio.create_task(self._ws_runner(ws), name=f'ws-{idx}')
            )

    async def _ws_runner(self, ws: BybitTickerWS) -> None:
        reader = asyncio.create_task(self._ws_reader(ws))
        try:
            await ws.run()
        finally:
            reader.cancel()

    async def _ws_reader(self, ws: BybitTickerWS) -> None:
        async for msg in ws.messages():
            try:
                symbol = msg['topic'].split('.', 1)[1]
                d = msg['data']
                price = float(d.get('lastPrice') or 0)
                ts = float(d.get('ts') or time.time() * 1000) / 1000.0
                if price > 0:
                    state.on_tick(symbol, price, ts)
            except Exception:
                log.exception('ws msg parse')

    async def _load_ma(self) -> None:
        assert self._rest is not None
        symbols = list(state.symbols.keys())
        sem = asyncio.Semaphore(5)

        async def load(sym: str) -> None:
            async with sem:
                try:
                    assert self._rest is not None
                    kl = await self._rest.get_kline_1h(sym, limit=settings.ma_period + 5)
                except Exception:
                    return
                closes = [float(k[4]) for k in kl]
                if len(closes) >= settings.ma_period:
                    ma = sum(closes[-settings.ma_period :]) / settings.ma_period
                    st = state.symbols.get(sym)
                    if st:
                        st.ma = ma

        await asyncio.gather(*(load(s) for s in symbols), return_exceptions=True)
        log.info('MA loaded for %d symbols',
                 sum(1 for s in state.symbols.values() if s.ma))

    async def _ma_refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=600)
                return
            except asyncio.TimeoutError:
                await self._load_ma()

    async def _check_loop(self) -> None:
        await asyncio.sleep(2)
        asyncio.create_task(self._ma_refresh_loop(), name='ma-refresh')
        while not self._stop.is_set():
            try:
                self._check_all()
                import sys
                main_mod = sys.modules.get('app.main')
                bot = getattr(main_mod, 'trading_bot', None) if main_mod else None
                if bot:
                    await bot.check_for_entries()
                    await bot.update_open_trades()
            except Exception:
                log.exception('check loop')
            await asyncio.sleep(2)

    def _check_all(self) -> None:
        now = time.time()
        for sym, s in state.symbols.items():
            if s.last_price <= 0 or s.last_price_ts <= 0:
                continue
            ref = state.price_n_minutes_ago(sym, settings.window_minutes)
            if ref is None or ref <= 0:
                continue
            change = s.last_price / ref - 1.0
            if abs(change) < settings.threshold:
                continue
            if now - s.last_price_ts > 300:
                continue
            kind = 'PUMP' if change > 0 else 'DUMP'
            
            # Дедупликация
            for a in state.alerts:
                if a.symbol == sym and now - a.ts < 600:
                    break
            else:
                alert = Alert(
                    ts=now,
                    symbol=sym,
                    kind=kind,
                    price_now=s.last_price,
                    ref_price=ref,
                    ma=s.ma,
                    change_pct=change * 100,
                    timeframe=f'{settings.window_minutes}m'  # Добавляем ТФ
                )
                state.add_alert(alert)
                self._attach_wait(sym, alert)
                log.warning('ALERT %s %s %+.2f%% (%.6f vs %.6f)',
                            kind, sym, change * 100, s.last_price, ref)

    def _attach_wait(self, sym: str, alert) -> None:
        """Привязывает к алерту ожидание закрытия свечи рабочего ТФ режима.

        Бот ждёт не «4ч/1д» как было раньше в UI-хардкоде, а конкретный бар
        рабочего ТФ выбранного режима (скальп — 5m, интрадэй — 15m, лонг — 4H),
        в пределах окна подтверждения пин-сигнала. Если бот уже нашёл пин по
        этому символу — берём точные границы ожидаемой свечи; иначе считаем
        от текущего бара. Режим кешируется (30с), свечей не запрашиваем.
        """
        try:
            mode = self._current_mode()
            if mode is None:
                return
            from .modes import interval_sec
            dur = interval_sec(mode.work_tf)
            now = time.time()
            open_ts = int(now // dur) * dur
            deadline = now + mode.confirm_window_sec
            import sys
            bot = getattr(sys.modules.get('app.main'), 'trading_bot', None)
            wait = getattr(bot, '_wait_tf', {}).get((sym, mode.id)) if bot else None
            if wait:
                open_ts = max(int(wait[0]), open_ts)
                deadline = min(float(wait[1]), deadline)
            if deadline <= now:
                return
            alert.wait_tf_label = mode.work_label
            alert.wait_candle_open_ts = open_ts
            alert.wait_deadline_ts = deadline
            alert.tf_sec = dur
        except Exception:
            log.debug('attach wait info failed for %s', sym, exc_info=True)

    def _current_mode(self):
        """Текущий режим торговли (из БД, кеш 30с)."""
        now = time.time()
        if self._mode_cache and now - self._mode_cache[0] < 30:
            return self._mode_cache[1]
        mode = None
        try:
            fut = asyncio.run_coroutine_threadsafe(self._load_trade_mode(), self._loop)
            mode = fut.result(timeout=3)
        except Exception:
            log.debug('load trade_mode failed', exc_info=True)
        # При сбое кеша оставляем прошлый режим, если он был.
        self._mode_cache = (now, mode or (self._mode_cache[1] if self._mode_cache else None))
        return self._mode_cache[1]

    async def _load_trade_mode(self):
        from .modes import get_mode
        async with SessionLocal() as session:
            s = await session.get(Settings, 1)
            return get_mode(getattr(s, 'trade_mode', None) if s else None)


monitor = Monitor()