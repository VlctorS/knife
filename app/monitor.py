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

    async def start(self) -> None:
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

        # Читаем настройки из БД (или используем дефолт)
        db_min_lev = settings.min_max_leverage
        db_min_turnover = 5.0
        async with SessionLocal() as session:
            s = await SessionLocal().get(Settings, 1)
            if s:
                db_min_lev = s.min_leverage
                db_min_turnover = s.min_turnover

        filtered = []
        for it in instr:
            sym = it['symbol']
            ml = lev_map.get(sym)
            if ml is None:
                continue
            
            # Сохраняем реальное плечо (например, 25, 50, 100)
            real_leverage = ml 
            
            if real_leverage < db_min_lev:
                continue
                
            turnover = turnover_map.get(sym, 0) / 1_000_000
            if turnover < db_min_turnover:
                continue
                
            # Передаем реальное плечо в стейт
            filtered.append({'symbol': sym, 'maxLeverage': real_leverage})
            
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
                # Вызовы торгового бота
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
            for a in state.alerts:
                if a.symbol == sym and now - a.ts < 600:
                    break
            else:
                state.add_alert(
                    Alert(
                        ts=now,
                        symbol=sym,
                        kind=kind,
                        price_now=s.last_price,
                        ref_price=ref,
                        ma=s.ma,
                        change_pct=change * 100,
                    )
                )
                log.warning('ALERT %s %s %+.2f%% (%.6f vs %.6f)',
                            kind, sym, change * 100, s.last_price, ref)


monitor = Monitor()
