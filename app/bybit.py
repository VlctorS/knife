import asyncio
import json
import logging
from typing import List

import aiohttp
import websockets

log = logging.getLogger(__name__)

REST_BASE = 'https://api.bybit.com'
WS_URL = 'wss://stream.bybit.com/v5/public/linear'


async def _read_json(resp):
    raw = await resp.text(encoding='utf-8', errors='replace')
    if raw.startswith('\ufeff'):
        raw = raw[1:]
    return json.loads(raw)


class BybitREST:
    def __init__(self, session):
        self._s = session

    async def get_instruments(self):
        out = []
        cursor = ''
        for _ in range(20):
            params = {'category': 'linear'}
            if cursor:
                params['cursor'] = cursor
            async with self._s.get(REST_BASE + '/v5/market/instruments-info', params=params) as r:
                r.raise_for_status()
                data = await _read_json(r)
            if data.get('retCode') != 0:
                log.warning('instruments retCode=%s', data.get('retCode'))
                break
            result = data.get('result', {})
            for it in result.get('list', []):
                if it.get('contractType') != 'LinearPerpetual':
                    continue
                if it.get('quoteCoin') != 'USDT':
                    continue
                if it.get('status') != 'Trading':
                    continue
                out.append(it)
            cursor = result.get('nextPageCursor') or ''
            if not cursor:
                break
        return out

    async def get_max_leverage_map(self):
        best = {}
        cursor = ''
        pages = 0
        for _ in range(40):
            params = {'category': 'linear', 'limit': '1000'}
            if cursor:
                params['cursor'] = cursor
            try:
                async with self._s.get(REST_BASE + '/v5/market/risk-limit', params=params) as r:
                    r.raise_for_status()
                    data = await _read_json(r)
            except Exception as e:
                log.warning('risk-limit page %d failed: %r', pages, e)
                break
            pages += 1
            if data.get('retCode') != 0:
                log.warning('risk-limit page %d retCode=%s retMsg=%s',
                            pages - 1, data.get('retCode'), data.get('retMsg'))
                break
            result = data.get('result', {})
            for it in result.get('list', []):
                sym = it.get('symbol')
                if not sym:
                    continue
                try:
                    ml = float(it.get('maxLeverage') or 0)
                except (TypeError, ValueError):
                    continue
                if ml > best.get(sym, 0.0):
                    best[sym] = ml
            cursor = result.get('nextPageCursor') or ''
            if not cursor:
                break
            await asyncio.sleep(0.15)
        log.info('risk-limit pages=%d, unique symbols=%d', pages, len(best))
        return best

    async def get_kline_1h(self, symbol, limit=200):
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': '60',
            'limit': str(min(limit, 1000)),
        }
        async with self._s.get(REST_BASE + '/v5/market/kline', params=params) as r:
            r.raise_for_status()
            data = await _read_json(r)
        lst = data.get('result', {}).get('list', [])
        lst.reverse()
        return lst


    async def get_tickers(self):
        params = {'category': 'linear'}
        async with self._s.get(
            f'{REST_BASE}/v5/market/tickers', params=params
        ) as r:
            r.raise_for_status()
            data = await _read_json(r)
        if data.get('retCode') != 0:
            log.warning('tickers retCode=%s retMsg=%s',
                        data.get('retCode'), data.get('retMsg'))
            return []
        return data.get('result', {}).get('list', [])


class BybitTickerWS:
    def __init__(self, symbols, batch_size=10):
        self._symbols = symbols
        self._batch_size = batch_size
        self._queue = asyncio.Queue(maxsize=10000)

    async def run(self):
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                    log.info('WS connected for %d symbols', len(self._symbols))
                    await self._subscribe(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            msg = json.loads(raw)
                            if 'data' in msg and 'topic' in msg:
                                if msg['topic'].startswith('tickers.'):
                                    await self._queue.put(msg)
                    finally:
                        ping_task.cancel()
                    backoff = 1.0
            except Exception as e:
                log.warning('WS error: %s -- reconnect in %.1fs', e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _subscribe(self, ws):
        for i in range(0, len(self._symbols), self._batch_size):
            batch = self._symbols[i : i + self._batch_size]
            args = ['tickers.' + s for s in batch]
            await ws.send(json.dumps({'op': 'subscribe', 'args': args}))
            await asyncio.sleep(0.1)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({'op': 'ping'}))
            except Exception:
                return

    async def messages(self):
        while True:
            yield await self._queue.get()


def chunked(lst, n):
    return [lst[i : i + n] for i in range(0, len(lst), n)]
