import asyncio
import json
import logging
import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
from typing import List

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

import aiohttp
import websockets

log = logging.getLogger(__name__)

REST_BASE = 'https://api.bybit.com'
WS_URL = 'wss://stream.bybit.com/v5/public/linear'

# Загружаем после load_dotenv() — иначе ключи будут пустые
_api_key_raw = os.getenv('BYBIT_API_KEY', '')
_api_secret_raw = os.getenv('BYBIT_API_SECRET', '')
# Debug
import sys
_log = logging.getLogger(__name__)
_log.warning('[BYBIT-DEBUG] os.getenv keys: KEY=%s SECRET=%s', repr(_api_key_raw[:10] if _api_key_raw else None), repr(_api_secret_raw[:10] if _api_secret_raw else None))

API_KEY = _api_key_raw
API_SECRET = _api_secret_raw

if API_KEY:
    log.info('[BYBIT] API ключи загружены: %s...', API_KEY[:8])
else:
    log.warning('[BYBIT] API ключи НЕ загружены — работает только публичное')

# --- Синхронизация времени с сервером Bybit ---
# Локальные часы могут уходить от биржевого времени (дрейф, NTP, часовой
# пояс). Bybit отклоняет подписанные запросы, если |timestamp - serverTime|
# превышает recv_window (5000 мс): 'ErrCode 10002 timestamp not in recvWindow'.
# Храним оффсет (server_time - local_time) в мс и подписываем им все запросы.
_time_offset_ms = 0.0
_time_sync_ts = 0.0
TIME_SYNC_INTERVAL_SEC = 1800  # пересинхронизация раз в 30 минут


def _server_now_ms() -> int:
    """Текущее время в мс с учётом оффсета к серверу Bybit."""
    return int(time.time() * 1000 + _time_offset_ms)


def time_offset_ms() -> float:
    """Текущий оффсет локального времени к серверу (мс)."""
    return _time_offset_ms


async def sync_server_time(session=None) -> float:
    """Синхронизирует время с сервером Bybit (/v5/market/time).

    Оффсет = server_time - локальное_время в момент ответа. Учитываем
    сетевую задержку: берём середину между отправкой и получением, чтобы
    не занизить/завысить оффсет на RTT/2.
    Возвращает вычисленный оффсет (мс). При сбое оставляет прежний.
    """
    global _time_offset_ms, _time_sync_ts
    url = REST_BASE + '/v5/market/time'
    own_session = session is None
    s = session
    if own_session:
        s = aiohttp.ClientSession(headers={'Accept': 'application/json'})
    try:
        t_before = time.time()
        async with s.get(url) as r:
            r.raise_for_status()
            data = await _read_json(r)
        t_after = time.time()
        if data.get('retCode') != 0:
            log.warning('[TIME] sync retCode=%s retMsg=%s',
                        data.get('retCode'), data.get('retMsg'))
            return _time_offset_ms
        server_ms = int(data.get('result', {}).get('timeSecond', 0)) * 1000 \
            + int(data.get('result', {}).get('timeNano', 0)) % 1_000_000_000 // 1_000_000
        if server_ms <= 0:
            log.warning('[TIME] sync: пустой serverTime в ответе')
            return _time_offset_ms
        local_ms = (t_before + t_after) / 2.0 * 1000.0
        new_offset = server_ms - local_ms
        _time_offset_ms = new_offset
        _time_sync_ts = time.time()
        log.info('[TIME] синхронизация с Bybit: оффсет %.0f мс (локально %+.1f с)',
                 new_offset, new_offset / 1000.0)
        return new_offset
    except Exception as e:
        log.warning('[TIME] sync failed: %r', e)
        return _time_offset_ms
    finally:
        if own_session and s is not None:
            await s.close()


async def time_sync_loop(session=None, interval_sec: int = TIME_SYNC_INTERVAL_SEC):
    """Фоновая задача: периодически пересинхронизирует время с сервером."""
    while True:
        try:
            await sync_server_time(session)
        except Exception as e:
            log.warning('[TIME] loop error: %r', e)
        await asyncio.sleep(interval_sec)

# Кэш instrument info: {symbol: {qtyStep, priceStep, minQty, maxQty, tickSize, ...}}
_instruments_cache = {}
_instruments_ts = {}
_INSTR_TTL = 3600  # 1 час


def _round_to_tick(value, step, round_down=True):
    """
    Округляет значение до шага тика (qtyStep / priceStep).
    
    round_down=True  → вниз для qty (меньше контрактов = меньше риск)
    round_down=False → вверх для qty (больше контрактов)
    Для цены: round_down=True → вниз, round_down=False → вверх
    """
    step = float(step)
    if step <= 0:
        return value
    
    rounded = value / step
    if round_down:
        rounded = int(rounded)
    else:
        rounded = int(rounded) + (1 if rounded != int(rounded) else 0)
    return rounded * step


async def _load_instruments_cache(rest):
    """Загружает и кэширует информацию о контрактах (qtyStep, priceStep, minQty, ...)."""
    global _instruments_cache, _instruments_ts
    
    now = time.time()
    if now - _instruments_ts.get('last_load', 0) < _INSTR_TTL and _instruments_cache:
        return _instruments_cache
    
    try:
        instruments = await rest.get_instruments()
        for inst in instruments:
            symbol = inst.get('symbol', '')
            # v5 /market/instruments-info отдаёт готовые объекты priceFilter /
            # lotSizeFilter (а не массив filters[] как в старой v3). Старый
            # парсер не находил ничего -> кэш пустой -> везде fallback minQty=0.001,
            # из-за чего BTC входил на 60 USDT вместо минимальных ~5.
            price_f = inst.get('priceFilter') or {}
            lot_f = inst.get('lotSizeFilter') or {}

            info = {'symbol': symbol}
            if price_f:
                info['priceStep'] = float(price_f.get('tickSize', '0.01'))
                info['minPrice'] = float(price_f.get('minPrice', '0'))
                info['maxPrice'] = float(price_f.get('maxPrice', '999999'))
                info['tickSize'] = info['priceStep']

            if lot_f:
                info['qtyStep'] = float(lot_f.get('qtyStep', '0.001'))
                info['minQty'] = float(lot_f.get('minOrderQty', '0.001'))
                info['maxQty'] = float(lot_f.get('maxOrderQty', '100'))
                info['minNotional'] = float(lot_f.get('minNotionalValue', '5'))
                info['lotSizeFilter'] = lot_f

            if 'qtyStep' in info:
                _instruments_cache[symbol] = info
        
        _instruments_ts['last_load'] = now
        log.info('[INSTR] Загружено %d контрактов с параметрами лотов', len(_instruments_cache))
        
    except Exception as e:
        log.warning('[INSTR] Загрузка failed: %s', e)
    
    return _instruments_cache


def get_instrument_info(symbol):
    """Возвращает информацию о контракте (qtyStep, priceStep, minQty, ...)."""
    # Fallback если кэш ещё не загружен
    info = _instruments_cache.get(symbol)
    if info:
        return info
    
    # Консервативные defaults
    return {
        'qtyStep': '1',           # BTCUSDT и крупные — обычно 1
        'priceStep': '0.01',      # большинство — 0.01
        'minQty': '0.001',
        'maxQty': '100',
        'fallback': True,
    }


def normalize_qty(symbol, qty):
    """Нормализует количество контрактов по qtyStep и minQty."""
    info = get_instrument_info(symbol)
    qty_step = float(info.get('qtyStep', '0.001'))
    min_qty = float(info.get('minQty', '0.001'))
    max_qty = float(info.get('maxQty', '10000'))
    
    # Округляем вниз по шагу
    qty = _round_to_tick(qty, qty_step, round_down=True)
    
    # Минимум
    if qty < min_qty:
        qty = min_qty
    
    # Максимум (защита от перерасхода)
    if qty > max_qty:
        qty = max_qty
        log.warning('[NORM] qty для %s обрезан до maxQty: %.3f > %.3f', symbol, qty, max_qty)
    
    return qty


def normalize_price(symbol, price):
    """Нормализует цену по priceStep."""
    info = get_instrument_info(symbol)
    price_step = float(info.get('priceStep', '0.01'))

    return _round_to_tick(price, price_step, round_down=False)


def min_order_notional(symbol, price: float) -> float:
    """Минимальная стоимость ордера в USDT для символа при данной цене.

    Учитываем оба ограничения биржи:
      * minQty контрактов (например BTC: 0.001 BTC),
      * minNotionalValue (обычно 5 USDT).
    Возвращаем max(minQty * price, minNotional) — меньше этого биржа не примет.
    """
    info = get_instrument_info(symbol)
    min_qty = float(info.get('minQty', '0.001') or 0.001)
    min_notional = float(info.get('minNotional', 5.0) or 5.0)
    try:
        price = float(price)
    except (TypeError, ValueError):
        price = 0.0
    return max(min_qty * price, min_notional)


async def _read_json(resp):
    raw = await resp.text(encoding='utf-8', errors='replace')
    if raw.startswith('\ufeff'):
        raw = raw[1:]
    return json.loads(raw)


def _sign_request(params: dict) -> dict:
    if not API_KEY or not API_SECRET:
        return {}
    timestamp = str(_server_now_ms())
    param_str = urlencode(params)
    payload = timestamp + API_KEY + '5000' + param_str
    signature = hmac.new(
        API_SECRET.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return {
        'X-BAPI-API-KEY': API_KEY,
        'X-BAPI-TIMESTAMP': timestamp,
        'X-BAPI-RECV-WINDOW': '5000',
        'X-BAPI-SIGN': signature,
    }


def _sign_post(timestamp: str, params: dict) -> str:
    """Подпись POST по канону Bybit v5: timestamp + api_key + recv_window + JSON-тело.

    Bybit вычисляет подпись над СЫРЫМ JSON-телом запроса, а не над
    urlencoded-строкой. Ключи должны быть в том же порядке, что и в теле.
    """
    body = json.dumps(params)
    payload = timestamp + API_KEY + '5000' + body
    return hmac.new(
        API_SECRET.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


class BybitREST:
    def __init__(self, session):
        self._s = session
        self._s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json',
        })

    async def _get(self, path: str, params: dict):
        """GET запрос с подписью. params добавляются в URL после urlencode."""
        query_str = urlencode(params)
        url = REST_BASE + path + ('?' + query_str if query_str else '')
        headers = _sign_request(params)
        async with self._s.get(url, headers=headers) as r:
            if r.status == 403:
                log.error('403 Forbidden on %s. API keys: %s', path, 'SET' if API_KEY else 'NOT SET')
            r.raise_for_status()
            return await _read_json(r)

    async def get_instruments(self):
        out = []
        cursor = ''
        for _ in range(20):
            params = {'category': 'linear'}
            if cursor:
                params['cursor'] = cursor
            data = await self._get('/v5/market/instruments-info', params)
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
                data = await self._get('/v5/market/risk-limit', params)
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

    async def get_kline(self, symbol, interval='60', limit=2):
        """Универсальный метод для получения свечей (любой интервал)."""
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval,
            'limit': str(limit),
        }
        data = await self._get('/v5/market/kline', params)
        lst = data.get('result', {}).get('list', [])
        lst.reverse() # Bybit отдает от новых к старым, переворачиваем
        return lst

    async def get_kline_1h(self, symbol, limit=200):
        """Оставлен для совместимости (используется в статистике)."""
        return await self.get_kline(symbol, interval='60', limit=limit)

    async def get_tickers(self):
        params = {'category': 'linear'}
        data = await self._get('/v5/market/tickers', params)
        if data.get('retCode') != 0:
            log.warning('tickers retCode=%s retMsg=%s',
                        data.get('retCode'), data.get('retMsg'))
            return []
        return data.get('result', {}).get('list', [])

    # ======================================================================
    # ТОРГОВЫЕ МЕТОДЫ (POST-запросы)
    # ======================================================================

    async def _post(self, path: str, params: dict):
        """Отправляет POST-запрос с подписью.

        Bybit v5 требует: тело = сырой JSON, подпись = HMAC(timestamp+key+recv+body).
        Отправка через params= (form-urlencoded) даёт 'Error sign'.
        """
        timestamp = str(_server_now_ms())
        body = json.dumps(params)
        signature = hmac.new(
            API_SECRET.encode('utf-8'),
            (timestamp + API_KEY + '5000' + body).encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        headers = {
            'X-BAPI-API-KEY': API_KEY,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': '5000',
            'X-BAPI-SIGN': signature,
            'Content-Type': 'application/json',
        }
        async with self._s.post(REST_BASE + path, data=body, headers=headers) as r:
            if r.status == 403:
                log.error('403 Forbidden on %s. API keys: %s', path, 'SET' if API_KEY else 'NOT SET')
            if r.status != 200:
                text = await r.text(encoding='utf-8', errors='replace')
                log.error('POST %s failed: status=%d body=%s', path, r.status, text[:500])
                raise aiohttp.ClientResponseError(r.request_info, r.status, text)
            r.raise_for_status()
            return await _read_json(r)

    async def create_order(self, symbol: str, side: str, order_type: str,
                           qty: float, price: float = None,
                           reduce_only: bool = False,
                           order_link_id: str = None) -> dict:
        """
        Создаёт ордер на Bybit.

        side: 'Buy' | 'Sell'
        order_type: 'Market' | 'Limit'
        qty: количество контрактов (будет нормализовано по qtyStep)
        price: цена (для Limit-ордеров, будет нормализовано по priceStep)
        reduce_only: True если только на закрытие
        order_link_id: уникальный ID клиента (для дедупликации)
        """
        # Нормализуем qty и price по шагам контракта
        qty = normalize_qty(symbol, qty)
        if price:
            price = normalize_price(symbol, price)
        
        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': side,
            'orderType': order_type,
            'qty': str(qty),
        }
        if price:
            params['price'] = str(price)
        if order_link_id:
            params['orderLinkId'] = order_link_id
        if reduce_only:
            params['reduceOnly'] = 'true'

        data = await self._post('/v5/order/create', params)
        if data.get('retCode') != 0:
            log.error('create_order failed: %s (qty=%.3f, price=%s)', 
                      data.get('retMsg'), qty, price or 'MKT')
            raise Exception(f"Bybit create_order failed: {data.get('retMsg')}")

        result = data.get('result', {})
        order_id = result.get('orderId') or result.get('orderLinkId') or ''
        log.info('ORDER created: %s %s %s qty=%.3f @ %s id=%s',
                 side, symbol, order_type, qty, price or 'MKT', order_id)
        return result

    async def cancel_order(self, symbol: str, order_id: str = None,
                           order_link_id: str = None) -> dict:
        """Отменяет ордер."""
        params = {'category': 'linear', 'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if order_link_id:
            params['orderLinkId'] = order_link_id

        data = await self._post('/v5/order/reject', params)
        if data.get('retCode') != 0:
            log.warning('cancel_order failed: %s', data.get('retMsg'))
        return data

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Отменяет все ордера символа."""
        params = {'category': 'linear', 'symbol': symbol}
        data = await self._post('/v5/order/cancel-all', params)
        if data.get('retCode') != 0:
            log.warning('cancel_all_orders failed: %s', data.get('retMsg'))
        return data

    async def close_position(self, symbol: str, order_type: str = 'Market',
                             order_link_id: str = None) -> dict:
        """
        Закрывает позицию через Reverse-Order или Sell.
        Использует reduceOnly для безопасного закрытия.
        """
        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': 'Sell',
            'orderType': order_type,
            'qty': '0',
            'reduceOnly': 'true',
            'slippageLimit': 'true',
        }
        if order_link_id:
            params['orderLinkId'] = order_link_id

        data = await self._post('/v5/order/create', params)
        if data.get('retCode') != 0:
            log.error('close_position failed: %s', data.get('retMsg'))
            raise Exception(f"Bybit close_position failed: {data.get('retMsg')}")

        result = data.get('result', {})
        log.info('POSITION CLOSE order: %s %s id=%s', symbol, order_type,
                 result.get('orderId', ''))
        return result

    async def set_leverage(self, symbol: str, leverage: int,
                           margin_mode: str = 'cross') -> dict:
        """Устанавливает плечо для символа."""
        params = {
            'category': 'linear',
            'symbol': symbol,
            'leverage': str(int(leverage)),
        }
        if margin_mode == 'cross':
            params['positionIdx'] = '0'
        else:
            params['positionIdx'] = '1'

        data = await self._post('/v5/position/set-leverage', params)
        if data.get('retCode') != 0:
            log.warning('set_leverage failed: %s', data.get('retMsg'))
        else:
            result = data.get('result', {})
            log.info('Leverage set for %s: %dx (%s)', symbol, leverage, margin_mode)
        return data

    async def get_positions(self, symbol: str = None, settle_coin: str = 'USDT') -> list:
        """Получает открытые позиции."""
        params = {'category': 'linear', 'settleCoin': settle_coin}
        if symbol:
            params['symbol'] = symbol

        data = await self._get('/v5/position/list', params)
        if data.get('retCode') != 0:
            log.warning('get_positions failed: %s', data.get('retMsg'))
            return []
        result = data.get('result', {})
        positions = result.get('list', [])
        if data.get('result', {}).get('nextPageCursor'):
            # Пагинация — подгружаем всё
            cursor = result['nextPageCursor']
            while cursor:
                params['cursor'] = cursor
                data = await self._get('/v5/position/list', params)
                positions.extend(data.get('result', {}).get('list', []))
                cursor = data.get('result', {}).get('nextPageCursor', '')
        return positions

    async def get_balance(self) -> dict:
        """
        Получает баланс аккаунта (USDT) с Bybit.
        
        Использует Unified Trading Account (UTA) режим.
        """
        # 1. UTA (Unified Trading Account) — основной режим
        try:
            params = {'category': 'unified', 'accountType': 'UNIFIED'}
            query_str = urlencode(params)
            url = REST_BASE + '/v5/account/wallet-balance' + ('?' + query_str if query_str else '')
            headers = _sign_request(params)
            
            async with self._s.get(url, headers=headers) as r:
                if r.status != 200:
                    raise aiohttp.ClientResponseError(r.request_info, r.status, await r.text())
                r.raise_for_status()
                data = await _read_json(r)
                
                if data.get('retCode') != 0:
                    raise Exception(f"retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
                
                wallet_data = data.get('result', {})
                for acct in wallet_data.get('list', []):
                    # UTA: свободные средства считаются на УРОВНЕ АККАУНТА.
                    # У монеты поля availableBalance может не быть вовсе (N/A) —
                    # тогда coin-level avail = 0 и бот ложно думает, что денег нет.
                    total_available = float(acct.get('totalAvailableBalance') or 0)
                    total_equity_acct = float(acct.get('totalEquity') or 0)
                    for coin_data in (acct.get('coin') or []):
                        if coin_data.get('coin') != 'USDT':
                            continue
                        equity = float(coin_data.get('equity') or 0)
                        # walletBalance в UTA может быть 0 — фоллбек на totalWalletBalance аккаунта
                        wallet_bal = float(coin_data.get('walletBalance') or 0)
                        if wallet_bal == 0:
                            wallet_bal = float(acct.get('totalWalletBalance') or 0)
                        unrealised_pnl = float(coin_data.get('unrealisedPnl') or 0)
                        # Приоритет: account-level totalAvailableBalance.
                        # Coin-level availableBalance используем только если он
                        # реально присутствует (не пустой/None).
                        raw_avail = coin_data.get('availableBalance')
                        coin_avail = float(raw_avail) if raw_avail not in (None, '') else 0.0
                        available = max(total_available, coin_avail)
                        # Если аккаунт-баланс тоже не отдали — считаем сами:
                        # wallet минус занято маржей (initial margin позиции/ордеров).
                        if available == 0 and wallet_bal > 0:
                            used_im = float(acct.get('totalInitialMargin') or 0) + \
                                float(acct.get('totalOrderIM') or 0)
                            available = max(0.0, total_equity_acct - used_im)

                        if equity > 0 or total_equity_acct > 0:
                            result = {
                                'total_equity': equity or total_equity_acct,
                                'available_balance': available,
                                'wallet_value': wallet_bal,
                                'unrealised_pnl': unrealised_pnl,
                                'mode': 'UTA',
                            }
                            log.info('[BALANCE] UTA: equity=%.2f wallet=%.2f upnl=%.2f avail=%.2f',
                                     equity or total_equity_acct, wallet_bal, unrealised_pnl, available)
                            return result
                log.warning('[BALANCE] UTA: USDT coin не найден')
        except Exception as e:
            log.warning('[BALANCE] UTA mode failed: %s', e)
        
        return {}

    async def get_order_status(self, symbol: str, order_id: str = None,
                               order_link_id: str = None) -> dict:
        """Получает статус ордера."""
        params = {'category': 'linear', 'symbol': symbol}
        if order_id:
            params['orderId'] = order_id
        if order_link_id:
            params['orderLinkId'] = order_link_id

        data = await self._get('/v5/order/realtime', params)
        if data.get('retCode') != 0:
            log.warning('get_order_status failed: %s', data.get('retMsg'))
        return data

    async def get_open_orders(self, symbol: str = None) -> list:
        """Получает активные ордера."""
        params = {'category': 'linear'}
        if symbol:
            params['symbol'] = symbol

        data = await self._get('/v5/order/open-orders', params)
        if data.get('retCode') != 0:
            log.warning('get_open_orders failed: %s', data.get('retMsg'))
            return []
        result = data.get('result', {})
        orders = result.get('list', [])
        if result.get('nextPageCursor'):
            cursor = result['nextPageCursor']
            while cursor:
                params['cursor'] = cursor
                data = await self._get('/v5/order/open-orders', params)
                orders.extend(data.get('result', {}).get('list', []))
                cursor = data.get('result', {}).get('nextPageCursor', '')
        return orders

    async def get_order_history(self, symbol: str, limit: int = 50,
                                order_link_id: str = None) -> list:
        """
        Получает историю ордеров (заполненные + отменённые).
        Используется для определения grid_step по количеству GRID_ADD ордеров.
        """
        params = {
            'category': 'linear',
            'symbol': symbol,
            'limit': str(limit),
        }
        if order_link_id:
            params['orderLinkId'] = order_link_id

        data = await self._get('/v5/order/history', params)
        if data.get('retCode') != 0:
            log.warning('get_order_history failed: %s', data.get('retMsg'))
            return []
        result = data.get('result', {})
        orders = result.get('list', [])
        if result.get('nextPageCursor'):
            cursor = result['nextPageCursor']
            while cursor:
                params['cursor'] = cursor
                data = await self._get('/v5/order/history', params)
                orders.extend(data.get('result', {}).get('list', []))
                cursor = data.get('result', {}).get('nextPageCursor', '')
        return orders

    async def get_execution_history(self, symbol: str, limit: int = 50) -> list:
        """
        Получает историю исполнений (executions) — реальных сделок.
        Полезно для точного подсчёта grid-шагов.
        """
        params = {
            'category': 'linear',
            'symbol': symbol,
            'limit': str(limit),
        }
        data = await self._get('/v5/execution/list', params)
        if data.get('retCode') != 0:
            log.warning('get_execution_history failed: %s', data.get('retMsg'))
            return []
        return data.get('result', {}).get('list', [])

    async def get_closed_pnl(self, symbol: str, limit: int = 50) -> list:
        """Закрытые сделки по символу (v5/position/closed-pnl).

        Возвращает записи с полями: avgEntryPrice, avgExitPrice, closedPnl,
        qty, side ('Buy'/'Sell'), createdTime, updatedTime.
        Используется, чтобы зафиксировать PnL по РЕАЛЬНОЙ цене закрытия,
        а не по последней сохранённой в БД current_price.
        """
        params = {
            'category': 'linear',
            'symbol': symbol,
            'limit': str(limit),
        }
        try:
            data = await self._get('/v5/position/closed-pnl', params)
        except Exception as e:
            log.warning('get_closed_pnl failed for %s: %r', symbol, e)
            return []
        if data.get('retCode') != 0:
            log.warning('get_closed_pnl retCode=%s retMsg=%s',
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