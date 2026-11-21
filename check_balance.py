import asyncio
import json
import os
import hmac
import hashlib
import time
from urllib.parse import urlencode
import aiohttp

API_KEY = 'pitHFYVp1ew9nHfVNA'
API_SECRET = '1FB7ERfpV4CA8Op3UuBbuWgwFCcrl3B7uaoh'

async def test_balance():
    params = {'category': 'unified', 'accountType': 'UNIFIED'}
    timestamp = str(int(time.time() * 1000))
    param_str = urlencode(params)
    payload = timestamp + API_KEY + '5000' + param_str
    signature = hmac.new(
        API_SECRET.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'X-BAPI-API-KEY': API_KEY,
        'X-BAPI-TIMESTAMP': timestamp,
        'X-BAPI-RECV-WINDOW': '5000',
        'X-BAPI-SIGN': signature,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(
            'https://api.bybit.com/v5/account/wallet-balance?' + param_str,
            headers=headers
        ) as r:
            print('Status:', r.status)
            text = await r.text()
            data = json.loads(text)
            print(json.dumps(data, indent=2, ensure_ascii=False))

asyncio.run(test_balance())
