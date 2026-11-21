import asyncio
import os
import json
import sys
sys.path.insert(0, r'C:\Users\Canon\Projects\Knife')

async def main():
    from app.bybit import BybitREST
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        rest = BybitREST(session)
        history = await rest.get_order_history('ELSAUSDT', limit=50)
        print(f"Total orders: {len(history)}")
        for o in reversed(history):
            status = o.get('orderStatus')
            avg = o.get('avgPrice', 'N/A')
            qty = o.get('cumExecQty', '0')
            olid = o.get('orderLinkId', '')
            if status == 'Filled' and float(qty) > 0:
                print(f"  Filled: avg={avg} qty={qty} olid={olid}")

asyncio.run(main())
