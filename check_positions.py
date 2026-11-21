import asyncio
import os
import json
import sys
sys.path.insert(0, r'C:\Users\Canon\Projects\Knife')

async def main():
    from app.bybit import BybitREST, BybitTickerWS
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        rest = BybitREST(session)
        positions = await rest.get_positions()
        print("POSITIONS:")
        for p in positions:
            print(f"  {p.get('symbol')}: size={p.get('size')} side={p.get('side')} avgEntry={p.get('avgEntryPrice')}")
        
        if not positions:
            print("  (none)")

asyncio.run(main())
