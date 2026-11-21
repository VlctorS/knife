import asyncio
import sys
sys.path.insert(0, r'C:\Users\Canon\Projects\Knife')

async def main():
    from app.db import SessionLocal, Trade
    
    async with SessionLocal() as session:
        result = await session.execute(
            __import__('sqlalchemy').select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL']))
        )
        trades = result.scalars().all()
        print("OPEN TRADES:")
        for t in trades:
            print(f"  ID={t.id} {t.symbol} side={t.side} qty={t.total_qty} avg={t.avg_entry_price} cur={t.current_price} status={t.status} pnl={t.pnl}")
        if not trades:
            print("  (none)")
            
        result2 = await session.execute(__import__('sqlalchemy').select(Trade).where(Trade.status == 'CLOSED'))
        closed = result2.scalars().all()
        print("\nCLOSED TRADES:")
        for t in closed:
            print(f"  ID={t.id} {t.symbol} side={t.side} qty={t.total_qty} status={t.status}")
        if not closed:
            print("  (none)")

asyncio.run(main())
