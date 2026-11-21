import asyncio
import sys
sys.path.insert(0, r'C:\Users\Canon\Projects\Knife')

async def main():
    from app.monitor import monitor
    from app.trading_bot import TradingBot
    
    await monitor.start()
    bot = TradingBot(monitor._rest)
    result = await bot.sync_open_positions()
    print(f'Result: {result}')
    await monitor.stop()

asyncio.run(main())
