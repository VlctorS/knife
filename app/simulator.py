import asyncio
import datetime
import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, engine
from .state import state

log = logging.getLogger(__name__)


def _get_upsert(model, values, sym):
    is_pg = engine.url.drivername.startswith('postgresql')
    if is_pg:
        return pg_insert(model).values(**values).on_conflict_do_update(
            index_elements=['symbol'],
            set_={k: v for k, v in values.items() if k != 'symbol'},
        )
    return sqlite_insert(model).values(**values).on_conflict_do_update(
        index_elements=['symbol'],
        set_={k: v for k, v in values.items() if k != 'symbol'},
    )


class Simulator:
    def __init__(self, rest):
        self.rest = rest

    async def run_history_scan(self):
        log.info('Starting history scan for %d symbols...', len(state.symbols))
        ok = 0
        async with SessionLocal() as session:
            for sym, st in state.symbols.items():
                try:
                    kl = await self.rest.get_kline_1h(sym, limit=720)
                    if len(kl) < 24:
                        continue
                    prostrels = []
                    for k in kl:
                        high = float(k[2])
                        low = float(k[3])
                        if low > 0:
                            prostrels.append((high - low) / low * 100)
                    if not prostrels:
                        continue
                    avg_p = sum(prostrels) / len(prostrels)
                    max_p = max(prostrels)
                    big_moves = sum(1 for p in prostrels if p > 5.0)
                    freq = big_moves / 30.0
                    
                    # Точка входа = 85% от среднего прострела, но не менее 3%
                    entry_l = max(3.0, avg_p * 0.85)
                    entry_s = entry_l

                    values = dict(
                        symbol=sym,
                        max_leverage=st.max_leverage,
                        avg_prostrel_pct=avg_p,
                        max_prostrel_pct=max_p,
                        freq_per_day=freq,
                        entry_long_pct=entry_l,
                        entry_short_pct=entry_s,
                        updated_at=datetime.datetime.utcnow(),
                    )
                    stmt = _get_upsert(SymbolStat, values, sym)
                    await session.execute(stmt)
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
        async with SessionLocal() as session:
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            open_trades = result.scalars().all()
            open_syms = {t.symbol for t in open_trades}

            for sym, st in state.symbols.items():
                if sym in open_syms:
                    continue
                stat = await session.get(SymbolStat, sym)
                price = st.last_price
                ma = st.ma
                if ma is None or price <= 0:
                    continue
                dist_from_ma = (price - ma) / ma * 100
                
                # Если статистики нет, используем 50% по умолчанию
                entry_long = stat.entry_long_pct if stat and stat.entry_long_pct > 0 else 50.0
                entry_short = stat.entry_short_pct if stat and stat.entry_short_pct > 0 else 50.0

                if dist_from_ma <= -entry_long:
                    await self._open_trade(session, sym, 'LONG', price, st.max_leverage, dist_from_ma)
                elif dist_from_ma >= entry_short:
                    await self._open_trade(session, sym, 'SHORT', price, st.max_leverage, dist_from_ma)
            await session.commit()

    async def _open_trade(self, session, sym, side, price, max_lev, dist_ma):
        trade = Trade(
            symbol=sym,
            side=side,
            entry_price=price,
            avg_entry_price=price,
            total_qty=6.0 / price,
            total_margin=6.0 / max(1.0, max_lev),
            status='OPEN',
            grid_step=0,
            max_steps=10,
        )
        session.add(trade)
        log.info('SIMULATED ENTRY %s %s @ %.6f (MA dist: %.2f%%)', side, sym, price, dist_ma)

    async def update_open_trades(self):
        async with SessionLocal() as session:
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            open_trades = result.scalars().all()

            for t in open_trades:
                st = state.symbols.get(t.symbol)
                if not st or st.last_price <= 0:
                    continue
                price = st.last_price
                t.current_price = price  # <--- Сохраняем текущую цену
                
                # Проверка сетки усреднения (строго пошагово)
                if t.grid_step < t.max_steps:
                    stat = await session.get(SymbolStat, t.symbol)
                    base_pct = stat.entry_long_pct if stat and stat.entry_long_pct > 0 else 50.0
                    step_pct = base_pct * (2.5 ** t.grid_step)
                    
                    trigger_hit = False
                    if t.side == 'LONG' and price <= t.entry_price * (1 - step_pct / 100):
                        trigger_hit = True
                    elif t.side == 'SHORT' and price >= t.entry_price * (1 + step_pct / 100):
                        trigger_hit = True
                        
                    if trigger_hit:
                        old_qty = t.total_qty
                        old_avg = t.avg_entry_price
                        t.grid_step += 1
                        add_qty = 6.0 / price
                        t.total_qty = old_qty + add_qty
                        t.avg_entry_price = (old_avg * old_qty + price * add_qty) / t.total_qty
                        log.info('SIMULATED GRID ADD %s %s @ %.6f (step %d, dist %.2f%%)', t.side, t.symbol, price, t.grid_step, step_pct)
                        # ВАЖНО: за один тик делаем только один шаг сетки, чтобы не перескакивать уровни
                        # На следующем тике (через 2 сек) проверится следующий шаг
                        await session.commit() 

                # Проверка закрытия (тейк-профит по средней цене)
                if t.side == 'LONG':
                    pnl_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    pnl_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100
                    
                if pnl_pct >= 15.0:
                    t.status = 'CLOSED'
                    t.closed_at = datetime.datetime.utcnow()
                    t.pnl = t.total_margin * (pnl_pct / 100.0)
                    log.info('SIMULATED CLOSE %s %s @ %.6f PnL: %.2f USDT', t.side, t.symbol, price, t.pnl)
            await session.commit()

def run_history_scan_sync(symbols_data):
    asyncio.run(_scan_async(symbols_data))


async def _scan_async(symbols_data):
    import aiohttp
    async with aiohttp.ClientSession() as s:
        rest = BybitREST(s)
        sim = Simulator(rest)
        from .state import SymbolState
        for sym, lev in symbols_data:
            state.symbols[sym] = SymbolState(symbol=sym, max_leverage=lev)
        await sim.run_history_scan()


simulator = None
