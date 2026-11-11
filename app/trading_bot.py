import asyncio
import datetime
import logging
import time
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine
from .state import state, Alert
from .config import settings

log = logging.getLogger(__name__)

def _get_upsert(model, values, pk_col):
    is_pg = engine.url.drivername.startswith('postgresql')
    if is_pg:
        return pg_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})
    return sqlite_insert(model).values(**values).on_conflict_do_update(index_elements=[pk_col], set_={k: v for k, v in values.items() if k != pk_col})

class TradingBot:
    def __init__(self, rest):
        self.rest = rest

    async def run_history_scan(self):
        log.info('Starting history scan for %d symbols...', len(state.symbols))
        ok = 0
        async with SessionLocal() as session:
            for sym, st in state.symbols.items():
                try:
                    kl = await self.rest.get_kline_1h(sym, limit=720)
                    if len(kl) < 24: continue
                    
                    prostrels = [(float(k[2]) - float(k[3])) / float(k[3]) * 100 for k in kl if float(k[3]) > 0]
                    if not prostrels: continue
                    
                    avg_p = sum(prostrels) / len(prostrels)
                    auto_entry = max(3.0, avg_p * 0.85)
                    
                    values = dict(
                        symbol=sym, max_leverage=st.max_leverage,
                        avg_prostrel_pct=avg_p, max_prostrel_pct=max(prostrels),
                        freq_per_day=sum(1 for p in prostrels if p > 5.0) / 30.0,
                        entry_long_pct=auto_entry, entry_short_pct=auto_entry,
                        updated_at=datetime.datetime.utcnow(),
                    )
                    stmt = _get_upsert(SymbolStat, values, 'symbol')
                    await session.execute(stmt)
                    
                    res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                    if not res.scalars().first():
                        session.add(SymbolConfig(symbol=sym, min_entry_pct=5.0))
                        
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
            open_syms = {t.symbol for t in result.scalars().all()}

            for sym, st in state.symbols.items():
                if sym in open_syms: continue
                
                cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                cfg = cfg_res.scalars().first()
                stat = await session.get(SymbolStat, sym)
                
                price, ma = st.last_price, st.ma
                if ma is None or price <= 0: continue
                
                dist = (price - ma) / ma * 100
                
                # ЛОГИКА ВХОДА
                if cfg and cfg.entry_pct > 0:
                    threshold = cfg.entry_pct
                elif cfg and cfg.min_entry_pct > 0:
                    threshold = cfg.min_entry_pct
                else:
                    threshold = max(settings.threshold * 100, 5.0)

                if dist <= -threshold:
                    state.add_alert(Alert(ts=time.time(), symbol=sym, kind='DUMP', price_now=price, ref_price=ma, ma=ma, change_pct=dist))
                    await self._open_trade(session, sym, 'LONG', price, st.max_leverage, cfg)
                elif dist >= threshold:
                    state.add_alert(Alert(ts=time.time(), symbol=sym, kind='PUMP', price_now=price, ref_price=ma, ma=ma, change_pct=dist))
                    await self._open_trade(session, sym, 'SHORT', price, st.max_leverage, cfg)
            await session.commit()

    async def _open_trade(self, session, sym, side, price, max_lev, cfg):
        margin = cfg.max_margin if cfg else 1.0
        position_size = margin * max_lev
        qty = position_size / price

        trade = Trade(
            symbol=sym, side=side, entry_price=price, avg_entry_price=price,
            total_qty=qty, total_margin=margin, status='OPEN',
            grid_step=0, max_steps=cfg.max_steps if cfg else 10,
            leverage=max_lev, current_price=price,
        )
        session.add(trade)
        await session.flush()
        
        history = TradeHistory(
            trade_id=trade.id, action='OPEN', price=price, qty=qty,
            margin=margin, pnl=0.0, notes=f'{side} open'
        )
        session.add(history)
        
        log.info('OPEN %s %s @ %.6f (dist: %.2f%%, size: %.2f USDT, lev: %.0fx)', 
                 side, sym, price, ((price - state.symbols[sym].ma)/state.symbols[sym].ma*100), position_size, max_lev)

    async def update_open_trades(self):
        async with SessionLocal() as session:
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            for t in result.scalars().all():
                st = state.symbols.get(t.symbol)
                if not st or st.last_price <= 0: continue
                
                price = st.last_price
                t.current_price = price
                
                cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == t.symbol))
                cfg = cfg_res.scalars().first()
                
                if t.grid_step < t.max_steps:
                    start_pct = cfg.grid_step_pct if cfg else 5.0
                    mult = cfg.grid_step_multiplier if cfg else 2.5
                    step_pct = start_pct * (mult ** t.grid_step)
                    
                    if (t.side == 'LONG' and price <= t.entry_price * (1 - step_pct / 100)) or \
                       (t.side == 'SHORT' and price >= t.entry_price * (1 + step_pct / 100)):
                        
                        old_qty = t.total_qty
                        t.grid_step += 1
                        add_qty = (t.total_margin * t.leverage) / price
                        t.total_qty += add_qty
                        t.avg_entry_price = (t.avg_entry_price * old_qty + price * add_qty) / t.total_qty
                        
                        history = TradeHistory(
                            trade_id=t.id, action='GRID_ADD', price=price, qty=add_qty,
                            margin=t.total_margin, pnl=0.0, notes=f'step {t.grid_step}'
                        )
                        session.add(history)
                        
                        log.info('GRID ADD %s %s @ %.6f (step %d, dist %.2f%%)', t.side, t.symbol, price, t.grid_step, step_pct)
                        await session.commit()

                if t.side == 'LONG':
                    pnl_pct = (price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    pnl_pct = (t.avg_entry_price - price) / t.avg_entry_price * 100
                t.pnl = t.total_margin * (pnl_pct / 100.0) * t.leverage
                
                exit_pct = cfg.initial_exit_pct if cfg else 15.0
                if pnl_pct >= exit_pct:
                    close_pct = cfg.exit_contract_pct if cfg else 50.0
                    closed_qty = t.total_qty * (close_pct / 100.0)
                    
                    if t.side == 'LONG':
                        closed_pnl = (closed_qty * price) - (closed_qty * t.avg_entry_price)
                    else:
                        closed_pnl = (closed_qty * t.avg_entry_price) - (closed_qty * price)
                    
                    if close_pct >= 100:
                        t.status = 'CLOSED'
                        t.closed_at = datetime.datetime.utcnow()
                        action = 'FULL_CLOSE'
                        log.info('CLOSE FULL %s %s @ %.6f PnL: %.2f USDT', t.side, t.symbol, price, closed_pnl)
                    else:
                        t.total_qty -= closed_qty
                        action = 'PARTIAL_CLOSE'
                        log.info('CLOSE PARTIAL %s %s @ %.6f (%.0f%%) PnL: %.2f USDT', t.side, t.symbol, price, close_pct, closed_pnl)
                    
                    history = TradeHistory(
                        trade_id=t.id, action=action, price=price, qty=closed_qty,
                        margin=t.total_margin, pnl=closed_pnl, notes=f'close {close_pct}%'
                    )
                    session.add(history)
            await session.commit()

trading_bot = None