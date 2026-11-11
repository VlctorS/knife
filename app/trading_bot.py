import asyncio
import datetime
import logging
import time
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .bybit import BybitREST
from .db import SessionLocal, SymbolStat, Trade, SymbolConfig, TradeHistory, engine, Settings
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
        self._last_scan_ts = 0
        self._seen_candles = set()

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
        # Сканируем свечи раз в 60 секунд
        now = time.time()
        if now - self._last_scan_ts < 60:
            return
        self._last_scan_ts = now

        async with SessionLocal() as session:
            # Получаем открытые сделки, чтобы не дублировать
            result = await session.execute(select(Trade).where(Trade.status == 'OPEN'))
            open_syms = {t.symbol for t in result.scalars().all()}

            symbols_to_check = [s for s in state.symbols.keys() if s not in open_syms]
            if not symbols_to_check:
                return

            intervals = ['1', '5', '15'] # 1m, 5m, 15m
            
            for sym in symbols_to_check:
                try:
                    for tf in intervals:
                        kl = await self.rest.get_kline(sym, interval=tf, limit=2)
                        if len(kl) < 2: continue
                        
                        # Берем последнюю закрытую свечу
                        last_candle = kl[-1] 
                        open_price = float(last_candle[1])
                        close_price = float(last_candle[4])
                        
                        if open_price <= 0: continue
                        change_pct = (close_price - open_price) / open_price * 100
                        
                        # Проверяем порог
                        if abs(change_pct) >= settings.threshold * 100:
                            candle_id = f"{sym}_{tf}_{last_candle[0]}"
                            if candle_id in self._seen_candles:
                                continue
                            self._seen_candles.add(candle_id)
                            
                            kind = 'PUMP' if change_pct > 0 else 'DUMP'
                            side = 'SHORT' if change_pct > 0 else 'LONG'
                                
                            state.add_alert(Alert(
                                ts=time.time(), symbol=sym, kind=kind,
                                price_now=close_price, ref_price=open_price, ma=0.0,
                                change_pct=change_pct, timeframe=tf + 'm'
                            ))
                            
                            cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == sym))
                            cfg = cfg_res.scalars().first()
                            
                            await self._open_trade(session, sym, side, close_price, state.symbols[sym].max_leverage, cfg)
                            
                            log.info(f'SIGNAL {kind} {sym} ({tf}m) {change_pct:.2f}% -> OPEN {side}')
                            
                except Exception as e:
                    log.warning(f'Check entries failed for {sym}: {e}')
            await session.commit()

    async def _open_trade(self, session, sym, side, price, max_lev, cfg):
        margin = 1.0
        if cfg and cfg.margin_entry > 0:
            margin = cfg.margin_entry
        else:
            gs = await session.get(Settings, 1)
            margin = gs.margin_entry if gs else 1.0
                
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
        log.info('OPEN %s %s @ %.6f (size: %.2f USDT, lev: %.0fx)', side, sym, price, position_size, max_lev)

    async def update_open_trades(self):
        # ... (логика сетки и PnL без изменений)
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
                        
                        add_margin = cfg.margin_grid if (cfg and cfg.margin_grid > 0) else t.total_margin
                        
                        add_qty = (add_margin * t.leverage) / price
                        t.total_qty += add_qty
                        t.total_margin += add_margin
                        t.avg_entry_price = (t.avg_entry_price * old_qty + price * add_qty) / t.total_qty
                        
                        history = TradeHistory(
                            trade_id=t.id, action='GRID_ADD', price=price, qty=add_qty,
                            margin=add_margin, pnl=0.0, notes=f'step {t.grid_step}'
                        )
                        session.add(history)
                        log.info('GRID ADD %s %s @ %.6f (step %d)', t.side, t.symbol, price, t.grid_step)
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