import asyncio
import logging
import os
import time
import datetime
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Body
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, delete, func

from .config import settings
from .monitor import monitor
from .state import state
from .db import init_db, SessionLocal, SymbolStat, Trade, Settings, SymbolConfig, TradeHistory
from .trading_bot import TradingBot

logging.basicConfig(level=settings.log_level, format='%(asctime)s %(levelname)s %(name)s | %(message)s')
log = logging.getLogger('app')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, 'templates'))

trading_bot = None

async def run_history_bg():
    await asyncio.sleep(60)
    if trading_bot:
        try:
            await trading_bot.run_history_scan()
        except Exception:
            log.exception('History scan crashed')

@asynccontextmanager
async def lifespan(app: FastAPI):
    global trading_bot
    await init_db()
    await monitor.start()
    trading_bot = TradingBot(monitor._rest)
    asyncio.create_task(run_history_bg())
    try:
        yield
    finally:
        await monitor.stop()

app = FastAPI(lifespan=lifespan, title='Bybit Trading Bot')

@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse('index.html', {'request': request})

@app.get('/api/status')
async def api_status():
    db_min_lev, db_min_turnover = 10.0, 5.0
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if s:
            db_min_lev, db_min_turnover = s.min_leverage, s.min_turnover

    return {
        'uptime_sec': int(time.time() - state.started_at),
        'symbols_count': len(state.symbols),
        'symbols_last_refresh': getattr(state, 'last_symbols_refresh', 0),
        'threshold_pct': settings.threshold * 100,
        'window_minutes': settings.window_minutes,
        'ma_period': settings.ma_period,
        'min_max_leverage': db_min_lev,
        'min_turnover': db_min_turnover,
        'alerts_count': len(state.alerts),
        'server_time': time.time(),
    }

@app.get('/api/alerts')
async def api_alerts(limit: int = 100):
    limit = max(1, min(limit, settings.max_alerts))
    return {
        'items': [
            {
                'ts': a.ts,
                'symbol': a.symbol,
                'kind': a.kind,
                'price_now': a.price_now,
                'ref_price': a.ref_price,
                'ma': a.ma,
                'change_pct': a.change_pct,
                'timeframe': getattr(a, 'timeframe', ''),
                'reason': getattr(a, 'reason', ''),
            }
            for a in list(state.alerts)[:limit]
        ]
    }

@app.get('/api/stats')
async def api_stats(limit: int = 50):
    async with SessionLocal() as session:
        open_res = await session.execute(
            select(Trade.symbol).where(Trade.status.in_(['OPEN', 'PARTIAL']))
        )
        open_symbols = {row[0] for row in open_res.all()}

        r = await session.execute(select(SymbolStat))
        all_stats = list(r.scalars().all())

        configs_res = await session.execute(select(SymbolConfig))
        configs = {c.symbol: c for c in configs_res.scalars().all()}
        gs = await session.get(Settings, 1)

        def effective_entry(s):
            cfg = configs.get(s.symbol)
            if cfg and cfg.entry_pct > 0:
                return cfg.entry_pct, True
            if gs and gs.entry_pct > 0:
                return gs.entry_pct, True
            if s.avg_prostrel_pct > 0:
                return s.avg_prostrel_pct * 0.85, False
            return 5.0, False

        all_stats.sort(key=lambda s: effective_entry(s)[0], reverse=True)

        selected = []
        seen = set()
        for s in all_stats:
            if s.symbol in open_symbols:
                selected.append(s)
                seen.add(s.symbol)
        for s in all_stats:
            if len(selected) >= max(limit, len(open_symbols)):
                break
            if s.symbol in seen:
                continue
            selected.append(s)
            seen.add(s.symbol)

        missing = open_symbols - {s.symbol for s in all_stats}
        pseudo = []
        for sym in missing:
            st = state.symbols.get(sym)
            pseudo.append({
                'symbol': sym,
                'avg_prostrel_pct': 0.0,
                'max_prostrel_pct': 0.0,
                'freq_per_day': 0.0,
                'entry_long_pct': 0.0,
                'is_manual': False,
                'max_leverage': st.max_leverage if st else 0.0,
                'has_open_trade': True,
                'stats_missing': True,
            })

        items = []
        for s in selected:
            eff, is_manual = effective_entry(s)
            items.append({
                'symbol': s.symbol,
                'avg_prostrel_pct': round(s.avg_prostrel_pct, 2),
                'max_prostrel_pct': round(s.max_prostrel_pct, 2),
                'freq_per_day': round(s.freq_per_day, 2),
                'entry_long_pct': round(eff, 2),
                'is_manual': is_manual,
                'max_leverage': s.max_leverage,
                'has_open_trade': s.symbol in open_symbols,
                'stats_missing': False,
            })

        items.extend(pseudo)
        return {'items': items}

@app.get('/api/trades')
async def api_trades(limit: int = 100):
    async with SessionLocal() as session:
        r = await session.execute(
            select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL'])).order_by(Trade.opened_at.desc()).limit(limit)
        )
        items = []
        for t in r.scalars().all():
            avg_price = float(t.avg_entry_price) if t.avg_entry_price else 0.0
            curr_price = float(t.current_price) if t.current_price else 0.0
            
            if avg_price > 0:
                if t.side == 'LONG':
                    change_pct = (curr_price - avg_price) / avg_price * 100
                else:
                    change_pct = (avg_price - curr_price) / avg_price * 100
            else:
                change_pct = 0.0
            
            total_qty = float(t.total_qty) if t.total_qty else 0.0
            
            if t.side == 'LONG':
                gross = total_qty * (curr_price - avg_price)
            else:
                gross = total_qty * (avg_price - curr_price)
            
            gs = await session.get(Settings, 1)
            fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055
            exit_fee = (total_qty * curr_price) * (fee_pct / 100.0)
            
            net_pnl = float(t.realized_pnl) + gross - float(t.entry_fee) - exit_fee
            
            items.append({
                'id': t.id, 'symbol': t.symbol, 'side': t.side,
                'entry_price': float(t.entry_price) if t.entry_price else 0.0, 
                'avg_entry_price': avg_price,
                'current_price': curr_price, 
                'grid_step': t.grid_step,
                'total_qty': total_qty, 
                'total_margin': float(t.total_margin) if t.total_margin else 0.0,
                'status': t.status, 
                'pnl': round(net_pnl, 4),
                'leverage': float(t.leverage) if t.leverage else 10.0,
                'change_pct': round(change_pct, 2),
                'exit_step': t.exit_step,
                'opened_at': t.opened_at.isoformat() if t.opened_at else None,
                'closed_at': t.closed_at.isoformat() if t.closed_at else None,
            })
        return {'items': items}

@app.get('/api/trade_by_symbol')
async def api_trade_by_symbol(symbol: str):
    async with SessionLocal() as session:
        r = await session.execute(
            select(Trade).where(Trade.symbol == symbol).where(Trade.status.in_(['OPEN', 'PARTIAL'])).order_by(Trade.opened_at.desc())
        )
        t = r.scalars().first()
        if not t:
            return {'ok': False, 'error': 'Trade not found'}
        
        hist_res = await session.execute(
            select(TradeHistory).where(TradeHistory.trade_id == t.id).where(TradeHistory.action == 'OPEN')
        )
        open_hist = hist_res.scalars().first()
        
        entry_details = None
        if open_hist and open_hist.details:
            try:
                entry_details = json.loads(open_hist.details)
            except Exception:
                pass
        
        return {
            'ok': True,
            'symbol': t.symbol,
            'side': t.side,
            'entry_price': float(t.entry_price) if t.entry_price else 0.0,
            'avg_entry_price': float(t.avg_entry_price) if t.avg_entry_price else 0.0,
            'current_price': float(t.current_price) if t.current_price else 0.0,
            'leverage': float(t.leverage) if t.leverage else 10.0,
            'entry_details': entry_details
        }

@app.get('/api/open_pnl')
async def api_open_pnl():
    async with SessionLocal() as session:
        r = await session.execute(
            select(func.sum(Trade.pnl))
            .where(Trade.status.in_(['OPEN', 'PARTIAL']))
        )
        total_open_pnl = r.scalar() or 0.0
        return {'total_pnl': round(float(total_open_pnl), 4)}

@app.get('/api/closed_trades')
async def api_closed_trades(limit: int = 100):
    async with SessionLocal() as session:
        r = await session.execute(
            select(Trade).where(Trade.status == 'CLOSED').order_by(Trade.closed_at.desc()).limit(limit)
        )
        items = []
        for t in r.scalars().all():
            items.append({
                'id': t.id, 'symbol': t.symbol, 'side': t.side,
                'entry_price': float(t.entry_price) if t.entry_price else 0.0, 
                'avg_entry_price': float(t.avg_entry_price) if t.avg_entry_price else 0.0,
                'total_qty': float(t.total_qty) if t.total_qty else 0.0, 
                'total_margin': float(t.total_margin) if t.total_margin else 0.0,
                'pnl': float(t.pnl) if t.pnl else 0.0, 
                'leverage': float(t.leverage) if t.leverage else 10.0,
                'opened_at': t.opened_at.isoformat() if t.opened_at else None,
                'closed_at': t.closed_at.isoformat() if t.closed_at else None,
            })
        return {'items': items}

@app.get('/api/all_exits')
async def api_all_exits(limit: int = 100):
    async with SessionLocal() as session:
        r = await session.execute(
            select(TradeHistory, Trade)
            .join(Trade, TradeHistory.trade_id == Trade.id)
            .where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
            .order_by(TradeHistory.ts.desc())
            .limit(limit)
        )
        
        items = []
        for h, t in r.all():
            items.append({
                'id': h.id,
                'trade_id': h.trade_id,
                'symbol': t.symbol,
                'side': t.side,
                'action': h.action,
                'ts': h.ts.isoformat(),
                'price': float(h.price) if h.price else 0.0,
                'qty': float(h.qty) if h.qty else 0.0,
                'pnl': float(h.pnl) if h.pnl else 0.0,
                'notes': h.notes
            })
        return {'items': items}

@app.get('/api/closed_stats')
async def api_closed_stats():
    async with SessionLocal() as session:
        r = await session.execute(select(Trade))
        all_trades = r.scalars().all()
        
        total_count = len([t for t in all_trades if t.status == 'CLOSED'])
        
        hist_res = await session.execute(
            select(func.sum(TradeHistory.pnl)).where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
        )
        total_realized = hist_res.scalar() or 0.0
        
        wins = [t for t in all_trades if t.status == 'CLOSED' and (t.pnl or 0) > 0]
        losses = [t for t in all_trades if t.status == 'CLOSED' and (t.pnl or 0) < 0]
        
        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / total_count * 100) if total_count > 0 else 0
        
        gross_profit = sum(t.pnl for t in wins if t.pnl)
        gross_loss = abs(sum(t.pnl for t in losses if t.pnl))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.99 if gross_profit > 0 else 0)
        
        now_utc = datetime.datetime.utcnow()
        msk_now = now_utc - datetime.timedelta(hours=3)
        today_start = msk_now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=3)
        
        today_hist_res = await session.execute(
            select(func.sum(TradeHistory.pnl))
            .where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
            .where(TradeHistory.ts >= today_start)
        )
        today_pnl = today_hist_res.scalar() or 0.0
        
        today_count_res = await session.execute(
            select(func.count(TradeHistory.id))
            .where(TradeHistory.action.in_(['PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE']))
            .where(TradeHistory.ts >= today_start)
        )
        today_count = today_count_res.scalar() or 0
        
        return {
            'total_pnl': round(float(total_realized), 4),
            'total_count': total_count,
            'win_count': win_count,
            'loss_count': loss_count,
            'win_rate': round(win_rate, 1),
            'profit_factor': round(float(profit_factor), 2),
            'today_pnl': round(float(today_pnl), 4),
            'today_count': today_count,
        }

@app.post('/api/clear_closed_trades')
async def api_clear_closed_trades(request: Request):
    data = await request.json()
    mode = data.get('mode', 'all')
    
    async with SessionLocal() as session:
        if mode == 'today':
            now_utc = datetime.datetime.utcnow()
            msk_now = now_utc - datetime.timedelta(hours=3)
            today_start = msk_now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=3)
            
            result = await session.execute(
                delete(Trade).where(Trade.status == 'CLOSED').where(Trade.closed_at >= today_start)
            )
        else:
            result = await session.execute(delete(Trade).where(Trade.status == 'CLOSED'))
        
        await session.commit()
        
    return {'ok': True}

@app.post('/api/close_trade')
async def api_close_trade(request: Request):
    data = await request.json()
    trade_id = data.get('trade_id')
    async with SessionLocal() as session:
        t = await session.get(Trade, trade_id)
        if not t or t.status not in ('OPEN', 'PARTIAL'):
            return {'ok': False, 'error': 'Trade not found or already closed'}
        
        st = state.symbols.get(t.symbol)
        price = st.last_price if st else t.current_price
        if price <= 0:
            return {'ok': False, 'error': 'No price'}
        
        gs = await session.get(Settings, 1)
        fee_pct = gs.fee_pct if gs and gs.fee_pct > 0 else 0.055

        closed_qty = t.total_qty
        if t.side == 'LONG':
            closed_pnl = (closed_qty * price) - (closed_qty * t.avg_entry_price)
        else:
            closed_pnl = (closed_qty * t.avg_entry_price) - (closed_qty * price)

        exit_commission = closed_qty * price * (fee_pct / 100.0)
        t.exit_fee += exit_commission
        
        realized_part = closed_pnl - exit_commission - t.entry_fee
        t.realized_pnl += realized_part
        total_pnl = t.realized_pnl
        closed_entry_fee = t.entry_fee

        t.status = 'CLOSED'
        t.closed_at = datetime.datetime.utcnow()
        t.current_price = price
        t.pnl = total_pnl
        t.total_qty = 0.0
        t.total_margin = 0.0
        t.entry_fee = 0.0
        
        history = TradeHistory(
            trade_id=t.id, action='MANUAL_CLOSE', price=price, qty=closed_qty,
            margin=t.total_margin, pnl=realized_part, notes=f'manual close, out fee {exit_commission:.4f}, in fee {closed_entry_fee:.4f}'
        )
        session.add(history)
        await session.commit()
        log.info('MANUAL CLOSE %s %s @ %.6f PnL: %.2f USDT', t.side, t.symbol, price, closed_pnl)
    return {'ok': True}

@app.delete('/api/closed_trades/{trade_id}')
async def api_delete_closed_trade(trade_id: int):
    async with SessionLocal() as session:
        t = await session.get(Trade, trade_id)
        if t and t.status == 'CLOSED':
            await session.execute(delete(TradeHistory).where(TradeHistory.trade_id == trade_id))
            await session.delete(t)
            await session.commit()
            return {'ok': True}
    return {'ok': False}

@app.get('/api/trade_history')
async def api_trade_history(trade_id: int):
    async with SessionLocal() as session:
        trade = await session.get(Trade, trade_id)
        if not trade:
            return {"items": []}
            
        cfg_res = await session.execute(select(SymbolConfig).where(SymbolConfig.symbol == trade.symbol))
        cfg = cfg_res.scalars().first()
        gs = await session.get(Settings, 1)
        
        start_pct = cfg.grid_step_pct if cfg and cfg.grid_step_pct > 0 else (gs.grid_step_pct if gs else 5.0)
        mult = cfg.grid_step_multiplier if cfg and cfg.grid_step_multiplier > 0 else (gs.grid_step_multiplier if gs else 2.5)
        size_mult = cfg.grid_size_multiplier if cfg and cfg.grid_size_multiplier > 0 else (gs.grid_size_multiplier if gs else 1.0)
        base_size = cfg.position_size_grid if cfg and cfg.position_size_grid > 0 else (gs.position_size_grid if gs else 6.0)
        
        current_grid_step = trade.grid_step
        next_step_pct = start_pct * (mult ** current_grid_step)
        
        next_grid_size = base_size * (size_mult ** current_grid_step)
        next_grid_size = max(6.0, next_grid_size)
        
        if trade.side == 'LONG':
            next_grid_price = trade.avg_entry_price * (1 - next_step_pct / 100)
        else:
            next_grid_price = trade.avg_entry_price * (1 + next_step_pct / 100)

        start_exit = cfg.initial_exit_pct if cfg and cfg.initial_exit_pct > 0 else (gs.initial_exit_pct if gs else 15.0)
        exit_mult = cfg.exit_step_multiplier if cfg and cfg.exit_step_multiplier > 0 else (gs.exit_step_multiplier if gs else 1.6)
        close_pct = cfg.exit_contract_pct if cfg and cfg.exit_contract_pct > 0 else (gs.exit_contract_pct if gs else 50.0)
        
        current_exit_step = trade.exit_step if trade.exit_step is not None else 0
        next_exit_pct = start_exit * (exit_mult ** current_exit_step)
        
        if trade.side == 'LONG':
            next_exit_price = trade.avg_entry_price * (1 + next_exit_pct / 100)
        else:
            next_exit_price = trade.avg_entry_price * (1 - next_exit_pct / 100)
            
        exit_qty = trade.total_qty * (close_pct / 100.0)
        est_profit = exit_qty * abs(next_exit_price - trade.avg_entry_price)
        
        r = await session.execute(
            select(TradeHistory).where(TradeHistory.trade_id == trade_id).order_by(TradeHistory.ts)
        )
        
        items = []
        for h in r.scalars().all():
            pnl_to_show = float(h.pnl) if h.action in ('PARTIAL_CLOSE', 'FULL_CLOSE', 'MANUAL_CLOSE') and h.pnl else 0.0
            
            items.append({
                'ts': h.ts.isoformat(), 'action': h.action, 'price': float(h.price) if h.price else 0.0,
                'qty': float(h.qty) if h.qty else 0.0, 'margin': float(h.margin) if h.margin else 0.0, 
                'pnl': pnl_to_show, 'notes': h.notes,
            })
            
        return {
            'items': items,
            'next_grid_pct': round(next_step_pct, 2),
            'next_grid_price': round(float(next_grid_price), 6),
            'next_grid_size': round(float(next_grid_size), 2),
            'current_grid_step': current_grid_step,
            'max_steps': trade.max_steps,
            'next_exit_pct': round(next_exit_pct, 2),
            'next_exit_price': round(float(next_exit_price), 6),
            'exit_close_pct': close_pct,
            'exit_qty': round(float(exit_qty), 2),
            'est_profit': round(float(est_profit), 2)
        }

@app.post('/api/simulate')
async def api_simulate(request: Request):
    data = await request.json()
    
    entry_price = float(data.get('entry_price', 0))
    entry_size = float(data.get('entry_size', 6))
    grid_step_pct = float(data.get('grid_step_pct', 5))
    grid_step_mult = float(data.get('grid_step_mult', 2.5))
    grid_size_mult = float(data.get('grid_size_mult', 1.0))
    max_steps = int(data.get('max_steps', 10))
    exit_pct = float(data.get('exit_pct', 15))
    exit_step_mult = float(data.get('exit_step_mult', 1.6))
    exit_contract_pct = float(data.get('exit_contract_pct', 30))
    fee_pct = float(data.get('fee_pct', 0.055))
    leverage = float(data.get('leverage', 10))
    side = data.get('side', 'LONG')
    
    initial_balance = float(data.get('balance', 100))
    if initial_balance <= 0:
        initial_balance = 100.0

    mmr = 0.005

    if entry_price <= 0:
        return {'ok': False, 'error': 'Invalid entry price'}

    loss_steps = []
    
    total_qty = entry_size / entry_price
    avg_price = entry_price
    total_position_nominal = entry_size
    total_entry_fee = entry_size * (fee_pct / 100)
    
    used_margin = entry_size / leverage
    realized_pnl = 0.0

    def equity_at(price, qty, avg):
        if side == 'LONG':
            unreal = qty * (price - avg)
        else:
            unreal = qty * (avg - price)
        return initial_balance + realized_pnl + unreal - total_entry_fee

    loss_steps.append({
        'step': 0,
        'price': entry_price,
        'qty': total_qty,
        'total_qty': total_qty,
        'avg_price': avg_price,
        'position_usdt': total_position_nominal,
        'pnl': -total_entry_fee,
        'action': 'Вход',
        'margin': used_margin
    })

    current_price = entry_price

    for i in range(1, max_steps + 1):
        step_pct = min(grid_step_pct * (grid_step_mult ** (i - 1)), 90.0)
        
        if side == 'LONG':
            new_price = current_price * (1 - step_pct / 100)
            current_price = max(new_price, 0.0000001)
        else:
            current_price = current_price * (1 + step_pct / 100)

        eq = equity_at(current_price, total_qty, avg_price)
        liq_threshold = used_margin * mmr
        if eq <= liq_threshold:
            loss_steps.append({
                'step': i,
                'price': current_price,
                'qty': 0,
                'total_qty': total_qty,
                'avg_price': avg_price,
                'position_usdt': total_position_nominal,
                'pnl': eq - initial_balance,
                'action': 'ЛИКВИДАЦИЯ',
                'margin': used_margin
            })
            break

        add_size = entry_size * (grid_size_mult ** (i - 1))
        add_size = max(6.0, min(add_size, 1_000_000))
        required_margin = add_size / leverage
        
        free_margin = eq - used_margin
        if free_margin < required_margin:
            loss_steps.append({
                'step': i,
                'price': current_price,
                'qty': 0,
                'total_qty': total_qty,
                'avg_price': avg_price,
                'position_usdt': total_position_nominal,
                'pnl': eq - initial_balance,
                'action': 'СТОП (мало маржи)',
                'margin': used_margin
            })
            break
            
        used_margin += required_margin
        add_qty = add_size / current_price
        
        old_total_qty = total_qty
        total_qty += add_qty
        avg_price = (avg_price * old_total_qty + current_price * add_qty) / total_qty
        
        add_fee = add_size * (fee_pct / 100)
        total_entry_fee += add_fee
        total_position_nominal += add_size
        
        eq_after = equity_at(current_price, total_qty, avg_price)
        net_pnl = eq_after - initial_balance
        
        loss_steps.append({
            'step': i,
            'price': current_price,
            'qty': add_qty,
            'total_qty': total_qty,
            'avg_price': avg_price,
            'position_usdt': total_position_nominal,
            'pnl': net_pnl,
            'action': f'Уср. {i}',
            'margin': used_margin
        })

    profit_steps = []
    current_price = entry_price
    total_qty = entry_size / entry_price
    avg_price = entry_price
    total_entry_fee = entry_size * (fee_pct / 100)
    realized_pnl = 0.0
    exit_step = 0
    remaining_qty = total_qty
    
    profit_steps.append({
        'step': 0,
        'price': entry_price,
        'qty': total_qty,
        'total_qty': total_qty,
        'avg_price': avg_price,
        'position_usdt': total_qty * entry_price,
        'pnl': 0.0,
        'action': 'Вход',
        'margin': 0
    })

    while remaining_qty > 0.0001 and exit_step < 10:
        exit_step += 1
        target_exit_pct = exit_pct * (exit_step_mult ** (exit_step - 1))
        
        if side == 'LONG':
            current_price = current_price * (1 + target_exit_pct / 100)
        else:
            current_price = current_price * (1 - target_exit_pct / 100)
            
        close_qty = remaining_qty * (exit_contract_pct / 100)
        
        if close_qty * current_price < 1.0:
            close_qty = remaining_qty
            action_label = 'Закрытие (остаток < 1$)'
        else:
            action_label = f'Выход {exit_step} ({exit_contract_pct}%)'
        
        if side == 'LONG':
            part_pnl = close_qty * (current_price - avg_price)
        else:
            part_pnl = close_qty * (avg_price - current_price)
            
        exit_fee = (close_qty * current_price) * (fee_pct / 100)
        
        entry_fee_part = total_entry_fee * (close_qty / total_qty) if total_qty > 0 else 0
        net_part_pnl = part_pnl - exit_fee - entry_fee_part
        
        realized_pnl += net_part_pnl
        remaining_qty -= close_qty
        
        profit_steps.append({
            'step': exit_step,
            'price': current_price,
            'qty': close_qty,
            'total_qty': remaining_qty,
            'avg_price': avg_price,
            'position_usdt': remaining_qty * current_price,
            'pnl': realized_pnl,
            'action': action_label,
            'margin': 0
        })
        
        if remaining_qty < 0.0001:
            break

    return {
        'ok': True,
        'loss_steps': loss_steps,
        'profit_steps': profit_steps
    }

@app.get("/api/configs")
async def api_get_configs():
    async with SessionLocal() as session:
        r = await session.execute(select(SymbolConfig).order_by(SymbolConfig.symbol))
        return {
            'items': [
                {
                    'symbol': c.symbol, 'entry_pct': float(c.entry_pct) if c.entry_pct else 0.0,
                    'grid_step_pct': float(c.grid_step_pct) if c.grid_step_pct else 0.0, 
                    'grid_step_multiplier': float(c.grid_step_multiplier) if c.grid_step_multiplier else 0.0,
                    'grid_size_multiplier': float(c.grid_size_multiplier) if c.grid_size_multiplier else 0.0,
                    'max_steps': c.max_steps, 
                    'initial_exit_pct': float(c.initial_exit_pct) if c.initial_exit_pct else 0.0,
                    'exit_step_multiplier': float(c.exit_step_multiplier) if c.exit_step_multiplier else 0.0,
                    'exit_contract_pct': float(c.exit_contract_pct) if c.exit_contract_pct else 0.0,
                    'position_size_entry': float(c.position_size_entry) if c.position_size_entry else 0.0, 
                    'position_size_grid': float(c.position_size_grid) if c.position_size_grid else 0.0,
                }
                for c in r.scalars().all()
            ]
        }

@app.post("/api/configs")
async def api_post_configs(request: Request):
    data = await request.json()
    async with SessionLocal() as session:
        cfg = await session.get(SymbolConfig, data["symbol"])
        if not cfg:
            cfg = SymbolConfig(symbol=data["symbol"])
            session.add(cfg)
        for k in ["entry_pct", "grid_step_pct", "grid_step_multiplier", "grid_size_multiplier", "max_steps", "initial_exit_pct", "exit_step_multiplier", "exit_contract_pct", "position_size_entry", "position_size_grid", "min_confidence"]:
            if k in data:
                setattr(cfg, k, float(data[k]) if k != "max_steps" else int(data[k]))
        await session.commit()
    return {"ok": True}

@app.get("/api/settings")
async def api_get_settings():
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if not s:
            s = Settings(id=1)
            session.add(s)
            await session.commit()
        return {
            "min_turnover": float(s.min_turnover) if s.min_turnover else 0.0, 
            "min_leverage": float(s.min_leverage) if s.min_leverage else 0.0,
            "entry_pct": float(s.entry_pct) if s.entry_pct else 0.0, 
            "grid_step_pct": float(s.grid_step_pct) if s.grid_step_pct else 0.0,
            "grid_step_multiplier": float(s.grid_step_multiplier) if s.grid_step_multiplier else 0.0, 
            "grid_size_multiplier": float(s.grid_size_multiplier) if s.grid_size_multiplier else 0.0,
            "max_steps": s.max_steps,
            "initial_exit_pct": float(s.initial_exit_pct) if s.initial_exit_pct else 0.0, 
            "exit_step_multiplier": float(s.exit_step_multiplier) if s.exit_step_multiplier else 0.0,
            "exit_contract_pct": float(s.exit_contract_pct) if s.exit_contract_pct else 0.0,
            "position_size_entry": float(s.position_size_entry) if s.position_size_entry else 0.0, 
            "position_size_grid": float(s.position_size_grid) if s.position_size_grid else 0.0,
            "fee_pct": float(s.fee_pct) if s.fee_pct else 0.055, 
            "min_confidence": float(s.min_confidence) if s.min_confidence else 0.4,
        }

@app.post("/api/settings")
async def api_post_settings(data: dict = Body(...)):
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if not s:
            s = Settings(id=1)
            session.add(s)
        for k in ["min_turnover", "min_leverage", "entry_pct", "grid_step_pct", "grid_step_multiplier", "grid_size_multiplier", "max_steps", "initial_exit_pct", "exit_step_multiplier", "exit_contract_pct", "position_size_entry", "position_size_grid", "fee_pct", "min_confidence"]:
            if k in data:
                setattr(s, k, float(data[k]) if k != "max_steps" else int(data[k]))
        await session.commit()
    asyncio.create_task(monitor._refresh_symbols())
    return {"ok": True}

@app.post("/api/clear_data")
async def api_clear_data():
    async with SessionLocal() as session:
        await session.execute(delete(Trade))
        await session.execute(delete(TradeHistory))
        await session.commit()
    state.alerts.clear()
    return {"ok": True, "message": "Data cleared"}

@app.get("/favicon.ico")
async def favicon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🔪</text></svg>'
    return Response(content=svg, media_type="image/svg+xml")

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/api/klines")
async def api_klines(symbol: str, interval: str, limit: int = 200):
    # interval: 1, 5, 15, 30, 60, 240, D
    if not monitor._rest:
        return {"ok": False, "error": "REST not ready"}
    
    try:
        # Ваш метод get_kline принимает строки '240', 'D' и т.д.
        # Для 1ч Bybit использует '60'
        kl = await monitor._rest.get_kline(symbol, interval=interval, limit=limit)
        return {"ok": True, "items": kl}
    except Exception as e:
        return {"ok": False, "error": str(e)}    