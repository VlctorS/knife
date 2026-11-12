import asyncio
import logging
import os
import time
import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
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
            }
            for a in list(state.alerts)[:limit]
        ]
    }

@app.get('/api/stats')
async def api_stats(limit: int = 50):
    async with SessionLocal() as session:
        r = await session.execute(select(SymbolStat).order_by(SymbolStat.entry_long_pct.desc()).limit(limit))
        configs_res = await session.execute(select(SymbolConfig))
        configs = {c.symbol: c for c in configs_res.scalars().all()}
        gs = await session.get(Settings, 1)
        
        items = []
        for s in r.scalars().all():
            cfg = configs.get(s.symbol)
            
            # Если есть индивидуальная настройка > 0 - используем её
            if cfg and cfg.entry_pct > 0:
                effective_entry = cfg.entry_pct
                is_manual = True
            # Если есть глобальная настройка > 0 - используем её
            elif gs and gs.entry_pct > 0:
                effective_entry = gs.entry_pct
                is_manual = True
            # Иначе считаем автоматически (без жестких минимумов в 5% или 3%)
            else:
                if s.avg_prostrel_pct > 0:
                    effective_entry = s.avg_prostrel_pct * 0.85
                else:
                    effective_entry = 5.0  # Фоллбек, если нет статистики
                is_manual = False
            
            items.append({
                'symbol': s.symbol,
                'avg_prostrel_pct': round(s.avg_prostrel_pct, 2),
                'max_prostrel_pct': round(s.max_prostrel_pct, 2),
                'freq_per_day': round(s.freq_per_day, 2),
                'entry_long_pct': round(effective_entry, 2),
                'is_manual': is_manual,
                'max_leverage': s.max_leverage,
            })
            
        return {'items': items}

@app.get('/api/trades')
async def api_trades(limit: int = 100):
    async with SessionLocal() as session:
        r = await session.execute(
            select(Trade).where(Trade.status.in_(['OPEN', 'PARTIAL'])).order_by(Trade.opened_at.desc()).limit(limit)
        )
        items = []
        for t in r.scalars().all():
            if t.avg_entry_price and t.current_price and t.avg_entry_price > 0:
                if t.side == 'LONG':
                    change_pct = (t.current_price - t.avg_entry_price) / t.avg_entry_price * 100
                else:
                    change_pct = (t.avg_entry_price - t.current_price) / t.avg_entry_price * 100
            else:
                change_pct = 0
            
            items.append({
                'id': t.id, 'symbol': t.symbol, 'side': t.side,
                'entry_price': t.entry_price, 'avg_entry_price': t.avg_entry_price,
                'current_price': t.current_price, 'grid_step': t.grid_step,
                'total_qty': t.total_qty, 'total_margin': t.total_margin,
                'status': t.status, 'pnl': t.pnl, 'leverage': t.leverage,
                'change_pct': round(change_pct, 2),
                'opened_at': t.opened_at.isoformat() if t.opened_at else None,
                'closed_at': t.closed_at.isoformat() if t.closed_at else None,
            })
        return {'items': items}

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
                'entry_price': t.entry_price, 'avg_entry_price': t.avg_entry_price,
                'total_qty': t.total_qty, 'total_margin': t.total_margin,
                'pnl': t.pnl, 'leverage': t.leverage,
                'opened_at': t.opened_at.isoformat() if t.opened_at else None,
                'closed_at': t.closed_at.isoformat() if t.closed_at else None,
            })
        return {'items': items}

@app.get('/api/closed_stats')
async def api_closed_stats():
    async with SessionLocal() as session:
        # Все закрытые сделки
        r = await session.execute(
            select(Trade).where(Trade.status == 'CLOSED')
        )
        all_trades = r.scalars().all()
        
        # Статистика по всем
        total_pnl = sum(t.pnl or 0 for t in all_trades)
        total_count = len(all_trades)
        
        wins = [t for t in all_trades if (t.pnl or 0) > 0]
        losses = [t for t in all_trades if (t.pnl or 0) < 0]
        
        win_count = len(wins)
        loss_count = len(losses)
        
        win_rate = (win_count / total_count * 100) if total_count > 0 else 0
        
        gross_profit = sum(t.pnl for t in wins if t.pnl)
        gross_loss = abs(sum(t.pnl for t in losses if t.pnl))
        
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.99 if gross_profit > 0 else 0)
        
        # Сегодня (с 00:00 по UTC, но можно и по МСК)
        # Проще: с 00:00 текущего дня (UTC)
        today_start = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        today_trades = [t for t in all_trades if t.closed_at and t.closed_at >= today_start]
        today_pnl = sum(t.pnl or 0 for t in today_trades)
        today_count = len(today_trades)
        
        return {
            'total_pnl': round(total_pnl, 4),
            'total_count': total_count,
            'win_count': win_count,
            'loss_count': loss_count,
            'win_rate': round(win_rate, 1),
            'profit_factor': round(profit_factor, 2),
            'today_pnl': round(today_pnl, 4),
            'today_count': today_count,
        }

@app.post('/api/clear_closed_trades')
async def api_clear_closed_trades(request: Request):
    data = await request.json()
    mode = data.get('mode', 'all')
    
    async with SessionLocal() as session:
        if mode == 'today':
            today_start = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
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
        
        # Закрываем всю оставшуюся позицию
        closed_qty = t.total_qty
        if t.side == 'LONG':
            closed_pnl = (closed_qty * price) - (closed_qty * t.avg_entry_price)
        else:
            closed_pnl = (closed_qty * t.avg_entry_price) - (closed_qty * price)
        
        # Обновляем сделку
        t.status = 'CLOSED'
        t.closed_at = datetime.datetime.utcnow()
        t.current_price = price
        t.pnl = closed_pnl          # <-- СОХРАНЯЕМ PnL в базе
        t.total_qty = 0.0           # <-- Обнуляем остаток
        t.total_margin = 0.0        # <-- Обнуляем маржу
        
        history = TradeHistory(
            trade_id=t.id, action='MANUAL_CLOSE', price=price, qty=closed_qty,
            margin=t.total_margin, pnl=closed_pnl, notes='manual close'
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
        
        if cfg and cfg.grid_step_pct > 0:
            start_pct = cfg.grid_step_pct
        else:
            start_pct = gs.grid_step_pct if gs else 5.0
        
        if cfg and cfg.grid_step_multiplier > 0:
            mult = cfg.grid_step_multiplier
        else:
            mult = gs.grid_step_multiplier if gs else 2.5
        
        next_step_pct = start_pct * (mult ** trade.grid_step)
        
        r = await session.execute(
            select(TradeHistory).where(TradeHistory.trade_id == trade_id).order_by(TradeHistory.ts)
        )
        
        items = []
        for h in r.scalars().all():
            current_pnl = h.pnl
            if trade.status in ('OPEN', 'PARTIAL') and trade.current_price > 0:
                if trade.side == 'LONG':
                    current_pnl = h.qty * (trade.current_price - h.price)
                else:
                    current_pnl = h.qty * (h.price - trade.current_price)
            
            items.append({
                'ts': h.ts.isoformat(), 'action': h.action, 'price': h.price,
                'qty': h.qty, 'margin': h.margin, 'pnl': current_pnl, 'notes': h.notes,
            })
            
        return {
            'items': items,
            'next_step_pct': round(next_step_pct, 2),
            'current_grid_step': trade.grid_step,
            'max_steps': trade.max_steps,
        }

@app.get("/api/configs")
async def api_get_configs():
    async with SessionLocal() as session:
        r = await session.execute(select(SymbolConfig).order_by(SymbolConfig.symbol))
        return {
            'items': [
                {
                    'symbol': c.symbol, 'entry_pct': c.entry_pct,
                    'grid_step_pct': c.grid_step_pct, 'grid_step_multiplier': c.grid_step_multiplier,
                    'max_steps': c.max_steps, 'initial_exit_pct': c.initial_exit_pct,
                    'exit_contract_pct': c.exit_contract_pct,
                    'position_size_entry': c.position_size_entry, 'position_size_grid': c.position_size_grid,
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
        for k in ["entry_pct", "grid_step_pct", "grid_step_multiplier", "max_steps", "initial_exit_pct", "exit_contract_pct", "position_size_entry", "position_size_grid"]:
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
            "min_turnover": s.min_turnover, "min_leverage": s.min_leverage,
            "entry_pct": s.entry_pct, "grid_step_pct": s.grid_step_pct,
            "grid_step_multiplier": s.grid_step_multiplier, "max_steps": s.max_steps,
            "initial_exit_pct": s.initial_exit_pct, "exit_contract_pct": s.exit_contract_pct,
            "position_size_entry": s.position_size_entry, "position_size_grid": s.position_size_grid
        }

@app.post("/api/settings")
async def api_post_settings(request: Request):
    data = await request.json()
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if not s:
            s = Settings(id=1)
            session.add(s)
        for k in ["min_turnover", "min_leverage", "entry_pct", "grid_step_pct", "grid_step_multiplier", "max_steps", "initial_exit_pct", "exit_contract_pct", "position_size_entry", "position_size_grid"]:
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