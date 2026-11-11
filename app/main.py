import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from .config import settings
from .monitor import monitor
from .state import state
from .db import init_db, SessionLocal, SymbolStat, Trade, Settings
from .simulator import Simulator

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("app")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Глобальный симулятор, инициализируется при старте
simulator = None


async def run_history_bg():
    """Фоновая задача для сканирования истории (запускается через 60 сек после старта)"""
    log.info("run_history_bg: waiting 60s before scan")
    await asyncio.sleep(60)
    
    if simulator is None:
        log.error("run_history_bg: simulator is None")
        return
        
    log.info("run_history_bg: starting history scan")
    try:
        await simulator.run_history_scan()
        log.info("run_history_bg: scan finished")
    except Exception:
        log.exception("run_history_bg: scan crashed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global simulator
    # 1. Инициализация БД (создание таблиц)
    await init_db()
    
    # 2. Запуск монитора (WS + REST)
    await monitor.start()
    
    # 3. Инициализация симулятора с REST-клиентом монитора
    simulator = Simulator(monitor._rest)
    log.info("simulator initialized: %r", simulator)
    
    # 4. Запуск фонового скана
    asyncio.create_task(run_history_bg())
    
    try:
        yield
    finally:
        await monitor.stop()


app = FastAPI(lifespan=lifespan, title="Bybit Pump/Dump Monitor")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/status")
async def api_status():
    # Читаем настройки из БД (если они есть), иначе используем .env
    db_min_lev = settings.min_max_leverage
    db_min_turnover = 5.0
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if s:
            db_min_lev = s.min_leverage
            db_min_turnover = s.min_turnover

    return {
        "uptime_sec": int(time.time() - state.started_at),
        "symbols_count": len(state.symbols),
        "symbols_last_refresh": getattr(state, "last_symbols_refresh", 0),
        "threshold_pct": settings.threshold * 100,
        "window_minutes": settings.window_minutes,
        "ma_period": settings.ma_period,
        "min_max_leverage": db_min_lev,
        "min_turnover": db_min_turnover,
        "alerts_count": len(state.alerts),
        "server_time": time.time(),
    }


@app.get("/api/alerts")
async def api_alerts(limit: int = 100):
    limit = max(1, min(limit, settings.max_alerts))
    return {
        "items": [
            {
                "ts": a.ts,
                "symbol": a.symbol,
                "kind": a.kind,
                "price_now": a.price_now,
                "ref_price": a.ref_price,
                "ma": a.ma,
                "change_pct": a.change_pct,
            }
            for a in list(state.alerts)[:limit]
        ]
    }


@app.get("/api/symbols")
async def api_symbols():
    return {
        "items": [
            {
                "symbol": s.symbol,
                "last_price": s.last_price,
                "ma": s.ma,
                "max_leverage": s.max_leverage,
                "last_price_ts": s.last_price_ts,
            }
            for s in state.symbols.values()
        ]
    }


@app.get("/api/stats")
async def api_stats(limit: int = 50):
    async with SessionLocal() as session:
        r = await session.execute(
            select(SymbolStat).order_by(SymbolStat.entry_long_pct.desc()).limit(limit)
        )
        return {
            "items": [
                {
                    "symbol": s.symbol,
                    "avg_prostrel_pct": round(s.avg_prostrel_pct, 2),
                    "max_prostrel_pct": round(s.max_prostrel_pct, 2),
                    "freq_per_day": round(s.freq_per_day, 2),
                    "entry_long_pct": round(s.entry_long_pct, 2),
                    "max_leverage": s.max_leverage,  # <-- ДОБАВИТЬ ЭТУ СТРОКУ
                }
                for s in r.scalars().all()
            ]
        }


@app.get("/api/trades")
async def api_trades(limit: int = 100):
    async with SessionLocal() as session:
        r = await session.execute(
            select(Trade).order_by(Trade.opened_at.desc()).limit(limit)
        )
        return {
            "items": [
                {
                    "id": t.id,
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry_price": t.entry_price,
                    "avg_entry_price": t.avg_entry_price,
                    "current_price": t.current_price,
                    "grid_step": t.grid_step,
                    "leverage": t.leverage,
                    "total_qty": t.total_qty,
                    "total_margin": t.total_margin,
                    "status": t.status,
                    "pnl": t.pnl,
                    "opened_at": t.opened_at.isoformat() if t.opened_at else None,
                    "closed_at": t.closed_at.isoformat() if t.closed_at else None,
                }
                for t in r.scalars().all()
            ]
        }



@app.get("/api/settings")
async def api_get_settings():
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if not s:
            s = Settings(id=1, min_turnover=5.0, min_leverage=10.0)
            session.add(s)
            await session.commit()
        return {"min_turnover": s.min_turnover, "min_leverage": s.min_leverage}


@app.post("/api/settings")
async def api_post_settings(request: Request):
    data = await request.json()
    async with SessionLocal() as session:
        s = await session.get(Settings, 1)
        if not s:
            s = Settings(id=1)
            session.add(s)
        if "min_turnover" in data:
            s.min_turnover = float(data["min_turnover"])
        if "min_leverage" in data:
            s.min_leverage = float(data["min_leverage"])
        await session.commit()
        return {"ok": True, "min_turnover": s.min_turnover, "min_leverage": s.min_leverage}


@app.get("/favicon.ico")

async def favicon():
    return Response(status_code=204)


@app.get("/healthz")
async def healthz():
    return {"ok": True}