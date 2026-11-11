import datetime
import os
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DB_URL = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///data/knife.db')

if DB_URL.startswith('postgresql'):
    connect_args = {
        'statement_cache_size': 0,
        'prepared_statement_cache_size': 0,
        'server_settings': {'jit': 'off'},
    }
else:
    connect_args = {}

engine = create_async_engine(DB_URL, echo=False, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Settings(Base):
    __tablename__ = 'settings'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    min_turnover: Mapped[float] = mapped_column(Float, default=5.0)
    min_leverage: Mapped[float] = mapped_column(Float, default=10.0)

class SymbolStat(Base):
    __tablename__ = 'symbol_stats'
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    max_leverage: Mapped[float] = mapped_column(Float, default=0.0)
    avg_prostrel_pct: Mapped[float] = mapped_column(Float, default=0.0)
    max_prostrel_pct: Mapped[float] = mapped_column(Float, default=0.0)
    freq_per_day: Mapped[float] = mapped_column(Float, default=0.0)
    entry_long_pct: Mapped[float] = mapped_column(Float, default=0.0)
    entry_short_pct: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)

class SymbolConfig(Base):
    __tablename__ = 'symbol_configs'
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    entry_pct: Mapped[float] = mapped_column(Float, default=0.0) 
    min_entry_pct: Mapped[float] = mapped_column(Float, default=5.0)
    grid_step_pct: Mapped[float] = mapped_column(Float, default=5.0)
    grid_step_multiplier: Mapped[float] = mapped_column(Float, default=2.5)
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    initial_exit_pct: Mapped[float] = mapped_column(Float, default=15.0)
    exit_contract_pct: Mapped[float] = mapped_column(Float, default=50.0)
    max_margin: Mapped[float] = mapped_column(Float, default=1.0)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)

class Trade(Base):
    __tablename__ = 'trades'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String, index=True)
    side: Mapped[str] = mapped_column(String)
    entry_price: Mapped[float] = mapped_column(Float)
    avg_entry_price: Mapped[float] = mapped_column(Float)
    total_qty: Mapped[float] = mapped_column(Float, default=0.0)
    total_margin: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String, default='OPEN')
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    current_price: Mapped[float] = mapped_column(Float, default=0.0)
    grid_step: Mapped[int] = mapped_column(Integer, default=0)
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    leverage: Mapped[float] = mapped_column(Float, default=10.0)
    opened_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)
    closed_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[int] = mapped_column(Integer, index=True)
    ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)
    action: Mapped[str] = mapped_column(String)  # OPEN, GRID_ADD, PARTIAL_CLOSE, FULL_CLOSE
    price: Mapped[float] = mapped_column(Float)
    qty: Mapped[float] = mapped_column(Float)
    margin: Mapped[float] = mapped_column(Float)
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    notes: Mapped[str] = mapped_column(String, default='')

async def init_db():
    os.makedirs('data', exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)