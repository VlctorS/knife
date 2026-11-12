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
    
    entry_pct: Mapped[float] = mapped_column(Float, default=0.0)
    # --- Усреднение (сетка): шаг и его рост по экспоненте ---
    grid_step_pct: Mapped[float] = mapped_column(Float, default=5.0)
    grid_step_multiplier: Mapped[float] = mapped_column(Float, default=2.5)
    grid_size_multiplier: Mapped[float] = mapped_column(Float, default=1.0) # <--- НОВОЕ
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    position_size_entry: Mapped[float] = mapped_column(Float, default=6.0)
    position_size_grid: Mapped[float] = mapped_column(Float, default=6.0)
    # --- Выход: своя прогрессия, независимая от сетки ---
    initial_exit_pct: Mapped[float] = mapped_column(Float, default=15.0)
    exit_step_multiplier: Mapped[float] = mapped_column(Float, default=1.6)
    exit_contract_pct: Mapped[float] = mapped_column(Float, default=50.0)
    min_confidence: Mapped[float] = mapped_column(Float, default=0.4)
    fee_pct: Mapped[float] = mapped_column(Float, default=0.055)  # taker, % на каждую сторону

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
    grid_step_pct: Mapped[float] = mapped_column(Float, default=0.0)
    grid_step_multiplier: Mapped[float] = mapped_column(Float, default=0.0)
    grid_size_multiplier: Mapped[float] = mapped_column(Float, default=0.0) # <--- НОВОЕ
    max_steps: Mapped[int] = mapped_column(Integer, default=0)
    position_size_entry: Mapped[float] = mapped_column(Float, default=0.0)
    position_size_grid: Mapped[float] = mapped_column(Float, default=0.0)
    initial_exit_pct: Mapped[float] = mapped_column(Float, default=0.0)
    exit_step_multiplier: Mapped[float] = mapped_column(Float, default=0.0)
    exit_contract_pct: Mapped[float] = mapped_column(Float, default=0.0)
    min_confidence: Mapped[float] = mapped_column(Float, default=0.0)
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
    initial_margin: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String, default='OPEN')
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    # Реализованный PnL закрытых частей (уже за вычетом комиссий входа и выхода)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    # Сумма уплаченных комиссий: вход+усреднения и выходы считаются раздельно
    entry_fee: Mapped[float] = mapped_column(Float, default=0.0)
    exit_fee: Mapped[float] = mapped_column(Float, default=0.0)
    current_price: Mapped[float] = mapped_column(Float, default=0.0)
    grid_step: Mapped[int] = mapped_column(Integer, default=0)
    exit_step: Mapped[int] = mapped_column(Integer, default=0)  # <--- ДОБАВИТЬ ЭТО
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    leverage: Mapped[float] = mapped_column(Float, default=10.0)
    opened_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)
    closed_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)


class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[int] = mapped_column(Integer, index=True)
    ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)
    action: Mapped[str] = mapped_column(String)  # OPEN, GRID_ADD, PARTIAL_CLOSE, FULL_CLOSE, MANUAL_CLOSE
    price: Mapped[float] = mapped_column(Float)
    qty: Mapped[float] = mapped_column(Float)
    margin: Mapped[float] = mapped_column(Float)
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    notes: Mapped[str] = mapped_column(String, default='')

async def init_db():
    os.makedirs('data', exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_migrate_add_columns)


def _migrate_add_columns(conn):
    """create_all не трогает существующие таблицы — добавляем недостающие колонки."""
    from sqlalchemy import inspect, text

    insp = inspect(conn)
    # (таблица, колонка, DDL-тип) — новые поля
    additions = [
        ('settings', 'exit_step_multiplier', 'FLOAT DEFAULT 1.6'),
        ('settings', 'min_confidence', 'FLOAT DEFAULT 0.4'),
        ('settings', 'grid_size_multiplier', 'FLOAT DEFAULT 1.0'), # <--- НОВОЕ
        ('symbol_configs', 'exit_step_multiplier', 'FLOAT DEFAULT 0'),
        ('symbol_configs', 'min_confidence', 'FLOAT DEFAULT 0'),
        ('symbol_configs', 'grid_size_multiplier', 'FLOAT DEFAULT 0'), # <--- НОВОЕ
        ('trades', 'entry_fee', 'FLOAT DEFAULT 0'),
        ('trades', 'exit_fee', 'FLOAT DEFAULT 0'),
        ('trades', 'realized_pnl', 'FLOAT DEFAULT 0'),
    ]
    for table, column, ddl in additions:
        if table not in insp.get_table_names():
            continue
        
        # Проверяем существование колонки
        existing = {c['name'] for c in insp.get_columns(table)}
        if column in existing:
            continue
        
        # Пытаемся добавить, но ловим ошибку, если колонка уже есть (кэш asyncpg)
        try:
            conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}'))
        except Exception as e:
            # Если ошибка "already exists" — просто игнорируем
            if 'already exists' in str(e).lower() or 'duplicate column' in str(e).lower():
                pass
            else:
                raise e