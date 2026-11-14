import datetime
import os
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import String, Float, Integer, DateTime, Text
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
    grid_step_pct: Mapped[float] = mapped_column(Float, default=5.0)
    grid_step_multiplier: Mapped[float] = mapped_column(Float, default=2.5)
    grid_size_multiplier: Mapped[float] = mapped_column(Float, default=1.0)
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    position_size_entry: Mapped[float] = mapped_column(Float, default=6.0)
    position_size_grid: Mapped[float] = mapped_column(Float, default=6.0)
    initial_exit_pct: Mapped[float] = mapped_column(Float, default=15.0)
    exit_step_multiplier: Mapped[float] = mapped_column(Float, default=1.6)
    exit_contract_pct: Mapped[float] = mapped_column(Float, default=50.0)
    min_confidence: Mapped[float] = mapped_column(Float, default=0.4)
    fee_pct: Mapped[float] = mapped_column(Float, default=0.055)
    
    # --- Новые настройки ---
    paper_balance: Mapped[float] = mapped_column(Float, default=100.0)
    max_open_trades: Mapped[int] = mapped_column(Integer, default=5)
    check_validity_before_grid: Mapped[bool] = mapped_column(default=False)
    exit_max_steps: Mapped[int] = mapped_column(Integer, default=0)  # 0 = без ограничения

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
    grid_size_multiplier: Mapped[float] = mapped_column(Float, default=0.0)
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
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    entry_fee: Mapped[float] = mapped_column(Float, default=0.0)
    exit_fee: Mapped[float] = mapped_column(Float, default=0.0)
    current_price: Mapped[float] = mapped_column(Float, default=0.0)
    grid_step: Mapped[int] = mapped_column(Integer, default=0)
    exit_step: Mapped[int] = mapped_column(Integer, default=0)
    max_steps: Mapped[int] = mapped_column(Integer, default=10)
    leverage: Mapped[float] = mapped_column(Float, default=10.0)
    opened_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.utcnow)
    closed_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)
    tf: Mapped[str] = mapped_column(String, default='60')
    
    # --- Новые поля ---
    validity: Mapped[str] = mapped_column(String, default='green')  # green / orange / red
    validity_reasons: Mapped[str] = mapped_column(Text, nullable=True)  # JSON-массив причин
    exit_max_steps: Mapped[int] = mapped_column(Integer, default=0)  # 0 = без ограничения


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
    details: Mapped[str] = mapped_column(Text, nullable=True) # НОВОЕ ПОЛЕ

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
        ('settings', 'grid_size_multiplier', 'FLOAT DEFAULT 1.0'),
        ('symbol_configs', 'exit_step_multiplier', 'FLOAT DEFAULT 0'),
        ('symbol_configs', 'min_confidence', 'FLOAT DEFAULT 0'),
        ('symbol_configs', 'grid_size_multiplier', 'FLOAT DEFAULT 0'),
        ('trades', 'entry_fee', 'FLOAT DEFAULT 0'),
        ('trades', 'exit_fee', 'FLOAT DEFAULT 0'),
        ('trades', 'realized_pnl', 'FLOAT DEFAULT 0'),
        ('trades', 'tf', "VARCHAR DEFAULT '60'"),
        ('trade_history', 'details', 'TEXT'), # НОВОЕ ПОЛЕ
        # --- Миграция новых полей ---
        ('settings', 'paper_balance', 'FLOAT DEFAULT 100'),
        ('settings', 'max_open_trades', 'INTEGER DEFAULT 5'),
        ('settings', 'check_validity_before_grid', 'INTEGER DEFAULT 0'),
        ('settings', 'exit_max_steps', 'INTEGER DEFAULT 0'),
        ('trades', 'validity', "VARCHAR DEFAULT 'green'"),
        ('trades', 'exit_max_steps', 'INTEGER DEFAULT 0'),
        ('trades', 'validity', "VARCHAR DEFAULT 'green'"),
        ('trades', 'validity_reasons', 'TEXT'),
        ('trades', 'exit_max_steps', 'INTEGER DEFAULT 0'),
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