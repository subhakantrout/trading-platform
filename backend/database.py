from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Text, DateTime,
    Index, text
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from config import DB_PATH

from sqlalchemy.pool import NullPool

ASYNC_DB_URL = f"sqlite+aiosqlite:///{DB_PATH}"
SYNC_DB_URL = f"sqlite:///{DB_PATH}"

async_engine = create_async_engine(ASYNC_DB_URL, echo=False, connect_args={"timeout": 30}, poolclass=NullPool)
sync_engine = create_engine(SYNC_DB_URL, echo=False, connect_args={"timeout": 30}, poolclass=NullPool)

AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession)
SyncSessionLocal = sessionmaker(sync_engine)

class Base(DeclarativeBase):
    pass

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(32), nullable=False, index=True)
    stream_id = Column(String(64), nullable=False, index=True)
    bid = Column(Float)
    ask = Column(Float)
    spread = Column(Float)
    last = Column(Float)
    change = Column(Float)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    indicators_json = Column(Text)
    account_json = Column(Text)
    structure_json = Column(Text)
    volume_profile_json = Column(Text)
    patterns_json = Column(Text)
    advanced_metrics_json = Column(Text)
    session_json = Column(Text)
    symbol_info_json = Column(Text)

    __table_args__ = (
        Index("idx_stream_time", "stream_id", "timestamp"),
    )

class MTFData(Base):
    __tablename__ = "mtf_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)
    stream_id = Column(String(64), nullable=False, index=True)
    mtf_json = Column(Text)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)

class CorrelationData(Base):
    __tablename__ = "correlation_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)
    correlation_json = Column(Text)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)

class EconomicCalendar(Base):
    __tablename__ = "economic_calendar"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)
    events_json = Column(Text)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)

class Candle(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(32), nullable=False)
    timeframe = Column(String(32), nullable=False)
    time = Column(Integer, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float, default=0.0)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("idx_candle_stream_time", "stream_id", "time", unique=True),
    )

class Analysis(Base):
    __tablename__ = "analyses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(32))
    timeframe = Column(String(32))
    analysis_type = Column(String(16))
    model = Column(String(64))
    prompt = Column(Text)
    result = Column(Text)
    bias = Column(String(16))
    confidence = Column(Float)
    agent_votes_json = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Trade(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(32), nullable=False)
    direction = Column(String(8))
    entry_price = Column(Float)
    exit_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True)
    quantity = Column(Float)
    status = Column(String(16), default="open")
    entry_time = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    exit_time = Column(DateTime, nullable=True)
    profit = Column(Float, nullable=True)
    profit_pips = Column(Float, nullable=True)
    strategy = Column(String(32))
    analysis_id = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)
    risk_percent = Column(Float)
    r_multiple = Column(Float, nullable=True)

class BacktestRun(Base):
    __tablename__ = "backtest_runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32))
    timeframe = Column(String(32))
    strategy_name = Column(String(64))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    initial_capital = Column(Float)
    final_capital = Column(Float)
    total_return = Column(Float)
    sharpe_ratio = Column(Float)
    sortino_ratio = Column(Float)
    max_drawdown = Column(Float)
    win_rate = Column(Float)
    total_trades = Column(Integer)
    profit_factor = Column(Float)
    metrics_json = Column(Text)
    trades_json = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class AgentPerformance(Base):
    __tablename__ = "agent_performance"
    id = Column(Integer, primary_key=True, autoincrement=True)
    agent_name = Column(String(32), nullable=False, index=True)
    model = Column(String(64))
    total_predictions = Column(Integer, default=0)
    correct_predictions = Column(Integer, default=0)
    accuracy = Column(Float, default=0.0)
    elo_rating = Column(Float, default=1200.0)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class SystemEvent(Base):
    __tablename__ = "system_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String(32), index=True)
    message = Column(Text)
    details_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

async def init_db():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("PRAGMA journal_mode=WAL"))

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

def get_sync_db():
    db = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_sync_db():
    Base.metadata.create_all(sync_engine)
    with sync_engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.commit()
