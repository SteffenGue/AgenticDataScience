"""
Cryptocurrency Data Models for CCXT and Binance Data Provider
Using SQLAlchemy for PostgreSQL Database Storage
"""

from enum import Enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

# Enums for data categorization
class ExchangeType(Enum):
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    BYBIT = "bybit"
    OTHER = "other"

class TimeFrame(Enum):
    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class Exchange(Base):
    """Exchange information and metadata"""
    __tablename__ = "exchanges"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(50), nullable=False, unique=True)
    exchange_type = Column(SQLEnum(ExchangeType), nullable=False)
    is_active = Column(Boolean, default=True)
    rate_limit = Column(Integer)  # requests per second
    timezone_name = Column(String(50), default="UTC")
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    symbols = relationship("Symbol", back_populates="exchange")
    ohlcv_data = relationship("OHLCV", back_populates="exchange")
    trades = relationship("Trade", back_populates="exchange")


class Symbol(Base):
    """Trading pairs/symbols available on exchanges"""
    __tablename__ = "symbols"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)
    
    # Symbol information
    symbol = Column(String(50), nullable=False)  # e.g., "BTC/USDT"
    base_currency = Column(String(25), nullable=False)  # e.g., "BTC"
    quote_currency = Column(String(25), nullable=False)  # e.g., "USDT"
    
    # Trading rules and limits
    is_active = Column(Boolean, default=True)
    min_order_size = Column(Numeric(20, 8))
    max_order_size = Column(Numeric(20, 8))
    min_price = Column(Numeric(20, 8))
    max_price = Column(Numeric(20, 8))
    price_precision = Column(Integer)
    quantity_precision = Column(Integer)
    
    # Fees
    maker_fee = Column(Numeric(10, 6))  # as decimal (0.001 = 0.1%)
    taker_fee = Column(Numeric(10, 6))
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    exchange = relationship("Exchange", back_populates="symbols")
    ohlcv_data = relationship("OHLCV", back_populates="symbol")
    trades = relationship("Trade", back_populates="symbol")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint("exchange_id", "symbol", name="uq_exchange_symbol"),
        Index("idx_symbol_currencies", "base_currency", "quote_currency"),
    )


class OHLCV(Base):
    """OHLCV (Open, High, Low, Close, Volume) candlestick data"""
    __tablename__ = "ohlcv"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)
    symbol_id = Column(UUID(as_uuid=True), ForeignKey("symbols.id"), nullable=False)
    
    # OHLCV data
    timeframe = Column(SQLEnum(TimeFrame), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    open_price = Column(Numeric(20, 8), nullable=False)
    high_price = Column(Numeric(20, 8), nullable=False)
    low_price = Column(Numeric(20, 8), nullable=False)
    close_price = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=False)
    
    # Additional metrics
    quote_volume = Column(Numeric(20, 8))  # Volume in quote currency
    trades_count = Column(Integer)  # Number of trades in this period
    taker_buy_volume = Column(Numeric(20, 8))  # Volume bought by takers
    taker_buy_quote_volume = Column(Numeric(20, 8))
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    exchange = relationship("Exchange", back_populates="ohlcv_data")
    symbol = relationship("Symbol", back_populates="ohlcv_data")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint("exchange_id", "symbol_id", "timeframe", "timestamp", 
                        name="uq_ohlcv_exchange_symbol_timeframe_timestamp"),
        Index("idx_ohlcv_timestamp", "timestamp"),
        Index("idx_ohlcv_symbol_timeframe_timestamp", "symbol_id", "timeframe", "timestamp"),
    )


class Trade(Base):
    """Individual trade executions"""
    __tablename__ = "trades"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)
    symbol_id = Column(UUID(as_uuid=True), ForeignKey("symbols.id"), nullable=False)
    
    # Trade identification
    trade_id = Column(String(50))  # Exchange's trade ID
    order_id = Column(String(50))  # Exchange's order ID
    
    # Trade data
    price = Column(Numeric(20, 8), nullable=False)
    quantity = Column(Numeric(20, 8), nullable=False)
    quote_quantity = Column(Numeric(20, 8))  # price * quantity
    side = Column(SQLEnum(OrderSide), nullable=False)
    
    # Timing
    timestamp = Column(DateTime(timezone=True), nullable=False)
    
    # Fees
    fee_amount = Column(Numeric(20, 8))
    fee_currency = Column(String(10))
    
    # Metadata
    is_buyer_maker = Column(Boolean)  # True if buyer is market maker
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    exchange = relationship("Exchange", back_populates="trades")
    symbol = relationship("Symbol", back_populates="trades")
    
    # Constraints
    __table_args__ = (
        Index("idx_trade_timestamp", "timestamp"),
        Index("idx_trade_symbol_timestamp", "symbol_id", "timestamp"),
        Index("idx_trade_exchange_trade_id", "exchange_id", "trade_id"),
    )

