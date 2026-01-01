"""
CCXT and Binance Data Providers for Cryptocurrency Data Collection
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional

import ccxt
from sqlalchemy.orm import Session

from data_model import (
    OHLCV,
    Exchange,
    ExchangeType,
    Symbol,
    TimeFrame,
)
from database_handler import DatabaseHandler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CryptoDataProvider(DatabaseHandler):
    """Base class for cryptocurrency data providers"""
    
    def __init__(self, database_url: str):
        super().__init__(database_url)
        
    def get_or_create_exchange(self, session: Session, name: str, exchange_type: ExchangeType) -> Exchange:
        """Get existing exchange or create new one"""
        exchange = session.query(Exchange).filter_by(name=name).first()
        if not exchange:
            exchange = Exchange(
                name=name,
                exchange_type=exchange_type,
                timezone_name="UTC"
            )
            session.add(exchange)
            session.flush()
        return exchange
    
    def get_or_create_symbol(self, session: Session, exchange_id: str, symbol: str, 
                           base: str, quote: str, symbol_info: dict = None) -> Optional[Symbol]:
        """Get existing symbol or create new one"""
        # Check if currency names are too long for database schema
        if len(base) > 10 or len(quote) > 10:
            logger.warning(f"Skipping symbol {symbol}: currency names too long (base: {base}, quote: {quote})")
            return None
            
        db_symbol = session.query(Symbol).filter_by(
            exchange_id=exchange_id, 
            symbol=symbol
        ).first()
        
        if not db_symbol:
            db_symbol = Symbol(
                exchange_id=exchange_id,
                symbol=symbol,
                base_currency=base,
                quote_currency=quote,
                is_active=True
            )
            
            # Add symbol info if available
            if symbol_info:
                db_symbol.min_order_size = symbol_info.get('limits', {}).get('amount', {}).get('min')
                db_symbol.max_order_size = symbol_info.get('limits', {}).get('amount', {}).get('max')
                db_symbol.min_price = symbol_info.get('limits', {}).get('price', {}).get('min')
                db_symbol.max_price = symbol_info.get('limits', {}).get('price', {}).get('max')
                db_symbol.price_precision = symbol_info.get('precision', {}).get('price')
                db_symbol.quantity_precision = symbol_info.get('precision', {}).get('amount')
                
            session.add(db_symbol)
            session.flush()
        return db_symbol


class CCXTDataProvider(CryptoDataProvider):
    """CCXT-based data provider for multiple exchanges"""
    
    def __init__(self, database_url: str, exchange_name: str = 'binance', 
                 sandbox: bool = True):
        super().__init__(database_url)
        
        # Initialize CCXT exchange
        exchange_class = getattr(ccxt, exchange_name)
        self.exchange = exchange_class({
            'sandbox': sandbox,  # Use testnet/sandbox for testing
            'enableRateLimit': True,
        })
        
        self.exchange_name = exchange_name
        self._exchange_type = self._get_exchange_type(exchange_name)
        
    def _get_exchange_type(self, name: str) -> ExchangeType:
        """Map exchange name to ExchangeType enum"""
        mapping = {
            'binance': ExchangeType.BINANCE,
            'coinbase': ExchangeType.COINBASE,
            'kraken': ExchangeType.KRAKEN,
            'kucoin': ExchangeType.KUCOIN,
            'bybit': ExchangeType.BYBIT,
        }
        return mapping.get(name, ExchangeType.OTHER)
    
    def fetch_and_store_markets(self) -> int:
        """Fetch market information and store symbols"""
        with self.get_session() as session:
            # Get or create exchange
            exchange = self.get_or_create_exchange(
                session, self.exchange_name, self._exchange_type
            )
            
            # Fetch markets
            markets = self.exchange.fetch_markets()
            symbols_created = 0
            
            for market in markets:
                if market['active']:  # Only active markets
                    symbol_obj = self.get_or_create_symbol(
                        session=session,
                        exchange_id=exchange.id,
                        symbol=market['symbol'],
                        base=market['base'],
                        quote=market['quote'],
                        symbol_info=market
                    )
                    # Only count if symbol was successfully created/retrieved
                    if symbol_obj:
                        symbols_created += 1
            
            logger.info(f"Processed {symbols_created} symbols for {self.exchange_name}")
            return symbols_created
        

    
    def subtract_interval_from_timestamp(self, timestamp: int, seconds=0, minutes=0, hours=0, days=0) -> int:
        """
        Subtracts a time interval from a given Unix timestamp.

        Parameters:
        - timestamp (int): The original Unix timestamp (in seconds).
        - seconds (int): Seconds to subtract.
        - minutes (int): Minutes to subtract.
        - hours (int): Hours to subtract.
        - days (int): Days to subtract.

        Returns:
        - int: The new Unix timestamp after subtraction.
        """
        total_seconds = seconds + minutes * 60 + hours * 3600 + days * 86400
        return timestamp - total_seconds

    
    def continuous_backfill_ohlcv(self, symbol: str, timeframe: str = '1h', 
                                 start_date: str = None, end_date: str = None, 
                                 limit_per_request: int = 1000) -> tuple[int, datetime, datetime]:
        """Continuously backfill OHLCV data from start_date until end_date or present.
        
        This method uses the efficient approach of fetching data batch by batch,
        automatically moving to the next timestamp until all data is retrieved.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            timeframe: Timeframe ('1m', '5m', '1h', '1d', etc.)
            start_date: Start date in ISO format (e.g., '2020-01-01T00:00:00Z')
            end_date: End date in ISO format (optional, defaults to now)
            limit_per_request: Maximum candles per API request
            
        Returns:
            tuple: (total_records_processed, actual_start_date, actual_end_date)
        """
        import time
        
        # Parse dates
        if start_date:
            since = self.exchange.parse8601(start_date)
        else:
            # Default to 30 days ago
            since = self.exchange.parse8601((datetime.now(timezone.utc) - timedelta(days=30)).isoformat())
            
        end_timestamp = None
        if end_date:
            end_timestamp = self.exchange.parse8601(end_date)
        
        with self.get_session() as session:
            try:
                # Get symbol info
                db_symbol = self._get_db_symbol(session, symbol)
                if not db_symbol:
                    raise ValueError(f"Symbol {symbol} not found in database")
                
                logger.info(f"Starting continuous backfill for {symbol} ({timeframe})")
                logger.info(f"From: {self.exchange.iso8601(since)} | Limit per request: {limit_per_request}")
                
                total_records = 0
                all_timestamps = []
                batch_count = 0
                
                while True:
                    try:
                        # Fetch OHLCV batch
                        candles = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit_per_request)
                        
                        if not candles:
                            logger.info("No more candles available")
                            break
                            
                        # Check if we've reached the end date
                        if end_timestamp and candles[0][0] >= end_timestamp:
                            logger.info(f"Reached end date: {self.exchange.iso8601(end_timestamp)}")
                            break
                        
                        batch_count += 1
                        batch_records = 0
                        
                        # Process and store candles
                        for ohlcv in candles:
                            timestamp, open_price, high, low, close, volume = ohlcv
                            
                            # Skip if beyond end date
                            if end_timestamp and timestamp >= end_timestamp:
                                break
                                
                            # Check if record already exists
                            record_datetime = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
                            existing = session.query(OHLCV).filter_by(
                                symbol_id=db_symbol.id,
                                timeframe=TimeFrame(timeframe),
                                timestamp=record_datetime
                            ).first()
                            
                            if not existing:
                                ohlcv_record = OHLCV(
                                    exchange_id=db_symbol.exchange_id,
                                    symbol_id=db_symbol.id,
                                    timeframe=TimeFrame(timeframe),
                                    timestamp=record_datetime,
                                    open_price=Decimal(str(open_price)),
                                    high_price=Decimal(str(high)),
                                    low_price=Decimal(str(low)),
                                    close_price=Decimal(str(close)),
                                    volume=Decimal(str(volume))
                                )
                                session.add(ohlcv_record)
                                batch_records += 1
                            
                            all_timestamps.append(timestamp)
                        
                        total_records += batch_records
                        
                        # Log progress
                        last_timestamp = candles[-1][0]
                        logger.info(f"Batch {batch_count}: {len(candles)} candles, {batch_records} new records. "
                                  f"Latest: {self.exchange.iso8601(last_timestamp)}")
                        
                        # Move to next batch (add 1ms to avoid overlap)
                        since = last_timestamp + 1
                        
                        # Commit batch to avoid large transactions
                        if batch_count % 10 == 0:  # Commit every 10 batches
                            session.commit()
                            logger.info(f"Committed batch {batch_count}, total records so far: {total_records}")
                        
                        # Respect rate limits
                        if self.exchange.rateLimit:
                            time.sleep(self.exchange.rateLimit / 1000)
                            
                    except Exception as batch_error:
                        logger.error(f"Error in batch {batch_count}: {batch_error}")
                        # Skip problematic timestamp and continue
                        since += 86400000  # Skip 1 day ahead
                        continue
                
                # Final commit
                session.commit()
                
                # Calculate actual start and end dates
                if all_timestamps:
                    actual_start = datetime.fromtimestamp(min(all_timestamps) / 1000, timezone.utc)
                    actual_end = datetime.fromtimestamp(max(all_timestamps) / 1000, timezone.utc)
                else:
                    actual_start = actual_end = datetime.now(timezone.utc)
                
                logger.info(f"  Continuous backfill completed for {symbol}: {total_records} records")
                logger.info(f"  Date range: {actual_start.date()} to {actual_end.date()}")
                logger.info(f"  Batches processed: {batch_count}")
                
                return total_records, actual_start, actual_end
                
            except Exception as e:
                logger.error(f"Continuous backfill failed for {symbol}: {e}")
                raise

    def interval_mapping(self, interval: str):
        
        _ = {
            "1s": "seconds",
            "1m": "minutes",
            "1h": "hours",
            "1d": "days",
        }

        return _.get(interval)
    
    def fetch_and_store_ohlcv(self, 
                              symbol: str,
                              timeframe: str, 
                              since_timestamp: str = '2020-01-01T00:00:00Z', 
                              limit: int = 1000) -> int:
        """Fetch OHLCV data and store in database"""
        # Set default since to today if not provided

        if isinstance(since_timestamp, str):
            since_timestamp = self.exchange.parse8601(since_timestamp)
            
        with self.get_session() as session:            
            try:
                # Get symbol info
                db_symbol = self._get_db_symbol(session, symbol)
                if not db_symbol:
                    raise ValueError(f"Symbol {symbol} not found in database")
                
                # Fetch OHLCV data
                ohlcv_data = self.exchange.fetch_ohlcv(
                    symbol, timeframe, since_timestamp, limit
                )

                if not ohlcv_data:
                    return 0, None
                
                new_timestamp = ohlcv_data[-1][0] + 1
                
                # min_timestamp = min([item[0] for item in ohlcv_data])
                # interval = self.interval_mapping(timeframe)
                # new_timestamp = self.subtract_interval_from_timestamp(timestamp=min_timestamp, **{interval: limit})
                
                # Store data
                records_processed = 0
                for ohlcv in ohlcv_data:
                    timestamp, open_price, high, low, close, volume = ohlcv
                    
                    # Check if record already exists
                    existing = session.query(OHLCV).filter_by(
                        symbol_id=db_symbol.id,
                        timeframe=TimeFrame(timeframe),
                        timestamp=datetime.fromtimestamp(timestamp / 1000, timezone.utc)
                    ).first()
                    
                    if not existing:
                        ohlcv_record = OHLCV(
                            exchange_id=db_symbol.exchange_id,
                            symbol_id=db_symbol.id,
                            timeframe=TimeFrame(timeframe),
                            timestamp=datetime.fromtimestamp(timestamp / 1000, timezone.utc),
                            open_price=Decimal(str(open_price)),
                            high_price=Decimal(str(high)),
                            low_price=Decimal(str(low)),
                            close_price=Decimal(str(close)),
                            volume=Decimal(str(volume))
                        )
                        session.add(ohlcv_record)
                        records_processed += 1
                
                logger.info(f"  Stored {records_processed} OHLCV records for {symbol}")
                return records_processed, new_timestamp
                
            except Exception as e:
                logger.error(f"  Failed to fetch OHLCV for {symbol}: {e}")
                raise
    
    def fetch_and_store_ohlcv_with_timestamps(self, symbol: str, timeframe: str = '1m', 
                                            limit: int = 100, since: datetime = None) -> tuple[int, datetime, datetime]:
        """Fetch OHLCV data and store in database, returning timestamp info"""
        with self.get_session() as session:
            # Create ingestion log
            log = DataIngestionLog(
                exchange_id=self._get_exchange_id(session),
                data_type='ohlcv',
                timeframe=TimeFrame(timeframe),
                status='started'
            )
            session.add(log)
            session.flush()
            
            try:
                # Get symbol info
                db_symbol = self._get_db_symbol(session, symbol)
                if not db_symbol:
                    raise ValueError(f"Symbol {symbol} not found in database")
                
                # Fetch OHLCV data
                since_timestamp = int(since.timestamp() * 1000) if since else None
                ohlcv_data = self.exchange.fetch_ohlcv(
                    symbol, timeframe, since_timestamp, limit
                )
                
                if not ohlcv_data:
                    return 0, since, since
                
                # Store data
                records_processed = 0
                for ohlcv in ohlcv_data:
                    timestamp, open_price, high, low, close, volume = ohlcv
                    
                    # Check if record already exists
                    existing = session.query(OHLCV).filter_by(
                        symbol_id=db_symbol.id,
                        timeframe=TimeFrame(timeframe),
                        timestamp=datetime.fromtimestamp(timestamp / 1000, timezone.utc)
                    ).first()
                    
                    if not existing:
                        ohlcv_record = OHLCV(
                            exchange_id=db_symbol.exchange_id,
                            symbol_id=db_symbol.id,
                            timeframe=TimeFrame(timeframe),
                            timestamp=datetime.fromtimestamp(timestamp / 1000, timezone.utc),
                            open_price=Decimal(str(open_price)),
                            high_price=Decimal(str(high)),
                            low_price=Decimal(str(low)),
                            close_price=Decimal(str(close)),
                            volume=Decimal(str(volume))
                        )
                        session.add(ohlcv_record)
                        records_processed += 1
                
                # Get actual start and end timestamps from the data
                actual_start = datetime.fromtimestamp(ohlcv_data[0][0] / 1000, timezone.utc)
                actual_end = datetime.fromtimestamp(ohlcv_data[-1][0] / 1000, timezone.utc)
                
                # Update log
                log.status = 'completed'
                log.records_processed = records_processed
                log.end_time = datetime.now(timezone.utc)
                log.data_start_time = actual_start
                log.data_end_time = actual_end
                
                logger.info(f"Stored {records_processed} OHLCV records for {symbol} ({actual_start} to {actual_end})")
                return records_processed, actual_start, actual_end
                
            except Exception as e:
                log.status = 'failed'
                log.error_message = str(e)
                log.end_time = datetime.now(timezone.utc)
                session.commit()
                logger.error(f"Failed to fetch OHLCV for {symbol}: {e}")
                raise
    
    def fetch_and_store_ticker(self, symbol: str) -> bool:
        """Fetch ticker data and store in database"""
        with self.get_session() as session:
            try:
                # Get symbol info
                db_symbol = self._get_db_symbol(session, symbol)
                if not db_symbol:
                    raise ValueError(f"Symbol {symbol} not found in database")
                
                # Fetch ticker
                ticker_data = self.exchange.fetch_ticker(symbol)
                
                # Store ticker
                ticker = Ticker(
                    exchange_id=db_symbol.exchange_id,
                    symbol_id=db_symbol.id,
                    last_price=Decimal(str(ticker_data.get('last', 0))),
                    bid_price=Decimal(str(ticker_data.get('bid', 0))),
                    ask_price=Decimal(str(ticker_data.get('ask', 0))),
                    high_24h=Decimal(str(ticker_data.get('high', 0))),
                    low_24h=Decimal(str(ticker_data.get('low', 0))),
                    open_24h=Decimal(str(ticker_data.get('open', 0))),
                    close_24h=Decimal(str(ticker_data.get('close', 0))),
                    volume_24h=Decimal(str(ticker_data.get('baseVolume', 0))),
                    volume_quote_24h=Decimal(str(ticker_data.get('quoteVolume', 0))),
                    price_change_24h=Decimal(str(ticker_data.get('change', 0))),
                    price_change_percent_24h=Decimal(str(ticker_data.get('percentage', 0))),
                    timestamp=datetime.fromtimestamp(ticker_data['timestamp'] / 1000, timezone.utc)
                )
                
                session.add(ticker)
                session.commit()
                logger.info(f"Stored ticker data for {symbol}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to fetch ticker for {symbol}: {e}")
                return False
    
    def _get_exchange_id(self, session: Session) -> str:
        """Get exchange ID from database"""
        exchange = session.query(Exchange).filter_by(name=self.exchange_name).first()
        return exchange.id if exchange else None
    
    def _get_db_symbol(self, session: Session, symbol: str) -> Optional[Symbol]:
        """Get symbol from database"""
        exchange_id = self._get_exchange_id(session)
        return session.query(Symbol).filter_by(
            exchange_id=exchange_id, symbol=symbol
        ).first()


# Example usage
async def main():
    """Example usage of the data providers"""
    database_url = "postgresql://user:password@db:5432/crypto_db"
    
    # Initialize CCXT provider
    ccxt_provider = CCXTDataProvider(
        database_url=database_url,
        exchange_name='binance',
        sandbox=True  # Use testnet
    )
    
    # Fetch and store market data
    symbols_count = ccxt_provider.fetch_and_store_markets()
    print(f"Stored {symbols_count} symbols")
    
    # Fetch OHLCV data for BTC/USDT
    records = ccxt_provider.fetch_and_store_ohlcv('BTC/USDT', '1m', limit=100)
    print(f"Stored {records} OHLCV records")
    
    # Fetch ticker data
    success = ccxt_provider.fetch_and_store_ticker('BTC/USDT')
    print(f"Ticker stored: {success}")


if __name__ == "__main__":
    asyncio.run(main())