"""
Main script for cryptocurrency data collection and management.

This script provides functionality to:
1. Initialize the database and create all tables
2. Fetch all markets from an exchange
3. Fetch and backfill OHLCV data for specific cryptocurrencies
"""

import logging
import os
from database_handler import DatabaseHandler
from crypto_data_providers import CCXTDataProvider
import dotenv
import config

dotenv.load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoDataManager:
    """Main class for managing cryptocurrency data collection operations."""
    
    def __init__(
        self,
        database_url: str,
        exchange_name: str,
        sandbox: bool = False
    ):
        """
        Initialize the crypto data manager.
        
        Args:
            database_url: Database connection string
            exchange_name: Exchange to use (e.g., 'binance', 'coinbase', 'kraken')
            sandbox: Use sandbox/testnet environment
        """
        self.database_url = database_url
        self.exchange_name = exchange_name
        
        # Initialize database handler
        self.db_handler = DatabaseHandler(database_url)
        
        # Initialize data provider
        self.data_provider = CCXTDataProvider(
            database_url=database_url,
            exchange_name=exchange_name,
            sandbox=sandbox
        )
        
        logger.info(f"CryptoDataManager initialized for {exchange_name} exchange")
    
    def init_database(self, drop_existing: bool = False) -> bool:
        """
        Initialize the database and create all tables.
        
        Args:
            drop_existing: Whether to drop existing tables first (CAUTION!)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if drop_existing:
                logger.warning("Dropping existing tables...")
                self.db_handler.drop_all_tables()
            
            logger.info("Creating database tables...")
            self.db_handler.create_all_tables()
            
            # Test connection
            if self.db_handler.test_connection():
                logger.info("Database initialized successfully")
                return True
            else:
                logger.error("✗ Database initialization failed - connection test unsuccessful")
                return False
                
        except Exception as e:
            logger.error(f"✗ Database initialization failed: {e}")
            return False
    
    def fetch_all_markets(self) -> int:
        """
        Fetch all available markets from the exchange and store them in the database.
        
        Returns:
            int: Number of markets/symbols processed
        """
        try:
            logger.info(f"Fetching all markets from {self.exchange_name}...")
            symbols_count = self.data_provider.fetch_and_store_markets()
            logger.info(f"Successfully processed {symbols_count} markets")
            return symbols_count
            
        except Exception as e:
            logger.error(f"✗ Failed to fetch markets: {e}")
            return 0
    
    def backfill_ohlcv_data(
        self,
        symbol: str,
        timeframe: str = "1h",
        since = '2020-01-01T00:00:00Z',
    ) -> int:
        """
        Backfill OHLCV data for a specific cryptocurrency.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            timeframe: Timeframe for OHLCV data ('1m', '5m', '1h', '1d', etc.)
            days_back: Number of days to backfill
            limit_per_request: Maximum records per API request
            use_continuous_method: Use the efficient continuous backfill method
        
        Returns:
            int: Number of OHLCV records processed
    """       

        logger.info(f"Starting OHLCV backfill for {symbol} ({timeframe})")

        total_records = 0        
        # Backfill in chunks to avoid API limits
        while True:
            
            try:
                records, since = self.data_provider.fetch_and_store_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    since_timestamp=since
                )

                if records == 0:
                    logging.info("Requested completed. Total number of records: %d." % total_records)
                    break 

                total_records += records

            except Exception as e:
                print(e)
                break

        print(total_records)


    def get_market_summary(self) -> dict:
        """
        Get a summary of markets in the database.
        
        Returns:
            dict: Summary statistics
        """
        try:
            with self.db_handler.get_session() as session:
                from data_model import Exchange, Symbol, OHLCV
                
                # Count exchanges
                exchange_count = session.query(Exchange).count()
                
                # Count symbols
                symbol_count = session.query(Symbol).count()
                active_symbols = session.query(Symbol).filter(Symbol.is_active == True).count()
                
                # Count OHLCV records
                ohlcv_count = session.query(OHLCV).count()
                
                # Latest OHLCV timestamp
                latest_ohlcv = session.query(OHLCV).order_by(OHLCV.timestamp.desc()).first()
                latest_timestamp = latest_ohlcv.timestamp if latest_ohlcv else None
                
                summary = {
                    'exchanges': exchange_count,
                    'total_symbols': symbol_count,
                    'active_symbols': active_symbols,
                    'ohlcv_records': ohlcv_count,
                    'latest_data': latest_timestamp
                }
                
                return summary
                
        except Exception as e:
            logger.error(f"Failed to get market summary: {e}")
            return {}
    

if __name__ == "__main__":
    data_manager = CryptoDataManager(exchange_name=config.EXCHANGE_NAME, database_url=os.getenv("DATABASE_CONNECTION_STRING"))
    data_manager.init_database()
    data_manager.fetch_all_markets()

    for pair in config.PAIRS.split(","):

        data_manager.backfill_ohlcv_data(
            symbol=pair,
            timeframe="1d",
        )

