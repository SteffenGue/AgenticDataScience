
"""
Database Handler for Cryptocurrency Data Management
Provides a centralized class for managing database connections and sessions
"""

import logging
from contextlib import contextmanager
from typing import Optional, Generator

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists, create_database

from data_model import Base

logger = logging.getLogger(__name__)


class DatabaseHandler:
    """
    Centralized database handler for managing connections and sessions.
    
    This class provides:
    - Database engine management
    - Session lifecycle management via context managers
    - Table creation and management
    - Connection pooling configuration
    """
    
    def __init__(
        self, 
        database_url: str = "postgresql://user:password@db:5432/crypto",
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_recycle: int = 3600,
        echo: bool = False
    ):
        """
        Initialize the database handler.
        
        Args:
            database_url: SQLAlchemy database URL
            pool_size: Number of connections to maintain in the pool
            max_overflow: Maximum number of connections to create beyond pool_size
            pool_recycle: Seconds after which a connection is recycled
            echo: Whether to echo SQL statements (for debugging)
        """
        self.database_url = database_url
        self.engine: Optional[Engine] = None
        self.session_maker: Optional[sessionmaker] = None
        
        # Connection pool configuration
        self.pool_config = {
            'pool_size': pool_size,
            'max_overflow': max_overflow,
            'pool_pre_ping': True,
            'pool_recycle': pool_recycle,
            'echo': echo
        }
        
        # Initialize the engine and session maker
        self._create_database()
        self._create_engine()
        self._create_session_maker()


    def _create_database(self) -> None:
        """
        Check if database exists and create it if it doesn't using sqlalchemy_utils.
        """
        try:
            if not database_exists(self.database_url):
                logger.info(f"Database does not exist, creating: {self.database_url}")
                create_database(self.database_url)
                logger.info("Database created successfully")
            else:
                logger.info("Database already exists")
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise
    
    
    def _create_engine(self) -> None:
        """Create SQLAlchemy engine with optimized settings."""
        try:
            self.engine = create_engine(
                self.database_url,
                **self.pool_config
            )
            logger.info("Database engine created successfully")
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    def _create_session_maker(self) -> None:
        """Create SQLAlchemy session maker."""
        if not self.engine:
            raise RuntimeError("Engine must be created before session maker")
        
        self.session_maker = sessionmaker(bind=self.engine)
        logger.info("Session maker created successfully")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        
        Provides automatic session lifecycle management:
        - Creates a new session
        - Commits on success
        - Rolls back on exception
        - Always closes the session
        
        Usage:
            with db_handler.get_session() as session:
                # Your database operations here
                result = session.query(Model).all()
        
        Yields:
            Session: SQLAlchemy session object
        """
        if not self.session_maker:
            raise RuntimeError("Session maker not initialized")
        
        session = self.session_maker()
        try:
            logger.debug("Database session started")
            yield session
            session.commit()
            logger.debug("Database session committed")
        except Exception as e:
            logger.error(f"Database session error, rolling back: {e}")
            session.rollback()
            raise
        finally:
            session.close()
            logger.debug("Database session closed")
    
    def create_all_tables(self) -> None:
        """Create all tables defined in the data model."""
        if not self.engine:
            raise RuntimeError("Engine not initialized")
                
        try:
            Base.metadata.create_all(self.engine)
            logger.info("All database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def drop_all_tables(self) -> None:
        """Drop all tables (use with caution!)."""
        if not self.engine:
            raise RuntimeError("Engine not initialized")
        
        try:
            Base.metadata.drop_all(self.engine)
            logger.warning("All database tables dropped")
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop tables: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.get_session() as session:
                # Simple query to test connection
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_engine(self) -> Engine:
        """
        Get the SQLAlchemy engine.
        
        Returns:
            Engine: SQLAlchemy engine instance
        """
        if not self.engine:
            raise RuntimeError("Engine not initialized")
        return self.engine
    
    def close(self) -> None:
        """Close all database connections."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
