import pandas as pd
import duckdb
import os
import logging
import time
from .db_initializer import DBInitializer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryExecutor:
    def __init__(self, db_path='retail_data.db'):
        """Initialize the query executor with a connection to the database."""
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        self.db_path = os.path.join(self.data_dir, db_path)
        
        # Initialize database if it doesn't exist
        if not os.path.exists(self.db_path):
            logger.info("Database does not exist, initializing...")
            initializer = DBInitializer(db_path)
            initializer.initialize_database()
        
        # Create or connect to the database
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"Connected to database at {self.db_path}")
        
        # Configure statement timeouts (DuckDB doesn't support direct query timeouts,
        # but we'll implement a timeout mechanism in execute_query)
        self.query_timeout_seconds = 10
        
        # Limit on returned rows
        self.max_rows = 1000
    
    def execute_query(self, query):
        """
        Execute an SQL query with a timeout and row limit.
        
        Args:
            query (str): The SQL query to execute
            
        Returns:
            pandas.DataFrame: The query results as a DataFrame
            
        Raises:
            Exception: If the query times out or other errors occur
        """
        try:
            # Check if the query already has a LIMIT clause
            has_limit = "LIMIT" in query.upper()
            
            # Add a LIMIT clause if not present
            if not has_limit:
                # Удаляем точку с запятой в конце запроса, если она есть
                if query.strip().endswith(';'):
                    query = query.strip()[:-1]
                
                # Добавляем LIMIT в конец запроса
                query = f"{query} LIMIT {self.max_rows}"
            
            logger.info(f"Executing query: {query}")
            
            # Start timer
            start_time = time.time()
            
            # Execute the query with a timeout
            result = self._execute_with_timeout(query)
            
            # Calculate query execution time
            execution_time = time.time() - start_time
            logger.info(f"Query executed in {execution_time:.2f} seconds")
            
            # Convert result to DataFrame
            if result is not None:
                df = result.fetchdf()
                logger.info(f"Query returned {len(df)} rows")
                return df
            return None
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise Exception(f"Ошибка выполнения запроса: {str(e)}")
    
    def _execute_with_timeout(self, query):
        """Execute a query with a timeout."""
        # DuckDB doesn't support query timeouts directly, so we'll implement
        # a simple timeout mechanism with a separate monitoring thread
        
        import threading
        import queue
        
        result_queue = queue.Queue()
        error_queue = queue.Queue()
        
        def execute_query_thread():
            try:
                result = self.conn.execute(query)
                result_queue.put(result)
            except Exception as e:
                error_queue.put(e)
        
        # Start the query execution in a separate thread
        query_thread = threading.Thread(target=execute_query_thread)
        query_thread.daemon = True  # Daemon threads are killed when the main thread exits
        query_thread.start()
        
        # Wait for the query to complete or timeout
        query_thread.join(timeout=self.query_timeout_seconds)
        
        # Check if the query is still running (thread is still alive)
        if query_thread.is_alive():
            # The query is still running after the timeout
            raise Exception(f"Запрос превысил ограничение времени выполнения ({self.query_timeout_seconds} секунд)")
        
        # Check if there was an error
        if not error_queue.empty():
            raise error_queue.get()
        
        # Return the result if available
        if not result_queue.empty():
            return result_queue.get()
        
        return None 