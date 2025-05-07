import dagster as dg
import psycopg2
import time
import logging
from psycopg2.extras import RealDictCursor, execute_batch

class PostgreSQLResource():
    """Resource for connecting to a PostgreSQL database with robust error handling."""
    
    def __init__(self, connection_string, max_retries=3, retry_delay=2, connect_timeout=15):
        """Initialize with connection string to PostgreSQL.
        
        Args:
            connection_string: Connection string for PostgreSQL
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay in seconds between retry attempts
            connect_timeout: Connection timeout in seconds
        """
        self.connection_string = connection_string
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connect_timeout = connect_timeout
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get a connection to the PostgreSQL database with retry logic."""
        retry_count = 0
        last_exception = None
        
        while retry_count < self.max_retries:
            try:
                # Explicitly set SSL parameters and timeout
                connection_params = {
                    "cursor_factory": RealDictCursor,
                    "connect_timeout": self.connect_timeout,
                    "sslmode": "require",  # Enforce SSL
                    "application_name": "dagster_pipeline"  # Helps identify connections in logs
                }
                
                conn = psycopg2.connect(self.connection_string, **connection_params)
                
                # Configure connection for better stability
                conn.set_session(autocommit=False)
                
                # Test the connection with a simple query
                with conn.cursor() as test_cursor:
                    test_cursor.execute("SELECT 1")
                
                return conn
                
            except psycopg2.OperationalError as e:
                retry_count += 1
                last_exception = e
                self.logger.warning(
                    f"Database connection attempt {retry_count}/{self.max_retries} failed: {str(e)}"
                )
                
                if retry_count < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"Failed to connect to database after {self.max_retries} attempts")
                    raise psycopg2.OperationalError(
                        f"Failed to connect to database after {self.max_retries} attempts: {str(last_exception)}"
                    ) from last_exception
    
    def execute_with_retry(self, operation_func, *args, **kwargs):
        """Execute a database operation with retry logic for transient errors."""
        retry_count = 0
        last_exception = None
        
        while retry_count < self.max_retries:
            try:
                connection = self.get_connection()
                try:
                    result = operation_func(connection, *args, **kwargs)
                    connection.commit()
                    return result
                except Exception as e:
                    connection.rollback()
                    raise
                finally:
                    connection.close()
                    
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                retry_count += 1
                last_exception = e
                self.logger.warning(
                    f"Database operation attempt {retry_count}/{self.max_retries} failed: {str(e)}"
                )
                
                if retry_count < self.max_retries:
                    time.sleep(self.retry_delay * retry_count)  # Progressive backoff
                else:
                    self.logger.error(f"Failed to execute database operation after {self.max_retries} attempts")
                    raise
    
    def execute_batch(self, cursor, query, data, page_size=1000):
        """Execute a batch of statements with the same SQL.
        
        Args:
            cursor: A psycopg2 cursor
            query: The SQL query to execute
            data: Sequence of sequences or dictionaries with query parameters
            page_size: Number of statements to execute at a time
            
        Returns:
            The number of rows affected (if applicable)
        """
        try:
            return execute_batch(cursor, query, data, page_size=page_size)
        except psycopg2.Error as e:
            cursor.connection.rollback()
            self.logger.error(f"Batch execution failed: {str(e)}")
            raise
    
    def execute_values(self, cursor, query, data, page_size=1000):
        """Insert multiple rows into a table.
        
        This is optimized for inserting large amounts of data.
        
        Args:
            cursor: A psycopg2 cursor
            query: The SQL query with the %s placeholder for values
            data: Sequence of sequences with values to insert
            page_size: Number of rows to insert at a time
            
        Returns:
            The number of rows affected
        """
        from psycopg2.extras import execute_values
        try:
            return execute_values(cursor, query, data, page_size=page_size)
        except psycopg2.Error as e:
            cursor.connection.rollback()
            self.logger.error(f"Execute values failed: {str(e)}")
            raise


@dg.resource(config_schema={
    "connection_string": str,
    "max_retries": dg.Field(int, default_value=3, is_required=False),
    "retry_delay": dg.Field(int, default_value=2, is_required=False),
    "connect_timeout": dg.Field(int, default_value=15, is_required=False)
})
def postgres_resource(init_context):
    """Factory for PostgreSQLResource with configurable retry settings."""
    connection_string = init_context.resource_config["connection_string"]
    max_retries = init_context.resource_config.get("max_retries", 3)
    retry_delay = init_context.resource_config.get("retry_delay", 2)
    connect_timeout = init_context.resource_config.get("connect_timeout", 15)
    
    return PostgreSQLResource(
        connection_string, 
        max_retries=max_retries,
        retry_delay=retry_delay,
        connect_timeout=connect_timeout
    )