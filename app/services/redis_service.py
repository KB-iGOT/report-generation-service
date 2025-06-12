import redis
import logging
import json
from constants import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_SSL, 
    REDIS_TIMEOUT, REDIS_KEY_MDO_PREFIX, REDIS_DEFAULT_TTL
)

class RedisService:
    """
    Service class for Redis operations with methods for setting and getting key-value pairs.
    """
    
    _instance = None
    _client = None
    logger = logging.getLogger(__name__)
    
    def __new__(cls):
        """
        Singleton pattern implementation to ensure only one Redis connection is created.
        """
        if cls._instance is None:
            cls._instance = super(RedisService, cls).__new__(cls)
            cls._instance._initialize_client()
        return cls._instance
    
    def _initialize_client(self):
        """Initialize the Redis client connection."""
        try:
            self._client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                ssl=REDIS_SSL,
                socket_timeout=REDIS_TIMEOUT,
                decode_responses=True
            )
            # Test connection
            self._client.ping()
            self.logger.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            self._client = None
    
    @property
    def client(self):
        """Property to access the Redis client."""
        return self._client
    
    @client.setter
    def client(self, value):
        """Setter for the Redis client (used in tests)."""
        self._client = value
    
    def set_value(self, key, value, ttl=REDIS_DEFAULT_TTL):
        """
        Set a key-value pair in Redis with optional TTL.
        
        Args:
            key (str): The key to set
            value (str or list/tuple): The value to store
            ttl (int, optional): Time to live in seconds. Defaults to REDIS_DEFAULT_TTL.
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.client is None:
                self.logger.error("Redis client is not initialized")
                return False
                
            prefixed_key = f"{REDIS_KEY_MDO_PREFIX}{key}"
            
            # Handle list/tuple values by converting to JSON
            if isinstance(value, (list, tuple)):
                value = json.dumps(list(value))
                
            result = self.client.set(prefixed_key, value, ex=ttl)
            self.logger.debug(f"Set key '{prefixed_key}' with TTL {ttl}s: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error setting Redis key '{key}': {e}")
            return False
    
    def get_value(self, key):
        """
        Get a value from Redis by key.
        
        Args:
            key (str): The key to retrieve
            
        Returns:
            str or list: The value if found, None otherwise
        """
        try:
            if self.client is None:
                self.logger.error("Redis client is not initialized")
                return None
                
            prefixed_key = f"{REDIS_KEY_MDO_PREFIX}{key}"
            value = self.client.get(prefixed_key)
            
            if value is None:
                self.logger.debug(f"Key '{prefixed_key}' not found in Redis")
                return None
                
            # Try to parse as JSON (for list/tuple values)
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                # Return as is if not JSON
                return value
                
        except Exception as e:
            self.logger.error(f"Error getting Redis key '{key}': {e}")
            return None