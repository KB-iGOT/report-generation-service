import pytest
import unittest
from unittest.mock import patch, MagicMock
import json
import redis
from app.services.redis_service import RedisService
from constants import REDIS_KEY_MDO_PREFIX, REDIS_DEFAULT_TTL

class TestRedisService(unittest.TestCase):
    """
    Test class for RedisService
    """
    
    def setUp(self):
        """
        Set up test environment before each test
        """
        # Reset the singleton instance before each test
        RedisService._instance = None
        RedisService._client = None
        
        # Create a patch for the Redis client initialization
        self.redis_patcher = patch('app.services.redis_service.redis.Redis')
        self.mock_redis = self.redis_patcher.start()
        self.mock_redis_client = MagicMock()
        self.mock_redis.return_value = self.mock_redis_client
        
        # Create the service instance
        self.redis_service = RedisService()
    
    def tearDown(self):
        """
        Clean up after each test
        """
        self.redis_patcher.stop()
        
    def test_set_value_string_success(self):
        """
        Test setting a string value successfully
        """
        # Arrange
        key = "test_key"
        value = "test_value"
        ttl = 300
        self.mock_redis_client.set.return_value = True
        
        # Act
        result = self.redis_service.set_value(key, value, ttl)
        
        # Assert
        self.assertTrue(result)
        self.mock_redis_client.set.assert_called_once_with(
            f"{REDIS_KEY_MDO_PREFIX}{key}", value, ex=ttl
        )
        
    def test_set_value_list_success(self):
        """
        Test setting a list value successfully
        """
        # Arrange
        key = "test_key"
        value = ["item1", "item2", "item3"]
        ttl = 300
        self.mock_redis_client.set.return_value = True
        
        # Act
        result = self.redis_service.set_value(key, value, ttl)
        
        # Assert
        self.assertTrue(result)
        self.mock_redis_client.set.assert_called_once_with(
            f"{REDIS_KEY_MDO_PREFIX}{key}", json.dumps(value), ex=ttl
        )
        
    def test_set_value_default_ttl(self):
        """
        Test setting a value with default TTL
        """
        # Arrange
        key = "test_key"
        value = "test_value"
        self.mock_redis_client.set.return_value = True
        
        # Act
        result = self.redis_service.set_value(key, value)
        
        # Assert
        self.assertTrue(result)
        self.mock_redis_client.set.assert_called_once_with(
            f"{REDIS_KEY_MDO_PREFIX}{key}", value, ex=REDIS_DEFAULT_TTL
        )
        
    def test_set_value_failure(self):
        """
        Test setting a value with Redis error
        """
        # Arrange
        key = "test_key"
        value = "test_value"
        self.mock_redis_client.set.side_effect = Exception("Redis error")
        
        # Act
        result = self.redis_service.set_value(key, value)
        
        # Assert
        self.assertFalse(result)
        
    def test_get_value_string_success(self):
        """
        Test getting a string value successfully
        """
        # Arrange
        key = "test_key"
        expected_value = "test_value"
        self.mock_redis_client.get.return_value = expected_value
        
        # Act
        result = self.redis_service.get_value(key)
        
        # Assert
        self.assertEqual(result, expected_value)
        self.mock_redis_client.get.assert_called_once_with(f"{REDIS_KEY_MDO_PREFIX}{key}")
        
    def test_get_value_list_success(self):
        """
        Test getting a list value successfully
        """
        # Arrange
        key = "test_key"
        expected_list = ["item1", "item2", "item3"]
        self.mock_redis_client.get.return_value = json.dumps(expected_list)
        
        # Act
        result = self.redis_service.get_value(key)
        
        # Assert
        self.assertEqual(result, expected_list)
        self.mock_redis_client.get.assert_called_once_with(f"{REDIS_KEY_MDO_PREFIX}{key}")
        
    def test_get_value_not_found(self):
        """
        Test getting a value that doesn't exist
        """
        # Arrange
        key = "nonexistent_key"
        self.mock_redis_client.get.return_value = None
        
        # Act
        result = self.redis_service.get_value(key)
        
        # Assert
        self.assertIsNone(result)
        self.mock_redis_client.get.assert_called_once_with(f"{REDIS_KEY_MDO_PREFIX}{key}")
        
    def test_get_value_failure(self):
        """
        Test getting a value with Redis error
        """
        # Arrange
        key = "test_key"
        self.mock_redis_client.get.side_effect = Exception("Redis error")
        
        # Act
        result = self.redis_service.get_value(key)
        
        # Assert
        self.assertIsNone(result)
        
    def test_singleton_pattern(self):
        """
        Test that RedisService implements the singleton pattern
        """
        # Reset the singleton instance
        RedisService._instance = None
        RedisService._client = None
        
        # Create a new mock for Redis
        with patch('app.services.redis_service.redis.Redis') as mock_redis:
            mock_redis_client = MagicMock()
            mock_redis.return_value = mock_redis_client
            
            # Create two instances
            instance1 = RedisService()
            instance2 = RedisService()
            
            # Assert they are the same instance
            self.assertIs(instance1, instance2)
            
            # Redis should be initialized only once
            mock_redis.assert_called_once()
            
    def test_client_property(self):
        """
        Test the client property getter and setter
        """
        # Create a new mock client
        new_client = MagicMock()
        
        # Set the client
        self.redis_service.client = new_client
        
        # Get the client and verify it's the same
        self.assertIs(self.redis_service.client, new_client)
        
    def test_initialize_client_error(self):
        """
        Test client initialization with connection error
        """
        # Reset the singleton instance
        RedisService._instance = None
        RedisService._client = None
        
        # Create a new mock for Redis that raises ConnectionError
        with patch('app.services.redis_service.redis.Redis') as mock_redis:
            mock_redis.side_effect = redis.ConnectionError("Connection refused")
            
            # Create an instance
            instance = RedisService()
            
            # Client should be None
            self.assertIsNone(instance.client)