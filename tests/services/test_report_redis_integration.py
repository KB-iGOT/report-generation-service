import pytest
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.services.report_service import ReportService
from app.services.redis_service import RedisService

class TestReportRedisIntegration(unittest.TestCase):
    """
    Test class for Redis integration with ReportService
    """
    
    def setUp(self):
        """
        Set up test environment before each test
        """
        # Reset the Redis singleton instance
        RedisService._instance = None
        RedisService._client = None
        
        # Create a patch for the Redis client
        self.redis_patcher = patch('app.services.redis_service.redis.Redis')
        self.mock_redis = self.redis_patcher.start()
        self.mock_redis_client = MagicMock()
        self.mock_redis.return_value = self.mock_redis_client
        
        # Create a patch for BigQueryService
        self.bigquery_patcher = patch('app.services.report_service.BigQueryService')
        self.mock_bigquery = self.bigquery_patcher.start()
        self.mock_bigquery_instance = MagicMock()
        self.mock_bigquery.return_value = self.mock_bigquery_instance
        
    def tearDown(self):
        """
        Clean up after each test
        """
        self.redis_patcher.stop()
        self.bigquery_patcher.stop()
        
    def test_get_mdo_id_org_list_cache_hit(self):
        """
        Test _get_mdo_id_org_list with Redis cache hit
        """
        # Arrange
        mdo_id = "test_mdo_id"
        org_list = ["org1", "org2", "org3"]
        
        # Set up Redis mock to return the cached value
        self.mock_redis_client.get.return_value = '["org1", "org2", "org3"]'
        
        # Act
        result = ReportService._get_mdo_id_org_list(self.mock_bigquery_instance, mdo_id)
        
        # Assert
        self.assertEqual(result, org_list)
        self.mock_redis_client.get.assert_called_once()
        # BigQuery should not be called on cache hit
        self.mock_bigquery_instance.run_query.assert_not_called()
        
    def test_get_mdo_id_org_list_cache_miss(self):
        """
        Test _get_mdo_id_org_list with Redis cache miss
        """
        # Arrange
        mdo_id = "test_mdo_id"
        org_list = ["org1", "org2", "org3"]
        
        # Set up Redis mock to return None (cache miss)
        self.mock_redis_client.get.return_value = None
        
        # Set up BigQuery mock to return data
        df = pd.DataFrame({"organisation_id": org_list})
        self.mock_bigquery_instance.run_query.return_value = df
        
        # Act
        result = ReportService._get_mdo_id_org_list(self.mock_bigquery_instance, mdo_id)
        
        # Assert
        self.assertEqual(result, org_list)
        self.mock_redis_client.get.assert_called_once()
        self.mock_bigquery_instance.run_query.assert_called_once()
        self.mock_redis_client.set.assert_called_once()
        
    def test_get_mdo_id_org_list_empty_result(self):
        """
        Test _get_mdo_id_org_list with empty result from BigQuery
        """
        # Arrange
        mdo_id = "test_mdo_id"
        
        # Set up Redis mock to return None (cache miss)
        self.mock_redis_client.get.return_value = None
        
        # Set up BigQuery mock to return empty data
        df = pd.DataFrame({"organisation_id": []})
        self.mock_bigquery_instance.run_query.return_value = df
        
        # Act
        result = ReportService._get_mdo_id_org_list(self.mock_bigquery_instance, mdo_id)
        
        # Assert
        self.assertEqual(result, [])
        self.mock_redis_client.get.assert_called_once()
        self.mock_bigquery_instance.run_query.assert_called_once()
        self.mock_redis_client.set.assert_called_once()
        
    def test_isValidOrg_with_cache(self):
        """
        Test isValidOrg method using Redis cache
        """
        # Arrange
        x_org_id = "parent_org"
        request_org_id = "child_org"
        org_list = ["child_org", "other_org"]
        
        # Set up Redis mock to return the cached value
        self.mock_redis_client.get.return_value = '["child_org", "other_org"]'
        
        # Act
        result = ReportService.isValidOrg(x_org_id, request_org_id)
        
        # Assert
        self.assertTrue(result)
        self.mock_redis_client.get.assert_called_once()
        # BigQuery should not be called on cache hit
        self.mock_bigquery_instance.run_query.assert_not_called()
        
    def test_isValidOrg_invalid_org(self):
        """
        Test isValidOrg method with invalid organization
        """
        # Arrange
        x_org_id = "parent_org"
        request_org_id = "invalid_org"
        org_list = ["child_org", "other_org"]
        
        # Set up Redis mock to return the cached value
        self.mock_redis_client.get.return_value = '["child_org", "other_org"]'
        
        # Act
        result = ReportService.isValidOrg(x_org_id, request_org_id)
        
        # Assert
        self.assertFalse(result)
        self.mock_redis_client.get.assert_called_once()
        # BigQuery should not be called on cache hit
        self.mock_bigquery_instance.run_query.assert_not_called()
        
    def test_isValidOrg_empty_request_org_id(self):
        """
        Test isValidOrg method with empty request_org_id
        """
        # Arrange
        x_org_id = "parent_org"
        request_org_id = ""
        
        # Act
        result = ReportService.isValidOrg(x_org_id, request_org_id)
        
        # Assert
        self.assertFalse(result)
        # Redis should not be called with empty request_org_id
        self.mock_redis_client.get.assert_not_called()
        
    def test_isValidOrg_empty_x_org_id(self):
        """
        Test isValidOrg method with empty x_org_id
        """
        # Arrange
        x_org_id = ""
        request_org_id = "child_org"
        
        # Act
        result = ReportService.isValidOrg(x_org_id, request_org_id)
        
        # Assert
        self.assertFalse(result)
        # Redis should not be called with empty x_org_id
        self.mock_redis_client.get.assert_not_called()