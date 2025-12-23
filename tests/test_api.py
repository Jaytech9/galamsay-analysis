"""
Test Suite for Flask REST API

This module contains tests for all API endpoints,
including success cases and error handling.

Author: The Ghost Packet
Date: December 2025
"""

import unittest
import json
import os
import tempfile
import csv
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app
from database import init_database


class TestAPIBase(unittest.TestCase):
    """Base class for API tests with common setup."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.temp_dir, 'test.db')
        cls.data_file = os.path.join(cls.temp_dir, 'test_data.csv')
        
        # Create test CSV file
        with open(cls.data_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['City', 'Region', 'Number_of_Galamsay_Sites'])
            writer.writerow(['Kumasi', 'Ashanti', '25'])
            writer.writerow(['Obuasi', 'Ashanti', '15'])
            writer.writerow(['Accra', 'Greater Accra', '30'])
            writer.writerow(['Tamale', 'Northern', '7'])
            writer.writerow(['Cape Coast', 'Central', '14'])
        
        # Configure app for testing
        app.config['TESTING'] = True
        
        # Set environment variables for test paths
        os.environ['GALAMSAY_DATA_FILE'] = cls.data_file
        os.environ['GALAMSAY_DB_PATH'] = cls.db_path
        
        # Reload app module to pick up new env vars
        import importlib
        import app as app_module
        importlib.reload(app_module)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(cls.temp_dir)
    
    def setUp(self):
        """Set up test client."""
        self.client = app.test_client()


class TestHealthEndpoint(TestAPIBase):
    """Tests for health check endpoint."""
    
    def test_health_check(self):
        """Test health check returns healthy status."""
        response = self.client.get('/api/health')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['status'], 'healthy')


class TestRootEndpoint(TestAPIBase):
    """Tests for API root endpoint."""
    
    def test_root_returns_api_info(self):
        """Test root endpoint returns API documentation."""
        response = self.client.get('/')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('name', data)
        self.assertIn('endpoints', data)
        self.assertEqual(data['name'], 'Galamsay Analysis API')


class TestAnalyzeEndpoint(TestAPIBase):
    """Tests for analysis endpoint."""
    
    def test_run_analysis(self):
        """Test running new analysis."""
        response = self.client.post('/api/analyze')
        
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertIn('batch_id', data)
        self.assertIn('summary', data)
        self.assertEqual(data['summary']['total_sites'], 91)  # 25+15+30+7+14
    
    def test_run_analysis_with_threshold(self):
        """Test analysis with custom threshold."""
        response = self.client.post('/api/analyze?threshold=20')
        
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        # Only cities with > 20 sites: Kumasi (25), Accra (30)
        self.assertEqual(data['summary']['cities_above_threshold']['count'], 2)
    
    def test_run_analysis_negative_threshold(self):
        """Test analysis with invalid negative threshold."""
        response = self.client.post('/api/analyze?threshold=-5')
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn('error', data)


class TestAnalysisRetrievalEndpoints(TestAPIBase):
    """Tests for analysis retrieval endpoints."""
    
    @classmethod
    def setUpClass(cls):
        """Run analysis before tests."""
        super().setUpClass()
        
        # Initialize database and run analysis
        init_database(cls.db_path)
        
        # Create client and run analysis
        with app.test_client() as client:
            response = client.post('/api/analyze')
            data = json.loads(response.data)
            cls.batch_id = data['batch_id']
    
    def test_get_latest_analysis(self):
        """Test retrieving latest analysis."""
        response = self.client.get('/api/analysis/latest')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['total_sites'], 91)
    
    def test_get_analysis_logs(self):
        """Test retrieving analysis logs."""
        response = self.client.get('/api/analysis/logs')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('logs', data)
        self.assertGreater(data['count'], 0)
    
    def test_get_analysis_by_batch_id(self):
        """Test retrieving specific analysis."""
        response = self.client.get(f'/api/analysis/{self.batch_id}')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['batch_id'], self.batch_id)
    
    def test_get_analysis_invalid_batch_id(self):
        """Test retrieving with invalid batch ID."""
        response = self.client.get('/api/analysis/invalid_id')
        
        self.assertEqual(response.status_code, 404)


class TestSiteEndpoints(TestAPIBase):
    """Tests for site data endpoints."""
    
    @classmethod
    def setUpClass(cls):
        """Run analysis before tests."""
        super().setUpClass()
        
        # Initialize and run analysis
        with app.test_client() as client:
            client.post('/api/analyze')
    
    def test_get_all_sites(self):
        """Test retrieving all sites."""
        response = self.client.get('/api/sites')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('sites', data)
        self.assertEqual(data['count'], 5)
    
    def test_get_sites_by_region(self):
        """Test retrieving sites for specific region."""
        response = self.client.get('/api/sites/region/Ashanti')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['region'], 'Ashanti')
        self.assertEqual(data['count'], 2)
    
    def test_get_sites_invalid_region(self):
        """Test retrieving sites for non-existent region."""
        response = self.client.get('/api/sites/region/InvalidRegion')
        
        self.assertEqual(response.status_code, 404)


class TestStatsEndpoints(TestAPIBase):
    """Tests for statistics endpoints."""
    
    @classmethod
    def setUpClass(cls):
        """Run analysis before tests."""
        super().setUpClass()
        
        with app.test_client() as client:
            client.post('/api/analyze')
    
    def test_get_database_stats(self):
        """Test retrieving database statistics."""
        response = self.client.get('/api/stats')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('total_site_records', data)
    
    def test_get_total_sites(self):
        """Test retrieving total sites count."""
        response = self.client.get('/api/stats/total')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['total_sites'], 91)
    
    def test_get_highest_region(self):
        """Test retrieving highest region."""
        response = self.client.get('/api/stats/highest-region')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['region'], 'Ashanti')
        self.assertEqual(data['total_sites'], 40)  # 25 + 15
    
    def test_get_cities_above_threshold(self):
        """Test retrieving cities above threshold."""
        response = self.client.get('/api/stats/cities-above-threshold?threshold=15')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['threshold'], 15)
        # Cities with > 15: Kumasi (25), Accra (30)
        self.assertEqual(data['count'], 2)
    
    def test_get_average_per_region(self):
        """Test retrieving average sites per region."""
        response = self.client.get('/api/stats/average-per-region')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('averages', data)
        self.assertEqual(data['averages']['Ashanti'], 20.0)  # (25 + 15) / 2


class TestErrorHandling(TestAPIBase):
    """Tests for error handling."""
    
    def test_404_endpoint(self):
        """Test 404 for non-existent endpoint."""
        response = self.client.get('/api/nonexistent')
        
        self.assertEqual(response.status_code, 404)
        data = json.loads(response.data)
        self.assertIn('error', data)
    
    def test_405_method_not_allowed(self):
        """Test 405 for wrong HTTP method."""
        response = self.client.get('/api/analyze')  # Should be POST
        
        self.assertEqual(response.status_code, 405)


if __name__ == '__main__':
    unittest.main(verbosity=2)
