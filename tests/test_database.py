"""
Test Suite for Database Module

This module contains unit tests for database operations,
including saving, retrieving, and querying analysis data.

Author: The Ghost Packet
Date: December 2025
"""

import unittest
import os
import tempfile
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import (
    init_database, save_analysis_to_database, get_all_analysis_logs,
    get_analysis_by_batch_id, get_latest_analysis, get_sites_by_region,
    get_all_sites, get_invalid_records, get_database_stats, generate_batch_id
)


class TestDatabaseInitialization(unittest.TestCase):
    """Tests for database initialization."""
    
    def setUp(self):
        """Create temporary database file."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_init_creates_database(self):
        """Test that init_database creates the database file."""
        init_database(self.db_path)
        self.assertTrue(os.path.exists(self.db_path))
    
    def test_init_creates_tables(self):
        """Test that init_database creates required tables."""
        import sqlite3
        init_database(self.db_path)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        self.assertIn('galamsay_sites', tables)
        self.assertIn('analysis_log', tables)
        self.assertIn('invalid_records', tables)
        
        conn.close()
    
    def test_init_idempotent(self):
        """Test that init_database can be called multiple times safely."""
        init_database(self.db_path)
        init_database(self.db_path)  # Should not raise
        self.assertTrue(os.path.exists(self.db_path))


class TestBatchIdGeneration(unittest.TestCase):
    """Tests for batch ID generation."""
    
    def test_batch_id_format(self):
        """Test batch ID format."""
        batch_id = generate_batch_id()
        
        # Should be in format: YYYYMMDD_HHMMSS_microseconds
        parts = batch_id.split('_')
        self.assertEqual(len(parts), 3)
        self.assertEqual(len(parts[0]), 8)  # Date
        self.assertEqual(len(parts[1]), 6)  # Time
    
    def test_batch_id_unique(self):
        """Test that batch IDs are unique."""
        import time
        
        ids = set()
        for _ in range(100):
            ids.add(generate_batch_id())
            time.sleep(0.001)  # Small delay to ensure uniqueness
        
        self.assertEqual(len(ids), 100)


class TestSaveAnalysis(unittest.TestCase):
    """Tests for saving analysis to database."""
    
    def setUp(self):
        """Create temporary database and test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        
        self.test_results = {
            'total_sites': 100,
            'total_valid_records': 10,
            'total_invalid_records': 2,
            'region_with_highest_sites': {
                'region': 'Ashanti',
                'total_sites': 50
            },
            'cities_above_threshold': {
                'threshold': 10,
                'count': 5,
                'cities': [
                    {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25}
                ]
            },
            'average_sites_per_region': {'Ashanti': 25.0, 'Northern': 10.0},
            'region_summary': [
                {'region': 'Ashanti', 'total_sites': 50}
            ],
            'valid_data': [
                {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
                {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
            ],
            'invalid_records': [
                {'row': 5, 'data': {'City': 'Bad', 'Region': '', 'Number_of_Galamsay_Sites': '10'}, 'reason': 'Missing region'}
            ]
        }
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_save_returns_batch_id(self):
        """Test that save returns a batch ID."""
        batch_id = save_analysis_to_database(self.test_results, self.db_path)
        
        self.assertIsNotNone(batch_id)
        self.assertIsInstance(batch_id, str)
    
    def test_save_creates_site_records(self):
        """Test that save creates site records."""
        batch_id = save_analysis_to_database(self.test_results, self.db_path)
        
        sites = get_all_sites(self.db_path, batch_id)
        
        self.assertEqual(len(sites), 2)
    
    def test_save_creates_analysis_log(self):
        """Test that save creates analysis log entry."""
        batch_id = save_analysis_to_database(self.test_results, self.db_path)
        
        analysis = get_analysis_by_batch_id(batch_id, self.db_path)
        
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis['total_sites'], 100)
        self.assertEqual(analysis['highest_region'], 'Ashanti')
    
    def test_save_creates_invalid_records(self):
        """Test that save stores invalid records."""
        batch_id = save_analysis_to_database(self.test_results, self.db_path)
        
        invalid = get_invalid_records(self.db_path, batch_id)
        
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]['reason'], 'Missing region')


class TestRetrieveAnalysis(unittest.TestCase):
    """Tests for retrieving analysis data."""
    
    def setUp(self):
        """Create temporary database with test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        
        self.test_results = {
            'total_sites': 100,
            'total_valid_records': 10,
            'total_invalid_records': 0,
            'region_with_highest_sites': {'region': 'Ashanti', 'total_sites': 50},
            'cities_above_threshold': {'threshold': 10, 'count': 5, 'cities': []},
            'average_sites_per_region': {'Ashanti': 25.0},
            'region_summary': [],
            'valid_data': [
                {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25}
            ],
            'invalid_records': []
        }
        
        self.batch_id = save_analysis_to_database(self.test_results, self.db_path)
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_get_all_logs(self):
        """Test retrieving all analysis logs."""
        logs = get_all_analysis_logs(self.db_path)
        
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]['batch_id'], self.batch_id)
    
    def test_get_by_batch_id(self):
        """Test retrieving analysis by batch ID."""
        analysis = get_analysis_by_batch_id(self.batch_id, self.db_path)
        
        self.assertIsNotNone(analysis)
        self.assertEqual(analysis['total_sites'], 100)
    
    def test_get_by_invalid_batch_id(self):
        """Test retrieving with invalid batch ID."""
        analysis = get_analysis_by_batch_id('invalid_id', self.db_path)
        
        self.assertIsNone(analysis)
    
    def test_get_latest(self):
        """Test retrieving latest analysis."""
        latest = get_latest_analysis(self.db_path)
        
        self.assertIsNotNone(latest)
        self.assertEqual(latest['batch_id'], self.batch_id)
    
    def test_get_latest_empty_db(self):
        """Test getting latest from empty database."""
        empty_db = os.path.join(self.temp_dir, 'empty.db')
        init_database(empty_db)
        
        latest = get_latest_analysis(empty_db)
        
        self.assertIsNone(latest)


class TestSiteQueries(unittest.TestCase):
    """Tests for site data queries."""
    
    def setUp(self):
        """Create temporary database with test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        
        self.test_results = {
            'total_sites': 100,
            'total_valid_records': 3,
            'total_invalid_records': 0,
            'region_with_highest_sites': {'region': 'Ashanti', 'total_sites': 50},
            'cities_above_threshold': {'threshold': 10, 'count': 2, 'cities': []},
            'average_sites_per_region': {},
            'region_summary': [],
            'valid_data': [
                {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
                {'city': 'Obuasi', 'region': 'Ashanti', 'num_sites': 15},
                {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
            ],
            'invalid_records': []
        }
        
        self.batch_id = save_analysis_to_database(self.test_results, self.db_path)
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_get_all_sites(self):
        """Test retrieving all sites."""
        sites = get_all_sites(self.db_path)
        
        self.assertEqual(len(sites), 3)
    
    def test_get_sites_by_batch(self):
        """Test filtering sites by batch ID."""
        sites = get_all_sites(self.db_path, self.batch_id)
        
        self.assertEqual(len(sites), 3)
    
    def test_get_sites_by_region(self):
        """Test filtering sites by region."""
        sites = get_sites_by_region('Ashanti', self.db_path)
        
        self.assertEqual(len(sites), 2)
        for site in sites:
            self.assertEqual(site['region'], 'Ashanti')
    
    def test_get_sites_by_invalid_region(self):
        """Test filtering by non-existent region."""
        sites = get_sites_by_region('Invalid', self.db_path)
        
        self.assertEqual(len(sites), 0)


class TestDatabaseStats(unittest.TestCase):
    """Tests for database statistics."""
    
    def setUp(self):
        """Create temporary database with test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        
        self.test_results = {
            'total_sites': 100,
            'total_valid_records': 3,
            'total_invalid_records': 1,
            'region_with_highest_sites': {'region': 'Ashanti', 'total_sites': 50},
            'cities_above_threshold': {'threshold': 10, 'count': 2, 'cities': []},
            'average_sites_per_region': {},
            'region_summary': [],
            'valid_data': [
                {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
                {'city': 'Obuasi', 'region': 'Ashanti', 'num_sites': 15},
                {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
            ],
            'invalid_records': [
                {'row': 5, 'data': {'City': 'Bad'}, 'reason': 'Error'}
            ]
        }
        
        save_analysis_to_database(self.test_results, self.db_path)
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_stats_counts(self):
        """Test database statistics counts."""
        stats = get_database_stats(self.db_path)
        
        self.assertEqual(stats['total_site_records'], 3)
        self.assertEqual(stats['total_analysis_logs'], 1)
        self.assertEqual(stats['total_invalid_records'], 1)
        self.assertEqual(stats['unique_regions'], 2)
        self.assertEqual(stats['unique_cities'], 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
