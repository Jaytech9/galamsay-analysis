"""
Test Suite for Galamsay Analysis Module

This module contains unit tests for all analysis functions,
including tests for edge cases and error handling.

Author: The Ghost Packet
Date: December 2025
"""

import unittest
import os
import tempfile
import csv
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis import (
    load_data, get_total_sites, get_region_with_highest_sites,
    get_cities_above_threshold, get_average_sites_per_region,
    get_region_summary, run_full_analysis
)


class TestLoadData(unittest.TestCase):
    """Tests for the load_data function."""
    
    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def create_csv(self, filename, rows):
        """Helper to create a CSV file with given rows."""
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['City', 'Region', 'Number_of_Galamsay_Sites'])
            for row in rows:
                writer.writerow(row)
        return filepath
    
    def test_load_valid_data(self):
        """Test loading valid CSV data."""
        filepath = self.create_csv('valid.csv', [
            ['Kumasi', 'Ashanti', '25'],
            ['Accra', 'Greater Accra', '30'],
            ['Tamale', 'Northern', '15']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 3)
        self.assertEqual(len(invalid), 0)
        self.assertEqual(valid[0]['city'], 'Kumasi')
        self.assertEqual(valid[0]['num_sites'], 25)
    
    def test_file_not_found(self):
        """Test FileNotFoundError for missing file."""
        with self.assertRaises(FileNotFoundError):
            load_data('nonexistent_file.csv')
    
    def test_missing_city(self):
        """Test handling of missing city names."""
        filepath = self.create_csv('missing_city.csv', [
            ['', 'Ashanti', '25'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]['reason'], 'Missing city name')
    
    def test_missing_region(self):
        """Test handling of missing region names."""
        filepath = self.create_csv('missing_region.csv', [
            ['Kumasi', '', '25'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]['reason'], 'Missing region')
    
    def test_invalid_region(self):
        """Test handling of invalid region names."""
        filepath = self.create_csv('invalid_region.csv', [
            ['Kumasi', 'Invalid Region', '25'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertIn('Invalid region', invalid[0]['reason'])
    
    def test_non_numeric_sites(self):
        """Test handling of non-numeric site values."""
        filepath = self.create_csv('non_numeric.csv', [
            ['Kumasi', 'Ashanti', 'abc'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertIn('Non-numeric', invalid[0]['reason'])
    
    def test_negative_sites(self):
        """Test handling of negative site values."""
        filepath = self.create_csv('negative.csv', [
            ['Kumasi', 'Ashanti', '-5'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertIn('Negative', invalid[0]['reason'])
    
    def test_outlier_sites(self):
        """Test handling of unrealistic outlier values."""
        filepath = self.create_csv('outlier.csv', [
            ['Kumasi', 'Ashanti', '1000'],
            ['Accra', 'Greater Accra', '30']
        ])
        
        valid, invalid = load_data(filepath)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertIn('outlier', invalid[0]['reason'])
    
    def test_empty_file_with_header(self):
        """Test handling of empty file (header only)."""
        filepath = self.create_csv('empty.csv', [])
        
        with self.assertRaises(ValueError) as context:
            load_data(filepath)
        self.assertIn('No valid data', str(context.exception))


class TestGetTotalSites(unittest.TestCase):
    """Tests for the get_total_sites function."""
    
    def test_total_sites_normal(self):
        """Test total calculation with normal data."""
        data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30},
            {'city': 'Tamale', 'region': 'Northern', 'num_sites': 15}
        ]
        
        total = get_total_sites(data)
        
        self.assertEqual(total, 70)
    
    def test_total_sites_empty(self):
        """Test total calculation with empty data."""
        self.assertEqual(get_total_sites([]), 0)
    
    def test_total_sites_single(self):
        """Test total calculation with single record."""
        data = [{'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25}]
        self.assertEqual(get_total_sites(data), 25)


class TestGetRegionWithHighestSites(unittest.TestCase):
    """Tests for the get_region_with_highest_sites function."""
    
    def test_highest_region_normal(self):
        """Test finding highest region with normal data."""
        data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Obuasi', 'region': 'Ashanti', 'num_sites': 15},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30},
            {'city': 'Tamale', 'region': 'Northern', 'num_sites': 15}
        ]
        
        region, count = get_region_with_highest_sites(data)
        
        self.assertEqual(region, 'Ashanti')
        self.assertEqual(count, 40)  # 25 + 15
    
    def test_highest_region_single(self):
        """Test with single record."""
        data = [{'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25}]
        
        region, count = get_region_with_highest_sites(data)
        
        self.assertEqual(region, 'Ashanti')
        self.assertEqual(count, 25)
    
    def test_highest_region_empty(self):
        """Test with empty data."""
        with self.assertRaises(ValueError):
            get_region_with_highest_sites([])


class TestGetCitiesAboveThreshold(unittest.TestCase):
    """Tests for the get_cities_above_threshold function."""
    
    def setUp(self):
        """Set up test data."""
        self.data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30},
            {'city': 'Tamale', 'region': 'Northern', 'num_sites': 7},
            {'city': 'Cape Coast', 'region': 'Central', 'num_sites': 10},
            {'city': 'Sunyani', 'region': 'Bono', 'num_sites': 5}
        ]
    
    def test_threshold_10(self):
        """Test with default threshold of 10."""
        result = get_cities_above_threshold(self.data, 10)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['city'], 'Accra')  # Highest first
        self.assertEqual(result[1]['city'], 'Kumasi')
    
    def test_threshold_0(self):
        """Test with threshold of 0."""
        result = get_cities_above_threshold(self.data, 0)
        
        self.assertEqual(len(result), 5)
    
    def test_threshold_high(self):
        """Test with very high threshold."""
        result = get_cities_above_threshold(self.data, 100)
        
        self.assertEqual(len(result), 0)
    
    def test_threshold_negative(self):
        """Test with negative threshold."""
        with self.assertRaises(ValueError):
            get_cities_above_threshold(self.data, -5)
    
    def test_empty_data(self):
        """Test with empty data."""
        result = get_cities_above_threshold([], 10)
        self.assertEqual(result, [])
    
    def test_sorted_descending(self):
        """Test that results are sorted by num_sites descending."""
        result = get_cities_above_threshold(self.data, 0)
        
        for i in range(len(result) - 1):
            self.assertGreaterEqual(result[i]['num_sites'], result[i+1]['num_sites'])


class TestGetAverageSitesPerRegion(unittest.TestCase):
    """Tests for the get_average_sites_per_region function."""
    
    def test_average_normal(self):
        """Test average calculation with normal data."""
        data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Obuasi', 'region': 'Ashanti', 'num_sites': 15},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
        ]
        
        averages = get_average_sites_per_region(data)
        
        self.assertEqual(averages['Ashanti'], 20.0)  # (25 + 15) / 2
        self.assertEqual(averages['Greater Accra'], 30.0)
    
    def test_average_empty(self):
        """Test with empty data."""
        self.assertEqual(get_average_sites_per_region([]), {})
    
    def test_average_single_per_region(self):
        """Test with single city per region."""
        data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
        ]
        
        averages = get_average_sites_per_region(data)
        
        self.assertEqual(averages['Ashanti'], 25.0)
        self.assertEqual(averages['Greater Accra'], 30.0)
    
    def test_average_rounding(self):
        """Test that averages are rounded to 2 decimal places."""
        data = [
            {'city': 'A', 'region': 'Ashanti', 'num_sites': 10},
            {'city': 'B', 'region': 'Ashanti', 'num_sites': 11},
            {'city': 'C', 'region': 'Ashanti', 'num_sites': 12}
        ]
        
        averages = get_average_sites_per_region(data)
        
        self.assertEqual(averages['Ashanti'], 11.0)  # (10 + 11 + 12) / 3 = 11.0


class TestGetRegionSummary(unittest.TestCase):
    """Tests for the get_region_summary function."""
    
    def test_summary_normal(self):
        """Test summary with normal data."""
        data = [
            {'city': 'Kumasi', 'region': 'Ashanti', 'num_sites': 25},
            {'city': 'Obuasi', 'region': 'Ashanti', 'num_sites': 15},
            {'city': 'Accra', 'region': 'Greater Accra', 'num_sites': 30}
        ]
        
        summary = get_region_summary(data)
        
        self.assertEqual(len(summary), 2)
        
        # Find Ashanti summary
        ashanti = next(s for s in summary if s['region'] == 'Ashanti')
        self.assertEqual(ashanti['total_sites'], 40)
        self.assertEqual(ashanti['city_count'], 2)
        self.assertEqual(ashanti['average_sites'], 20.0)
        self.assertEqual(ashanti['max_sites'], 25)
        self.assertEqual(ashanti['min_sites'], 15)
    
    def test_summary_sorted(self):
        """Test that summary is sorted by total_sites descending."""
        data = [
            {'city': 'A', 'region': 'Ashanti', 'num_sites': 10},
            {'city': 'B', 'region': 'Greater Accra', 'num_sites': 50},
            {'city': 'C', 'region': 'Northern', 'num_sites': 5}
        ]
        
        summary = get_region_summary(data)
        
        self.assertEqual(summary[0]['region'], 'Greater Accra')
        self.assertEqual(summary[1]['region'], 'Ashanti')
        self.assertEqual(summary[2]['region'], 'Northern')
    
    def test_summary_empty(self):
        """Test with empty data."""
        self.assertEqual(get_region_summary([]), [])


class TestRunFullAnalysis(unittest.TestCase):
    """Tests for the run_full_analysis function."""
    
    def setUp(self):
        """Create a test CSV file."""
        self.temp_dir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.temp_dir, 'test.csv')
        
        with open(self.filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['City', 'Region', 'Number_of_Galamsay_Sites'])
            writer.writerow(['Kumasi', 'Ashanti', '25'])
            writer.writerow(['Obuasi', 'Ashanti', '15'])
            writer.writerow(['Accra', 'Greater Accra', '30'])
            writer.writerow(['Tamale', 'Northern', '7'])
            writer.writerow(['BadCity', '', '10'])  # Invalid - missing region
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_full_analysis(self):
        """Test complete analysis workflow."""
        results = run_full_analysis(self.filepath, threshold=10)
        
        self.assertEqual(results['total_sites'], 77)  # 25 + 15 + 30 + 7
        self.assertEqual(results['total_valid_records'], 4)
        self.assertEqual(results['total_invalid_records'], 1)
        self.assertEqual(results['region_with_highest_sites']['region'], 'Ashanti')
        self.assertEqual(results['cities_above_threshold']['count'], 3)  # Kumasi 25, Obuasi 15, Accra 30
        self.assertIn('Ashanti', results['average_sites_per_region'])
    
    def test_full_analysis_file_not_found(self):
        """Test with missing file."""
        with self.assertRaises(FileNotFoundError):
            run_full_analysis('nonexistent.csv')


class TestIntegration(unittest.TestCase):
    """Integration tests using the actual data file."""
    
    @classmethod
    def setUpClass(cls):
        """Check if actual data file exists."""
        cls.data_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'galamsay_data.csv'
        )
        cls.file_exists = os.path.exists(cls.data_file)
    
    @unittest.skipUnless(
        os.path.exists(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'galamsay_data.csv'
        )),
        "galamsay_data.csv not found"
    )
    def test_load_actual_data(self):
        """Test loading actual galamsay_data.csv."""
        valid, invalid = load_data(self.data_file)
        
        # Check that data was loaded
        self.assertGreater(len(valid), 0)
        
        # Check data structure
        for record in valid:
            self.assertIn('city', record)
            self.assertIn('region', record)
            self.assertIn('num_sites', record)
            self.assertIsInstance(record['num_sites'], int)
            self.assertGreaterEqual(record['num_sites'], 0)
    
    @unittest.skipUnless(
        os.path.exists(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'galamsay_data.csv'
        )),
        "galamsay_data.csv not found"
    )
    def test_full_analysis_actual_data(self):
        """Test full analysis on actual data."""
        results = run_full_analysis(self.data_file)
        
        # Verify all expected keys exist
        expected_keys = [
            'total_sites', 'total_valid_records', 'total_invalid_records',
            'region_with_highest_sites', 'cities_above_threshold',
            'average_sites_per_region', 'region_summary'
        ]
        for key in expected_keys:
            self.assertIn(key, results)


if __name__ == '__main__':
    unittest.main(verbosity=2)
