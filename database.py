"""
Database Module for Galamsay Analysis

This module handles database operations including:
- Creating and managing database tables
- Storing raw CSV data
- Logging analysis results
- Retrieving stored data and analysis history

Uses SQLite for simplicity and portability.

Author: The Ghost Packet
Date: December 2025
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager


# Default database file
DEFAULT_DB_PATH = 'galamsay_analysis.db'


@contextmanager
def get_db_connection(db_path: str = DEFAULT_DB_PATH):
    """
    Context manager for database connections.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Yields:
        sqlite3.Connection object.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
    try:
        yield conn
    finally:
        conn.close()


def init_database(db_path: str = DEFAULT_DB_PATH) -> None:
    """
    Initialize the database with required tables.
    
    Creates the following tables:
    - galamsay_sites: Stores the raw CSV data
    - analysis_log: Stores analysis results as a log
    - invalid_records: Stores records that failed validation
    
    Args:
        db_path: Path to the SQLite database file.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Table for storing raw galamsay site data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS galamsay_sites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                region TEXT NOT NULL,
                num_sites INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                batch_id TEXT NOT NULL
            )
        ''')
        
        # Table for logging analysis results
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL UNIQUE,
                analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_sites INTEGER,
                total_valid_records INTEGER,
                total_invalid_records INTEGER,
                highest_region TEXT,
                highest_region_sites INTEGER,
                threshold_used INTEGER,
                cities_above_threshold_count INTEGER,
                average_per_region_json TEXT,
                region_summary_json TEXT,
                cities_above_threshold_json TEXT
            )
        ''')
        
        # Table for storing invalid/skipped records
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invalid_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                row_number INTEGER,
                city TEXT,
                region TEXT,
                num_sites_raw TEXT,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better query performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_galamsay_region 
            ON galamsay_sites(region)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_galamsay_batch 
            ON galamsay_sites(batch_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_analysis_batch 
            ON analysis_log(batch_id)
        ''')
        
        conn.commit()


def generate_batch_id() -> str:
    """
    Generate a unique batch ID for tracking data imports.
    
    Returns:
        String batch ID based on current timestamp.
    """
    return datetime.now().strftime('%Y%m%d_%H%M%S_%f')


def save_analysis_to_database(
    analysis_results: Dict[str, Any],
    db_path: str = DEFAULT_DB_PATH
) -> str:
    """
    Save CSV data and analysis results to the database.
    
    This function stores:
    - All valid galamsay site records
    - Analysis results in the log table
    - Invalid records for reference
    
    Args:
        analysis_results: Dictionary containing all analysis results from run_full_analysis().
        db_path: Path to the SQLite database file.
        
    Returns:
        The batch_id used for this data import.
    """
    # Initialize database if needed
    init_database(db_path)
    
    batch_id = generate_batch_id()
    
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Save valid galamsay site records
        valid_data = analysis_results.get('valid_data', [])
        for record in valid_data:
            cursor.execute('''
                INSERT INTO galamsay_sites (city, region, num_sites, batch_id)
                VALUES (?, ?, ?, ?)
            ''', (record['city'], record['region'], record['num_sites'], batch_id))
        
        # Save analysis log entry
        cursor.execute('''
            INSERT INTO analysis_log (
                batch_id, total_sites, total_valid_records, total_invalid_records,
                highest_region, highest_region_sites, threshold_used,
                cities_above_threshold_count, average_per_region_json,
                region_summary_json, cities_above_threshold_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            batch_id,
            analysis_results['total_sites'],
            analysis_results['total_valid_records'],
            analysis_results['total_invalid_records'],
            analysis_results['region_with_highest_sites']['region'],
            analysis_results['region_with_highest_sites']['total_sites'],
            analysis_results['cities_above_threshold']['threshold'],
            analysis_results['cities_above_threshold']['count'],
            json.dumps(analysis_results['average_sites_per_region']),
            json.dumps(analysis_results['region_summary']),
            json.dumps(analysis_results['cities_above_threshold']['cities'])
        ))
        
        # Save invalid records
        for invalid in analysis_results.get('invalid_records', []):
            cursor.execute('''
                INSERT INTO invalid_records (
                    batch_id, row_number, city, region, num_sites_raw, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                batch_id,
                invalid['row'],
                invalid['data'].get('City', ''),
                invalid['data'].get('Region', ''),
                invalid['data'].get('Number_of_Galamsay_Sites', ''),
                invalid['reason']
            ))
        
        conn.commit()
    
    return batch_id


def get_all_analysis_logs(db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """
    Retrieve all analysis log entries.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        List of all analysis log entries as dictionaries.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM analysis_log 
            ORDER BY analysis_timestamp DESC
        ''')
        
        rows = cursor.fetchall()
        results = []
        for row in rows:
            result = dict(row)
            # Parse JSON fields
            result['average_per_region'] = json.loads(result.pop('average_per_region_json'))
            result['region_summary'] = json.loads(result.pop('region_summary_json'))
            result['cities_above_threshold'] = json.loads(result.pop('cities_above_threshold_json'))
            results.append(result)
        
        return results


def get_analysis_by_batch_id(batch_id: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific analysis log entry by batch ID.
    
    Args:
        batch_id: The batch ID to look up.
        db_path: Path to the SQLite database file.
        
    Returns:
        Analysis log entry as dictionary, or None if not found.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM analysis_log WHERE batch_id = ?
        ''', (batch_id,))
        
        row = cursor.fetchone()
        if row:
            result = dict(row)
            result['average_per_region'] = json.loads(result.pop('average_per_region_json'))
            result['region_summary'] = json.loads(result.pop('region_summary_json'))
            result['cities_above_threshold'] = json.loads(result.pop('cities_above_threshold_json'))
            return result
        
        return None


def get_latest_analysis(db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    """
    Retrieve the most recent analysis log entry.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        Most recent analysis log entry, or None if no entries exist.
    """
    logs = get_all_analysis_logs(db_path)
    return logs[0] if logs else None


def get_sites_by_region(region: str, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """
    Retrieve all galamsay site records for a specific region.
    
    Args:
        region: Name of the region to filter by.
        db_path: Path to the SQLite database file.
        
    Returns:
        List of site records for the specified region.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT city, region, num_sites, created_at, batch_id
            FROM galamsay_sites 
            WHERE region = ?
            ORDER BY num_sites DESC
        ''', (region,))
        
        return [dict(row) for row in cursor.fetchall()]


def get_all_sites(db_path: str = DEFAULT_DB_PATH, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve all galamsay site records, optionally filtered by batch ID.
    
    Args:
        db_path: Path to the SQLite database file.
        batch_id: Optional batch ID to filter results.
        
    Returns:
        List of all site records.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        if batch_id:
            cursor.execute('''
                SELECT city, region, num_sites, created_at, batch_id
                FROM galamsay_sites 
                WHERE batch_id = ?
                ORDER BY region, city
            ''', (batch_id,))
        else:
            cursor.execute('''
                SELECT city, region, num_sites, created_at, batch_id
                FROM galamsay_sites 
                ORDER BY region, city
            ''')
        
        return [dict(row) for row in cursor.fetchall()]


def get_invalid_records(db_path: str = DEFAULT_DB_PATH, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve invalid/skipped records, optionally filtered by batch ID.
    
    Args:
        db_path: Path to the SQLite database file.
        batch_id: Optional batch ID to filter results.
        
    Returns:
        List of invalid record entries.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        if batch_id:
            cursor.execute('''
                SELECT * FROM invalid_records 
                WHERE batch_id = ?
                ORDER BY row_number
            ''', (batch_id,))
        else:
            cursor.execute('''
                SELECT * FROM invalid_records 
                ORDER BY batch_id, row_number
            ''')
        
        return [dict(row) for row in cursor.fetchall()]


def get_database_stats(db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """
    Get overall database statistics.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        Dictionary containing database statistics.
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Count total records
        cursor.execute('SELECT COUNT(*) FROM galamsay_sites')
        total_sites = cursor.fetchone()[0]
        
        # Count analysis logs
        cursor.execute('SELECT COUNT(*) FROM analysis_log')
        total_analyses = cursor.fetchone()[0]
        
        # Count invalid records
        cursor.execute('SELECT COUNT(*) FROM invalid_records')
        total_invalid = cursor.fetchone()[0]
        
        # Get unique regions
        cursor.execute('SELECT COUNT(DISTINCT region) FROM galamsay_sites')
        unique_regions = cursor.fetchone()[0]
        
        # Get unique cities
        cursor.execute('SELECT COUNT(DISTINCT city) FROM galamsay_sites')
        unique_cities = cursor.fetchone()[0]
        
        return {
            'total_site_records': total_sites,
            'total_analysis_logs': total_analyses,
            'total_invalid_records': total_invalid,
            'unique_regions': unique_regions,
            'unique_cities': unique_cities
        }


if __name__ == '__main__':
    # Example usage
    from analysis import run_full_analysis
    
    print("Initializing database...")
    init_database()
    
    print("Running analysis and saving to database...")
    try:
        results = run_full_analysis('galamsay_data.csv', threshold=10)
        batch_id = save_analysis_to_database(results)
        print(f"Data saved with batch ID: {batch_id}")
        
        # Retrieve and display stats
        stats = get_database_stats()
        print(f"\nDatabase Statistics:")
        print(f"  - Total site records: {stats['total_site_records']}")
        print(f"  - Total analysis logs: {stats['total_analysis_logs']}")
        print(f"  - Total invalid records: {stats['total_invalid_records']}")
        print(f"  - Unique regions: {stats['unique_regions']}")
        print(f"  - Unique cities: {stats['unique_cities']}")
        
    except Exception as e:
        print(f"Error: {e}")
