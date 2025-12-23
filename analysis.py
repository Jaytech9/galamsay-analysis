"""
Galamsay Data Analysis Module

This module provides functions to analyze illegal small-scale mining (Galamsay)
data in Ghana. It handles data loading, cleaning, and various statistical calculations.

Author: Jones Kwamehene
Date: December 2025
"""

import csv
import os
from typing import Dict, List, Tuple, Optional, Any


def load_data(filepath: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Load and clean Galamsay data from a CSV file.
    
    Args:
        filepath: Path to the CSV file containing Galamsay data.
        
    Returns:
        A tuple containing:
        - List of valid records (cleaned data)
        - List of invalid/skipped records for logging
        
    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the file is empty or has invalid format.
    """
    # Check if file exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")
    
    valid_records = []
    invalid_records = []
    
    # Define valid regions in Ghana for validation
    valid_regions = {
        'Ashanti', 'Western', 'Upper East', 'Greater Accra', 'Northern',
        'Central', 'Bono', 'Upper West', 'Volta', 'Eastern', 'Bono East',
        'Savannah', 'Oti', 'North East', 'Ahafo', 'Western North'
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Validate CSV headers
            required_headers = {'City', 'Region', 'Number_of_Galamsay_Sites'}
            if not required_headers.issubset(set(reader.fieldnames or [])):
                raise ValueError(f"CSV file missing required headers: {required_headers}")
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 (1 is header)
                city = row.get('City', '').strip()
                region = row.get('Region', '').strip()
                sites_str = row.get('Number_of_Galamsay_Sites', '').strip()
                
                # Validate city name
                if not city:
                    invalid_records.append({
                        'row': row_num,
                        'data': row,
                        'reason': 'Missing city name'
                    })
                    continue
                
                # Validate region
                if not region:
                    invalid_records.append({
                        'row': row_num,
                        'data': row,
                        'reason': 'Missing region'
                    })
                    continue
                
                if region not in valid_regions:
                    invalid_records.append({
                        'row': row_num,
                        'data': row,
                        'reason': f'Invalid region: {region}'
                    })
                    continue
                
                # Validate and parse number of sites
                try:
                    num_sites = int(sites_str)
                    
                    # Check for negative values
                    if num_sites < 0:
                        invalid_records.append({
                            'row': row_num,
                            'data': row,
                            'reason': f'Negative site count: {num_sites}'
                        })
                        continue
                    
                    # Check for unrealistic outliers (threshold: 500)
                    if num_sites > 500:
                        invalid_records.append({
                            'row': row_num,
                            'data': row,
                            'reason': f'Unrealistic site count (outlier): {num_sites}'
                        })
                        continue
                        
                except ValueError:
                    invalid_records.append({
                        'row': row_num,
                        'data': row,
                        'reason': f'Non-numeric site count: {sites_str}'
                    })
                    continue
                
                # Add valid record
                valid_records.append({
                    'city': city,
                    'region': region,
                    'num_sites': num_sites
                })
                
    except csv.Error as e:
        raise ValueError(f"Error parsing CSV file: {e}")
    
    if not valid_records:
        raise ValueError("No valid data records found in the file")
    
    return valid_records, invalid_records


def get_total_sites(data: List[Dict[str, Any]]) -> int:
    """
    Calculate the total number of Galamsay sites across all cities.
    
    Args:
        data: List of valid data records.
        
    Returns:
        Total count of all Galamsay sites.
    """
    if not data:
        return 0
    return sum(record['num_sites'] for record in data)


def get_region_with_highest_sites(data: List[Dict[str, Any]]) -> Tuple[str, int]:
    """
    Find the region with the highest number of Galamsay sites.
    
    Args:
        data: List of valid data records.
        
    Returns:
        Tuple of (region_name, total_sites) for the region with most sites.
        
    Raises:
        ValueError: If data is empty.
    """
    if not data:
        raise ValueError("Cannot determine highest region from empty data")
    
    # Aggregate sites by region
    region_totals: Dict[str, int] = {}
    for record in data:
        region = record['region']
        region_totals[region] = region_totals.get(region, 0) + record['num_sites']
    
    # Find region with maximum sites
    max_region = max(region_totals.items(), key=lambda x: x[1])
    return max_region


def get_cities_above_threshold(data: List[Dict[str, Any]], threshold: int = 10) -> List[Dict[str, Any]]:
    """
    Get list of cities where Galamsay sites exceed a given threshold.
    
    Args:
        data: List of valid data records.
        threshold: Minimum number of sites (exclusive). Default is 10.
        
    Returns:
        List of records for cities exceeding the threshold, sorted by site count descending.
    """
    if threshold < 0:
        raise ValueError("Threshold cannot be negative")
    
    cities_above = [
        record for record in data 
        if record['num_sites'] > threshold
    ]
    
    # Sort by number of sites in descending order
    return sorted(cities_above, key=lambda x: x['num_sites'], reverse=True)


def get_average_sites_per_region(data: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Calculate the average number of Galamsay sites per region.
    
    Args:
        data: List of valid data records.
        
    Returns:
        Dictionary mapping region names to their average site counts.
    """
    if not data:
        return {}
    
    # Group data by region
    region_data: Dict[str, List[int]] = {}
    for record in data:
        region = record['region']
        if region not in region_data:
            region_data[region] = []
        region_data[region].append(record['num_sites'])
    
    # Calculate averages
    averages = {}
    for region, sites in region_data.items():
        averages[region] = round(sum(sites) / len(sites), 2)
    
    return averages


def get_region_summary(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Get a comprehensive summary of Galamsay sites by region.
    
    Args:
        data: List of valid data records.
        
    Returns:
        List of dictionaries containing region statistics.
    """
    if not data:
        return []
    
    # Group data by region
    region_data: Dict[str, List[int]] = {}
    for record in data:
        region = record['region']
        if region not in region_data:
            region_data[region] = []
        region_data[region].append(record['num_sites'])
    
    # Calculate summary statistics
    summary = []
    for region, sites in region_data.items():
        summary.append({
            'region': region,
            'total_sites': sum(sites),
            'city_count': len(sites),
            'average_sites': round(sum(sites) / len(sites), 2),
            'max_sites': max(sites),
            'min_sites': min(sites)
        })
    
    # Sort by total sites descending
    return sorted(summary, key=lambda x: x['total_sites'], reverse=True)


def run_full_analysis(filepath: str, threshold: int = 10) -> Dict[str, Any]:
    """
    Run complete analysis on Galamsay data and return all results.
    
    Args:
        filepath: Path to the CSV file.
        threshold: Threshold for city filtering. Default is 10.
        
    Returns:
        Dictionary containing all analysis results.
    """
    # Load and clean data
    valid_data, invalid_data = load_data(filepath)
    
    # Perform all calculations
    total_sites = get_total_sites(valid_data)
    highest_region, highest_count = get_region_with_highest_sites(valid_data)
    cities_above_threshold = get_cities_above_threshold(valid_data, threshold)
    average_per_region = get_average_sites_per_region(valid_data)
    region_summary = get_region_summary(valid_data)
    
    return {
        'total_sites': total_sites,
        'total_valid_records': len(valid_data),
        'total_invalid_records': len(invalid_data),
        'region_with_highest_sites': {
            'region': highest_region,
            'total_sites': highest_count
        },
        'cities_above_threshold': {
            'threshold': threshold,
            'count': len(cities_above_threshold),
            'cities': cities_above_threshold
        },
        'average_sites_per_region': average_per_region,
        'region_summary': region_summary,
        'invalid_records': invalid_data,
        'valid_data': valid_data
    }


if __name__ == '__main__':
    # Example usage
    import json
    
    try:
        results = run_full_analysis('galamsay_data.csv', threshold=10)
        
        print("=" * 60)
        print("GALAMSAY DATA ANALYSIS RESULTS")
        print("=" * 60)
        
        print(f"\n1. Total Galamsay Sites: {results['total_sites']}")
        print(f"   (From {results['total_valid_records']} valid records, "
              f"{results['total_invalid_records']} invalid records skipped)")
        
        print(f"\n2. Region with Highest Sites: {results['region_with_highest_sites']['region']} "
              f"({results['region_with_highest_sites']['total_sites']} sites)")
        
        print(f"\n3. Cities Above Threshold ({results['cities_above_threshold']['threshold']} sites):")
        print(f"   Count: {results['cities_above_threshold']['count']} cities")
        for city in results['cities_above_threshold']['cities'][:10]:  # Show top 10
            print(f"   - {city['city']} ({city['region']}): {city['num_sites']} sites")
        
        print("\n4. Average Sites Per Region:")
        for region, avg in sorted(results['average_sites_per_region'].items(), 
                                   key=lambda x: x[1], reverse=True):
            print(f"   - {region}: {avg:.2f}")
        
        print("\n" + "=" * 60)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Data Error: {e}")
