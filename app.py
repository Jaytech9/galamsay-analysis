"""
Galamsay Analysis REST API

This module provides a RESTful API to access Galamsay data analysis results.
Built with Flask, it exposes endpoints for:
- Running new analyses
- Retrieving analysis logs
- Querying site data by region
- Getting statistics and summaries

Author: The Ghost Packet
Date: December 2025
"""

from flask import Flask, jsonify, request
from functools import wraps
import os

from analysis import run_full_analysis, load_data, get_total_sites, \
    get_region_with_highest_sites, get_cities_above_threshold, \
    get_average_sites_per_region, get_region_summary
from database import (
    init_database, save_analysis_to_database, get_all_analysis_logs,
    get_analysis_by_batch_id, get_latest_analysis, get_sites_by_region,
    get_all_sites, get_invalid_records, get_database_stats
)


# Initialize Flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False  # Preserve order in JSON responses

# Configuration
DATA_FILE = os.environ.get('GALAMSAY_DATA_FILE', 'galamsay_data.csv')
DB_PATH = os.environ.get('GALAMSAY_DB_PATH', 'galamsay_analysis.db')


def handle_errors(f):
    """
    Decorator to handle exceptions and return appropriate error responses.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except FileNotFoundError as e:
            return jsonify({
                'error': 'File not found',
                'message': str(e)
            }), 404
        except ValueError as e:
            return jsonify({
                'error': 'Invalid data',
                'message': str(e)
            }), 400
        except Exception as e:
            return jsonify({
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    return decorated_function


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/')
def index():
    """
    API root endpoint - returns available endpoints.
    """
    return jsonify({
        'name': 'Galamsay Analysis API',
        'version': '1.0.0',
        'description': 'RESTful API for analyzing illegal small-scale mining (Galamsay) data in Ghana',
        'endpoints': {
            'GET /': 'This help message',
            'GET /api/health': 'Health check endpoint',
            'POST /api/analyze': 'Run new analysis and save to database',
            'GET /api/analysis/latest': 'Get the most recent analysis results',
            'GET /api/analysis/logs': 'Get all analysis log entries',
            'GET /api/analysis/<batch_id>': 'Get specific analysis by batch ID',
            'GET /api/sites': 'Get all site records',
            'GET /api/sites/region/<region>': 'Get sites for a specific region',
            'GET /api/stats': 'Get database statistics',
            'GET /api/stats/total': 'Get total number of Galamsay sites',
            'GET /api/stats/highest-region': 'Get region with highest sites',
            'GET /api/stats/cities-above-threshold': 'Get cities above threshold',
            'GET /api/stats/average-per-region': 'Get average sites per region',
            'GET /api/invalid-records': 'Get invalid/skipped records'
        }
    })


@app.route('/api/health')
def health_check():
    """
    Health check endpoint for monitoring.
    """
    return jsonify({
        'status': 'healthy',
        'data_file': DATA_FILE,
        'database': DB_PATH
    })


@app.route('/api/analyze', methods=['POST'])
@handle_errors
def run_analysis():
    """
    Run a new analysis on the CSV data and save results to database.
    
    Query Parameters:
        threshold (int): Threshold for city filtering. Default is 10.
        
    Returns:
        JSON object containing analysis results and batch_id.
    """
    # Get threshold from query parameters
    threshold = request.args.get('threshold', 10, type=int)
    
    # Validate threshold
    if threshold < 0:
        return jsonify({
            'error': 'Invalid threshold',
            'message': 'Threshold must be a non-negative integer'
        }), 400
    
    # Initialize database
    init_database(DB_PATH)
    
    # Run analysis
    results = run_full_analysis(DATA_FILE, threshold=threshold)
    
    # Save to database
    batch_id = save_analysis_to_database(results, DB_PATH)
    
    # Prepare response (exclude valid_data for cleaner response)
    response = {
        'batch_id': batch_id,
        'message': 'Analysis completed and saved to database',
        'summary': {
            'total_sites': results['total_sites'],
            'total_valid_records': results['total_valid_records'],
            'total_invalid_records': results['total_invalid_records'],
            'region_with_highest_sites': results['region_with_highest_sites'],
            'cities_above_threshold': {
                'threshold': results['cities_above_threshold']['threshold'],
                'count': results['cities_above_threshold']['count']
            },
            'regions_analyzed': len(results['average_sites_per_region'])
        }
    }
    
    return jsonify(response), 201


@app.route('/api/analysis/latest')
@handle_errors
def get_latest():
    """
    Get the most recent analysis results.
    """
    init_database(DB_PATH)
    latest = get_latest_analysis(DB_PATH)
    
    if not latest:
        return jsonify({
            'error': 'No analysis found',
            'message': 'No analysis has been run yet. Use POST /api/analyze first.'
        }), 404
    
    return jsonify(latest)


@app.route('/api/analysis/logs')
@handle_errors
def get_logs():
    """
    Get all analysis log entries.
    
    Query Parameters:
        limit (int): Maximum number of logs to return. Default is 10.
    """
    init_database(DB_PATH)
    limit = request.args.get('limit', 10, type=int)
    
    logs = get_all_analysis_logs(DB_PATH)
    
    return jsonify({
        'count': len(logs[:limit]),
        'total': len(logs),
        'logs': logs[:limit]
    })


@app.route('/api/analysis/<batch_id>')
@handle_errors
def get_analysis(batch_id):
    """
    Get a specific analysis by batch ID.
    """
    init_database(DB_PATH)
    analysis = get_analysis_by_batch_id(batch_id, DB_PATH)
    
    if not analysis:
        return jsonify({
            'error': 'Analysis not found',
            'message': f'No analysis found with batch_id: {batch_id}'
        }), 404
    
    return jsonify(analysis)


@app.route('/api/sites')
@handle_errors
def get_sites():
    """
    Get all galamsay site records.
    
    Query Parameters:
        batch_id (str): Optional batch ID to filter results.
        limit (int): Maximum number of records to return. Default is 100.
    """
    init_database(DB_PATH)
    batch_id = request.args.get('batch_id')
    limit = request.args.get('limit', 100, type=int)
    
    sites = get_all_sites(DB_PATH, batch_id)
    
    return jsonify({
        'count': len(sites[:limit]),
        'total': len(sites),
        'sites': sites[:limit]
    })


@app.route('/api/sites/region/<region>')
@handle_errors
def get_region_sites(region):
    """
    Get all galamsay site records for a specific region.
    """
    init_database(DB_PATH)
    sites = get_sites_by_region(region, DB_PATH)
    
    if not sites:
        return jsonify({
            'error': 'No sites found',
            'message': f'No sites found for region: {region}'
        }), 404
    
    return jsonify({
        'region': region,
        'count': len(sites),
        'sites': sites
    })


@app.route('/api/stats')
@handle_errors
def get_stats():
    """
    Get overall database statistics.
    """
    init_database(DB_PATH)
    stats = get_database_stats(DB_PATH)
    return jsonify(stats)


@app.route('/api/stats/total')
@handle_errors
def get_total():
    """
    Get the total number of Galamsay sites from the latest analysis.
    
    This is a direct response to requirement:
    "Total number of Galamsay sites across all cities"
    """
    init_database(DB_PATH)
    latest = get_latest_analysis(DB_PATH)
    
    if not latest:
        # Run analysis on the fly if no stored data
        valid_data, _ = load_data(DATA_FILE)
        total = get_total_sites(valid_data)
        return jsonify({
            'total_sites': total,
            'source': 'live_calculation'
        })
    
    return jsonify({
        'total_sites': latest['total_sites'],
        'source': 'database',
        'batch_id': latest['batch_id']
    })


@app.route('/api/stats/highest-region')
@handle_errors
def get_highest_region():
    """
    Get the region with the highest number of Galamsay sites.
    
    This is a direct response to requirement:
    "Region with the highest number of Galamsay sites"
    """
    init_database(DB_PATH)
    latest = get_latest_analysis(DB_PATH)
    
    if not latest:
        # Run analysis on the fly if no stored data
        valid_data, _ = load_data(DATA_FILE)
        region, count = get_region_with_highest_sites(valid_data)
        return jsonify({
            'region': region,
            'total_sites': count,
            'source': 'live_calculation'
        })
    
    return jsonify({
        'region': latest['highest_region'],
        'total_sites': latest['highest_region_sites'],
        'source': 'database',
        'batch_id': latest['batch_id']
    })


@app.route('/api/stats/cities-above-threshold')
@handle_errors
def get_cities_threshold():
    """
    Get cities where Galamsay sites exceed a given threshold.
    
    Query Parameters:
        threshold (int): Minimum number of sites. Default is 10.
    
    This is a direct response to requirement:
    "List cities where the Galamsay sites exceed a given threshold"
    """
    threshold = request.args.get('threshold', 10, type=int)
    
    if threshold < 0:
        return jsonify({
            'error': 'Invalid threshold',
            'message': 'Threshold must be a non-negative integer'
        }), 400
    
    # Always calculate live for accurate threshold filtering
    valid_data, _ = load_data(DATA_FILE)
    cities = get_cities_above_threshold(valid_data, threshold)
    
    return jsonify({
        'threshold': threshold,
        'count': len(cities),
        'cities': cities
    })


@app.route('/api/stats/average-per-region')
@handle_errors
def get_averages():
    """
    Get average number of Galamsay sites per region.
    
    This is a direct response to requirement:
    "Average number of Galamsay sites per region"
    """
    init_database(DB_PATH)
    latest = get_latest_analysis(DB_PATH)
    
    if not latest:
        # Run analysis on the fly if no stored data
        valid_data, _ = load_data(DATA_FILE)
        averages = get_average_sites_per_region(valid_data)
        return jsonify({
            'averages': averages,
            'source': 'live_calculation'
        })
    
    return jsonify({
        'averages': latest['average_per_region'],
        'source': 'database',
        'batch_id': latest['batch_id']
    })


@app.route('/api/invalid-records')
@handle_errors
def get_invalid():
    """
    Get all invalid/skipped records.
    
    Query Parameters:
        batch_id (str): Optional batch ID to filter results.
    """
    init_database(DB_PATH)
    batch_id = request.args.get('batch_id')
    
    records = get_invalid_records(DB_PATH, batch_id)
    
    return jsonify({
        'count': len(records),
        'records': records
    })


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'error': 'Method not allowed',
        'message': 'The method is not allowed for the requested URL'
    }), 405


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # Initialize database on startup
    print("Initializing database...")
    init_database(DB_PATH)
    
    # Check if data file exists
    if os.path.exists(DATA_FILE):
        print(f"Data file found: {DATA_FILE}")
    else:
        print(f"Warning: Data file not found: {DATA_FILE}")
    
    # Run the Flask development server
    print("\nStarting Galamsay Analysis API...")
    print("API documentation available at: http://localhost:5000/")
    
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )
