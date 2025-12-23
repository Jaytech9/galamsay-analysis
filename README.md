# Galamsay Data Analysis API

A Python application for analyzing illegal small-scale mining (Galamsay) data in Ghana. This project provides data analysis functions, database storage, and a RESTful API to access the analysis results.

## Project Overview

This project analyzes Galamsay site data across various cities and regions in Ghana, providing insights such as:
- Total number of Galamsay sites across all cities
- Region with the highest number of Galamsay sites
- Cities where Galamsay sites exceed a given threshold
- Average number of Galamsay sites per region

## Features

- **Data Analysis**: Comprehensive analysis of Galamsay data with error handling for invalid entries
- **Database Storage**: SQLite database to store raw data and analysis results
- **RESTful API**: Flask-based API to expose analysis results
- **Data Validation**: Handles missing data, invalid regions, non-numeric values, and outliers
- **Comprehensive Testing**: Unit tests for all modules

## Project Structure

```
galamsay_analysis/
├── analysis.py          # Data analysis functions
├── database.py          # Database operations
├── app.py              # Flask REST API
├── galamsay_data.csv   # Input data file
├── requirements.txt    # Project dependencies
├── README.md          # This file
└── tests/
    ├── __init__.py
    ├── test_analysis.py    # Analysis module tests
    ├── test_database.py    # Database module tests
    └── test_api.py         # API endpoint tests
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd galamsay_analysis
   ```

2. **Create a virtual environment (recommended)**
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Analysis Directly

To run the analysis from the command line:

```bash
python analysis.py
```

This will output the analysis results to the console.

### Running the API Server

1. **Start the Flask server**
   ```bash
   python app.py
   ```

2. **The API will be available at** `http://localhost:5000`

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API documentation |
| `/api/health` | GET | Health check |
| `/api/analyze` | POST | Run new analysis and save to database |
| `/api/analysis/latest` | GET | Get most recent analysis results |
| `/api/analysis/logs` | GET | Get all analysis log entries |
| `/api/analysis/<batch_id>` | GET | Get specific analysis by batch ID |
| `/api/sites` | GET | Get all site records |
| `/api/sites/region/<region>` | GET | Get sites for a specific region |
| `/api/stats` | GET | Get database statistics |
| `/api/stats/total` | GET | Get total Galamsay sites |
| `/api/stats/highest-region` | GET | Get region with highest sites |
| `/api/stats/cities-above-threshold` | GET | Get cities above threshold |
| `/api/stats/average-per-region` | GET | Get average sites per region |
| `/api/invalid-records` | GET | Get invalid/skipped records |

### Example API Calls

```bash
# Run a new analysis
curl -X POST http://localhost:5000/api/analyze

# Get total sites
curl http://localhost:5000/api/stats/total

# Get cities above threshold of 15
curl "http://localhost:5000/api/stats/cities-above-threshold?threshold=15"

# Get sites in Ashanti region
curl http://localhost:5000/api/sites/region/Ashanti
```

## Running Tests

### Using unittest (built-in)

```bash
# Run all tests
python -m unittest discover -s tests -v

# Run specific test file
python -m unittest tests.test_analysis -v
python -m unittest tests.test_database -v
python -m unittest tests.test_api -v
```

### Using pytest (if installed)

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ -v --cov=. --cov-report=html
```

## Data Validation

The application handles the following data quality issues:
- **Missing city names**: Records are skipped and logged
- **Missing regions**: Records are skipped and logged
- **Invalid regions**: Only valid Ghanaian regions are accepted
- **Non-numeric site counts**: Values like "abc" or "eleven" are rejected
- **Negative values**: Negative site counts are rejected
- **Outliers**: Values above 500 are flagged as unrealistic

## Database Schema

The SQLite database contains three tables:

1. **galamsay_sites**: Stores valid site records
   - id, city, region, num_sites, created_at, batch_id

2. **analysis_log**: Stores analysis results
   - id, batch_id, analysis_timestamp, total_sites, etc.

3. **invalid_records**: Stores rejected records for review
   - id, batch_id, row_number, city, region, reason, etc.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GALAMSAY_DATA_FILE` | Path to CSV data file | `galamsay_data.csv` |
| `GALAMSAY_DB_PATH` | Path to SQLite database | `galamsay_analysis.db` |

## Author

**Jones Kwamehene**  
Cybersecurity Professional & Software Developer

## License

This project is created for the OFWA Coding Test.

## Acknowledgments

- OFWA for providing the coding test requirements
- Data represents illegal small-scale mining activities in Ghana
