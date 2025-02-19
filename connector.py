import os  # Import os module for handling file paths
from datetime import datetime
import requests as rq
import logging
from fivetran_connector_sdk import Connector, Operations as op
import duckdb
from tabulate import tabulate

# Setup basic logging configuration
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DEFAULT_CURSOR_DATE = '2024-09-01'
WAREHOUSE_DB_PATH = 'files/warehouse.db'

def schema(configuration: dict) -> list:
    """
    Defines the schema of the connector's output.
    """

    # Check if API Key exists in configuration.json
    if not configuration.get("apikey"):
        log.error("No API key found in configuration. Aborting...")
        return []

    return [
        {
            "table": "eod_historical_data",
            "primary_key": ["symbol", "date"],
            "columns": {
                "date": "STRING",
                "open": "FLOAT",
                "close": "FLOAT",
                "high": "FLOAT",
                "low": "FLOAT",
                "changeOverTime": "FLOAT",
                "changePercent": "FLOAT",
            },
        }
    ]

def str2dt(date_str: str) -> datetime:
    """
    Converts a date string to a datetime object.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        log.error(f"Error converting date string to datetime: {e}")
        raise

def update(configuration: dict, state: dict):
    """
    Fetches and processes historical data for a list of symbols.
    """
    symbols = ["AAPL", "TSLA"]

    # Initialize state if not present
    state.setdefault('cursors', {})

    for symbol in symbols:
        cursor = state['cursors'].get(symbol, DEFAULT_CURSOR_DATE)
        base_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"

        params = {
            "from": cursor,
            "apikey": configuration["apikey"]
        }

        try:
            response = get_api_response(base_url, params)
        except rq.RequestException as e:
            log.error(f"API request failed for {symbol}: {e}")
            continue

        for result in process_api_response(response, cursor, symbol, state):
            yield result

def get_api_response(base_url: str, params: dict) -> dict:
    """
    Makes an API call and returns the response.
    """
    log.info(f"Making API call to URL: {base_url} with params: {params}")
    try:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except rq.RequestException as e:
        log.error(f"Failed to fetch data from {base_url}: {e}")
        raise

def process_api_response(response: dict, cursor: str, symbol: str, state: dict):
    """
    Processes the API response to extract data and update the state.
    """
    if 'historical' not in response or not response['historical']:
        log.warning(f"No historical data found for {symbol}.")
        return

    for row in reversed(response['historical']):
        try:
            row_date = str2dt(row['date'])
            if row_date <= str2dt(cursor):
                continue

            log.info(f"Processing row for date {row['date']}")

            yield op.upsert(
                table="eod_historical_data",
                data={
                    "symbol": symbol,
                    "date": row['date'],
                    "open": row['open'],
                    "close": row['close'],
                    "high": row['high'],
                    "low": row['low'],
                    "changeOverTime": row['changeOverTime'],
                    "changePercent": row['changePercent']
                }
            )

            cursor = row['date']

        except KeyError as e:
            log.error(f"Missing expected data in API response for {symbol}: {e}")
            continue
        except ValueError as e:
            log.error(f"Error processing date for {symbol}: {e}")
            continue

    state['cursors'][symbol] = cursor
    yield op.checkpoint(state={"cursors": state['cursors']})

def run_duckdb_debugging():
    """
    Connects to DuckDB database file, runs a SELECT query on the created table to view the data.
    """
    if not os.path.exists(WAREHOUSE_DB_PATH):
        log.error(f"DuckDB file not found at path: {WAREHOUSE_DB_PATH}")
        return

    try:
        # Connect to DuckDB using the specified file path
        conn = duckdb.connect(WAREHOUSE_DB_PATH)
        log.info(f"Connected to DuckDB database at {WAREHOUSE_DB_PATH} for debugging.")

        # Run a query to check the data in the table
        results = conn.execute("SELECT * FROM tester.eod_historical_data;").fetchall()
        headers = [desc[0] for desc in conn.description]  # Fetch column names

        # Print the results as a formatted table
        print(tabulate(results, headers=headers, tablefmt="psql"))

    except Exception as e:
        log.error(f"Error during DuckDB debugging: {e}")
    finally:
        conn.close()
        log.info("DuckDB connection closed.")

# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Allow for testing by running the script directly
    connector.debug()

    # If in debug mode, connect to DuckDB and print table data
    run_duckdb_debugging()
