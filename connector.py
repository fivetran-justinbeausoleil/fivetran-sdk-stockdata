import os
from datetime import datetime
import requests as rq
import logging
from fivetran_connector_sdk import Connector, Operations as op
import duckdb
from tabulate import tabulate

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Default cursor value if no previous state exists
DEFAULT_CURSOR_DATE = '2024-09-01'

# Local DuckDB file for post-run inspection (debugging only)
WAREHOUSE_DB_PATH = 'files/warehouse.db'


def schema(configuration: dict) -> list:
    """
    Defines the output schema for the connector.

    Required by the Connector SDK. This function is called during the
    connector's setup phase to communicate the destination schema to Fivetran.
    """

    # Validate API key presence in configuration
    if not configuration.get("apikey"):
        log.error("No API key found in configuration. Aborting...")
        return []

    # Return schema definition in SDK-required format
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
    Parses a date string (YYYY-MM-DD) to a datetime object.

    Used for cursor comparison in the update cycle.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        log.error(f"Invalid date format: {e}")
        raise


def update(configuration: dict, state: dict):
    """
    The core sync function required by the Connector SDK.

    Responsible for reading data from the source API, yielding upserts to the
    destination, and checkpointing state for resumability.
    """
    symbols = ["AAPL", "TSLA"]

    # Initialize per-symbol cursor tracking if not present
    state.setdefault('cursors', {})

    for symbol in symbols:
        # Determine current cursor position (i.e., last synced date)
        cursor = state['cursors'].get(symbol, DEFAULT_CURSOR_DATE)
        base_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"

        # Build request parameters
        params = {
            "from": cursor,
            "apikey": configuration["apikey"]
        }

        try:
            # Fetch historical data from the source API
            response = get_api_response(base_url, params)
        except rq.RequestException as e:
            log.error(f"API request failed for {symbol}: {e}")
            continue  # Skip to next symbol on failure

        # Yield upserts and checkpoint state for each row processed
        for result in process_api_response(response, cursor, symbol, state):
            yield result


def get_api_response(base_url: str, params: dict) -> dict:
    """
    Helper function to execute a GET request and return the parsed JSON response.
    """
    log.info(f"Calling API: {base_url} with params: {params}")
    try:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except rq.RequestException as e:
        log.error(f"Failed request to {base_url}: {e}")
        raise


def process_api_response(response: dict, cursor: str, symbol: str, state: dict):
    """
    Iterates over API results and emits one upsert per record.

    Updates the cursor to the latest processed row and checkpoints it using SDK conventions.
    """
    if 'historical' not in response or not response['historical']:
        log.warning(f"No data returned for {symbol}")
        return

    # Process records in ascending order by date
    for row in reversed(response['historical']):
        try:
            row_date = str2dt(row['date'])

            # Skip already-synced rows
            if row_date <= str2dt(cursor):
                continue

            log.info(f"Syncing row for {symbol} on {row['date']}")

            # Emit a row-level upsert operation
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

            # Move cursor forward
            cursor = row['date']

        except KeyError as e:
            log.error(f"Missing key in response for {symbol}: {e}")
            continue
        except ValueError as e:
            log.error(f"Invalid date in row for {symbol}: {e}")
            continue

    # After processing all new rows, checkpoint state for the symbol
    state['cursors'][symbol] = cursor
    yield op.checkpoint(state={"cursors": state['cursors']})


def run_duckdb_debugging():
    """
    Local debug utility to inspect warehouse contents using DuckDB.

    Only intended for developer use when running the connector locally.
    """
    if not os.path.exists(WAREHOUSE_DB_PATH):
        log.error(f"Warehouse not found at: {WAREHOUSE_DB_PATH}")
        return

    try:
        conn = duckdb.connect(WAREHOUSE_DB_PATH)
        log.info(f"Connected to DuckDB at {WAREHOUSE_DB_PATH}")

        # Run query to inspect recently synced data
        results = conn.execute("SELECT * FROM tester.eod_historical_data;").fetchall()
        headers = [desc[0] for desc in conn.description]
        print(tabulate(results, headers=headers, tablefmt="psql"))

    except Exception as e:
        log.error(f"DuckDB query failed: {e}")
    finally:
        conn.close()
        log.info("DuckDB connection closed")


# Initialize the connector by wiring schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import json

    # Load connector configuration from file
    config_path = "configuration.json"
    try:
        with open(config_path, "r") as config_file:
            config = json.load(config_file)
            log.info(f"Loaded configuration: {config}")
            assert "apikey" in config and config["apikey"], "Missing 'apikey'"
    except Exception as e:
        log.error(f"Error loading configuration: {e}")
        raise

    # Start with an empty initial state for local testing
    initial_state = {}

    # Run the connector using SDK's local debug mode
    connector.debug(configuration=config, state=initial_state)

    # Optionally inspect results written to DuckDB
    run_duckdb_debugging()