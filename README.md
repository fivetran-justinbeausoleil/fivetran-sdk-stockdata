# Fivetran Stock Data Connector

## Overview

This project is a Fivetran connector designed to fetch and process historical stock price data from the **Financial Modeling Prep** API. The connector retrieves stock price data for predefined symbols and stores it in a DuckDB warehouse.

## Features

- Fetches historical stock price data for predefined symbols (`AAPL`, `TSLA`).
- Uses **Fivetran Connector SDK** for integration.
- Implements state management to track the last fetched date for each stock.
- Logs API interactions and data processing events.
- Supports DuckDB for debugging and data validation.

## Setup

### Prerequisites

- Python 3.10+
- Virtual environment (optional but recommended)
- Required dependencies listed in `requirements.txt`

### Installation

1. Clone the repository:
   ```sh
   git clone <your-repo-url>
   cd fivetran-sdk-stockdata
   ```

2. Create and activate a virtual environment:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scriptsctivate
   ```

3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

### Configuration

The connector requires an **API key** from [Financial Modeling Prep](https://financialmodelingprep.com/).

Create a `configuration.json` file:
```json
{
  "apikey": "your_api_key_here"
}
```

### Running the Connector Locally

To test the connector locally, use:

```sh
fivetran debug --configuration configuration.json
```

### Deploying to Fivetran

To deploy the connector:

```sh
fivetran deploy --api-key <your-api-key> --destination <your-destination> --connection <your-connection-name> --configuration configuration.json
```

## Schema

The connector outputs data in the following schema:

### Table: `eod_historical_data`

| Column          | Type   | Description                  |
|----------------|--------|------------------------------|
| `symbol`       | STRING | Stock symbol (e.g., AAPL)   |
| `date`         | STRING | Trading date                |
| `open`         | FLOAT  | Opening price               |
| `close`        | FLOAT  | Closing price               |
| `high`         | FLOAT  | Highest price               |
| `low`          | FLOAT  | Lowest price                |
| `changeOverTime` | FLOAT  | Change in price over time  |
| `changePercent`  | FLOAT  | Percentage price change    |

## Debugging with DuckDB

To verify the fetched data in DuckDB:

```sh
python -c "from connector import run_duckdb_debugging; run_duckdb_debugging()"
```

## Logging

Logs are generated to track API requests and data processing:

```python
logging.basicConfig(level=logging.INFO)
```

## Troubleshooting

### Common Issues

- **Missing API Key**: Ensure `configuration.json` includes a valid API key.
- **Unauthorized (401) Errors**: Verify your API key and API limits.
- **Database Lock Issues**: Run `lsof <path-to-db>` to check for active processes and kill them if necessary.

## License

This project is licensed under the MIT License.

---

Developed by **Justin Beausoleil**
