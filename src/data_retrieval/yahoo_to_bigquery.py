"""
Yahoo Finance to BigQuery Data Loader

This module downloads historical stock market data from Yahoo Finance
and inserts it directly into Google BigQuery with a clean, well-defined schema.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict
import argparse
import sys
import logging

import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQueryStockLoader:
    """
    Handles downloading stock data from Yahoo Finance and loading it into BigQuery
    with a clean, properly structured schema.
    """

    def __init__(self, project_id: str, dataset_id: str = 'StockData', table_id: str = 'daily_prices'):
        """
        Initialize the BigQuery Stock Loader.

        Parameters
        ----------
        project_id : str
            Your Google Cloud project ID (e.g., 'pro-visitor-429015-f5')
        dataset_id : str
            BigQuery dataset name (default: 'StockData')
        table_id : str
            BigQuery table name (default: 'daily_prices')
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"

        logger.info(f"Initialized BigQuery loader for {self.table_ref}")

    def create_table_if_not_exists(self):
        """
        Create the BigQuery table with proper schema if it doesn't exist.
        """
        schema = [
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED", description="Stock ticker symbol"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED", description="Trading date"),
            bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE", description="Opening price"),
            bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE", description="Highest price"),
            bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE", description="Lowest price"),
            bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED", description="Closing price"),
            bigquery.SchemaField("volume", "INT64", mode="NULLABLE", description="Trading volume"),
            bigquery.SchemaField("dividends", "FLOAT64", mode="NULLABLE", description="Dividend amount"),
            bigquery.SchemaField("stock_splits", "FLOAT64", mode="NULLABLE", description="Stock split ratio"),
        ]

        try:
            self.client.get_dataset(f"{self.project_id}.{self.dataset_id}")
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.dataset_id}")

        try:
            self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_id} already exists")
        except NotFound:
            table = bigquery.Table(self.table_ref, schema=schema)
            table.clustering_fields = ["ticker", "date"]
            table = self.client.create_table(table)
            logger.info(f"Created table {self.table_id} with clean column structure")

    def download_and_insert(
        self,
        tickers: List[str],
        start_date: str,
        end_date: Optional[str] = None
    ) -> Dict[str, bool]:
        """
        Download stock data from Yahoo Finance and insert into BigQuery.
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        self.create_table_if_not_exists()
        results = {}

        logger.info("=" * 70)
        logger.info(f"Starting download and load for {len(tickers)} ticker(s)")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Target table: {self.table_ref}")
        logger.info("=" * 70)

        for ticker in tickers:
            try:
                logger.info(f"Processing {ticker}...")
                stock = yf.Ticker(ticker)
                df = stock.history(start=start_date, end=end_date)

                if df.empty:
                    logger.warning(f"No data retrieved for {ticker}")
                    results[ticker] = False
                    continue

                df = df.reset_index()

                clean_data = pd.DataFrame({
                    'ticker': ticker,
                    'date': pd.to_datetime(df['Date']).dt.date,
                    'open': df['Open'].round(2),
                    'high': df['High'].round(2),
                    'low': df['Low'].round(2),
                    'close': df['Close'].round(2),
                    'volume': df['Volume'].astype('Int64'),
                    'dividends': df['Dividends'].round(4),
                    'stock_splits': df['Stock Splits'].round(4)
                })

                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                    ],
                )

                job = self.client.load_table_from_dataframe(
                    clean_data,
                    self.table_ref,
                    job_config=job_config
                )
                job.result()

                logger.info(f"Successfully loaded {len(clean_data)} rows for {ticker}")
                results[ticker] = True

            except Exception as e:
                logger.error(f"Failed to process {ticker}: {str(e)}")
                results[ticker] = False

        successful = sum(results.values())
        logger.info("=" * 70)
        logger.info(f"Load complete: {successful}/{len(tickers)} successful")
        logger.info("=" * 70)

        return results

    def query_sample(self, limit: int = 10):
        """
        Run a sample query to verify data looks clean and properly separated.
        """
        query = f"""
        SELECT 
            ticker,
            date,
            open,
            high,
            low,
            close,
            volume
        FROM `{self.table_ref}`
        ORDER BY date DESC, ticker
        LIMIT {limit}
        """

        logger.info("\nRunning sample query to verify clean column separation...")
        logger.info("-" * 70)

        try:
            df = self.client.query(query).to_dataframe()
            print(df.to_string())
            logger.info("-" * 70)
            logger.info("Data looks clean with properly separated columns!")
        except Exception as e:
            logger.error(f"Query failed: {e}")


def run_loader(
    tickers: List[str],
    project_id: str,
    dataset_id: str = "StockData",
    table_id: str = "daily_prices",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, bool]:
    """
    Helper to download and load tickers; callable from other modules.
    """
    end_date_str = end_date or datetime.now().strftime('%Y-%m-%d')
    if start_date is None:
        start_date_str = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    else:
        start_date_str = start_date

    loader = BigQueryStockLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )

    return loader.download_and_insert(
        tickers=tickers,
        start_date=start_date_str,
        end_date=end_date_str
    )


def main():
    """
    Example usage with flexible ticker input.
    - Accepts tickers via command-line arguments.
    - Prompts the user for ticker symbols when run interactively.
    - Can be imported and called programmatically via `run_loader`.
    """
    default_tickers = ['AAPL', 'GOOGL', 'MSFT']

    parser = argparse.ArgumentParser(description="Load Yahoo Finance data into BigQuery.")
    parser.add_argument("--project-id", default="pro-visitor-429015-f5", help="GCP project ID")
    parser.add_argument("--dataset-id", default="StockData", help="BigQuery dataset ID")
    parser.add_argument("--table-id", default="daily_prices", help="BigQuery table ID")
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Space-separated ticker symbols (e.g., AAPL MSFT GOOGL)"
    )
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (default: 5 years ago)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (default: today)")

    args = parser.parse_args()

    if args.tickers:
        tickers = args.tickers
    elif sys.stdin.isatty():
        user_input = input("Enter ticker symbols (comma-separated) [default: AAPL,GOOGL,MSFT]: ").strip()
        tickers = [t.strip().upper() for t in user_input.split(",") if t.strip()] if user_input else default_tickers
    else:
        tickers = default_tickers

    results = run_loader(
        tickers=tickers,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        start_date=args.start_date,
        end_date=args.end_date
    )

    logger.info("\nLoad Results:")
    for ticker, success in results.items():
        status = "Success" if success else "Failed"
        logger.info(f"  {ticker}: {status}")

    if any(results.values()):
        logger.info("\n" + "=" * 70)
        loader = BigQueryStockLoader(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id
        )
        loader.query_sample(limit=10)


if __name__ == '__main__':
    main()
