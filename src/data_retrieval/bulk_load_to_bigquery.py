"""
Bulk Load Stock Data to BigQuery

This script loads historical data for hundreds of companies from Yahoo Finance
directly into BigQuery in batches.
"""

import logging
from datetime import datetime, timedelta
from yahoo_to_bigquery import BigQueryStockLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_tickers_from_file(filename='data/tickers_top1000.txt'):
    """Load ticker symbols from a text file."""
    with open(filename, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
    return tickers


def bulk_load_stocks(
    tickers,
    project_id,
    dataset_id='StockData',
    table_id='daily_prices',
    batch_size=50,
    start_date=None,
    end_date=None
):
    """
    Load multiple stocks into BigQuery in batches.
    
    Parameters
    ----------
    tickers : list
        List of ticker symbols
    project_id : str
        Google Cloud project ID
    dataset_id : str
        BigQuery dataset name
    table_id : str
        BigQuery table name
    batch_size : int
        Number of tickers to process at once (to handle failures gracefully)
    start_date : str
        Start date (YYYY-MM-DD), defaults to 5 years ago
    end_date : str
        End date (YYYY-MM-DD), defaults to today
    """
    # Set default dates if not provided
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    logger.info("=" * 80)
    logger.info(f"BULK LOADING {len(tickers)} COMPANIES INTO BIGQUERY")
    logger.info("=" * 80)
    logger.info(f"Project: {project_id}")
    logger.info(f"Dataset: {dataset_id}")
    logger.info(f"Table: {table_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Batch Size: {batch_size} tickers")
    logger.info("=" * 80)
    
    # Initialize BigQuery loader
    loader = BigQueryStockLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )
    
    # Process in batches
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    all_results = {}
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"BATCH {batch_num + 1}/{total_batches} - Processing tickers {start_idx + 1}-{end_idx}")
        logger.info("=" * 80)
        
        # Download and insert batch
        batch_results = loader.download_and_insert(
            tickers=batch_tickers,
            start_date=start_date,
            end_date=end_date
        )
        
        all_results.update(batch_results)
        
        # Progress summary
        successful_so_far = sum(all_results.values())
        total_so_far = len(all_results)
        logger.info(f"Progress: {successful_so_far}/{total_so_far} successful ({successful_so_far/total_so_far*100:.1f}%)")
    
    # Final summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    
    successful = [t for t, success in all_results.items() if success]
    failed = [t for t, success in all_results.items() if not success]
    
    logger.info(f"Total tickers processed: {len(all_results)}")
    logger.info(f"✓ Successful: {len(successful)} ({len(successful)/len(all_results)*100:.1f}%)")
    logger.info(f"✗ Failed: {len(failed)} ({len(failed)/len(all_results)*100:.1f}%)")
    
    if failed:
        logger.info(f"\nFailed tickers ({len(failed)}):")
        logger.info(", ".join(failed[:20]) + ("..." if len(failed) > 20 else ""))
    
    logger.info("=" * 80)
    logger.info("✓ BULK LOAD COMPLETE!")
    logger.info("=" * 80)
    
    # Query sample to verify
    logger.info("\nVerifying data with sample query...")
    loader.query_sample(limit=20)
    
    return all_results


def main():
    """Main function to bulk load stocks from file."""
    # Configuration
    PROJECT_ID = "pro-visitor-429015-f5"
    DATASET_ID = "StockData"
    TABLE_ID = "daily_prices"
    TICKER_FILE = "data/tickers_top1000.txt"
    BATCH_SIZE = 50  # Process 50 tickers at a time
    
    # Load tickers from file
    logger.info(f"Loading tickers from {TICKER_FILE}...")
    tickers = load_tickers_from_file(TICKER_FILE)
    logger.info(f"✓ Loaded {len(tickers)} tickers")
    
    # Calculate date range (last 5 years)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    # Bulk load
    results = bulk_load_stocks(
        tickers=tickers,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        batch_size=BATCH_SIZE,
        start_date=start_date,
        end_date=end_date
    )
    
    logger.info("\n" + "=" * 80)
    logger.info("All done! Your BigQuery table is now loaded with stock data.")
    logger.info(f"View it at: https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()

