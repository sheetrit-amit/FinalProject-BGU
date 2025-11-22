"""
Check which tickers are missing from BigQuery
"""

import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_missing_tickers():
    """Compare tickers in file vs tickers in BigQuery."""
    # Load tickers from file
    with open('data/tickers_top1000.txt', 'r') as f:
        file_tickers = set(line.strip() for line in f if line.strip())
    
    logger.info(f"Tickers in file: {len(file_tickers)}")
    
    # Get tickers from BigQuery
    client = bigquery.Client(project='pro-visitor-429015-f5')
    query = """
    SELECT DISTINCT ticker
    FROM `pro-visitor-429015-f5.StockData.daily_prices`
    ORDER BY ticker
    """
    
    df = client.query(query).to_dataframe()
    bigquery_tickers = set(df['ticker'].tolist())
    
    logger.info(f"Tickers in BigQuery: {len(bigquery_tickers)}")
    
    # Find missing tickers
    missing = file_tickers - bigquery_tickers
    
    logger.info(f"\n{'='*70}")
    logger.info(f"MISSING TICKERS: {len(missing)}")
    logger.info(f"{'='*70}")
    
    if missing:
        missing_sorted = sorted(list(missing))
        logger.info(f"\nMissing tickers:\n{', '.join(missing_sorted)}")
        
        # Save to file for easy retry
        with open('data/missing_tickers.txt', 'w') as f:
            for ticker in missing_sorted:
                f.write(f"{ticker}\n")
        
        logger.info(f"\n✓ Saved missing tickers to data/missing_tickers.txt")
    else:
        logger.info("\n✓ No missing tickers - all loaded successfully!")
    
    return list(missing)


if __name__ == '__main__':
    missing = get_missing_tickers()

