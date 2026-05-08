import logging
import os
from datetime import datetime
from scraper.spider import QuoteSpider
from scraper.pipeline import DataPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Starting scraping pipeline...")
    start = datetime.now()

    spider = QuoteSpider()
    raw_data = spider.scrape()
    logger.info(f"Scraped {len(raw_data)} items")

    pipeline = DataPipeline()
    df = pipeline.process(raw_data)
    logger.info(f"Processed {len(df)} records")

    pipeline.save_to_csv(df, "data/quotes.csv")
    pipeline.save_to_json(df, "data/quotes.json")
    pipeline.save_to_db(df)

    duration = (datetime.now() - start).total_seconds()
    logger.info(f"Pipeline completed in {duration:.2f}s — {len(df)} records saved")
    return df


if __name__ == "__main__":
    run_pipeline()
