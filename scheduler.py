import schedule
import time
import logging
from main import run_pipeline

logger = logging.getLogger(__name__)


def scheduled_run():
    logger.info("Scheduled pipeline run starting...")
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("Scheduler started — running every 6 hours")
    schedule.every(6).hours.do(scheduled_run)
    scheduled_run()
    while True:
        schedule.run_pending()
        time.sleep(60)
