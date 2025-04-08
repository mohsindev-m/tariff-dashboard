import logging
import threading
from datetime import datetime
from app.services.tariff_pipeline import TariffDataPipeline

logger = logging.getLogger("tariff_scheduler")

class TariffScheduler:
    def __init__(self, pipeline: TariffDataPipeline):
        self.pipeline = pipeline
        self.running = False
        self.thread = None
    
        
    def start(self):
        """Start the scheduler in a background thread"""
        if self.running:
            logger.warning("Scheduler is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler)
        self.thread.daemon = True
        self.thread.start()
        
        # Add an immediate task to run after startup (with slight delay)
        delayed_start = threading.Timer(10.0, self._run_full_update)
        delayed_start.start()
        
        logger.info("Tariff data scheduler started with immediate update scheduled")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
            logger.info("Tariff data scheduler stopped")
    
    def _run_scheduler(self):
        """Run the scheduler loop"""
        import time
        import schedule
        
        # Schedule tasks
        
        # Full pipeline update daily at 4 AM
        schedule.every().day.at("04:00").do(self._run_full_update)
        
        # White House updates at noon and 4 PM
        schedule.every().day.at("12:00").do(self._run_whitehouse_update)
        schedule.every().day.at("16:00").do(self._run_whitehouse_update)
        
        # News updates every 3 hours
        schedule.every(3).hours.do(self._run_news_update)
        
        logger.info("Scheduler initialized with tasks")
        
        # Run the loop
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def _run_full_update(self):
        """Run a full pipeline update"""
        logger.info(f"Starting scheduled full pipeline update at {datetime.now().isoformat()}")
        try:
            self.pipeline.run_full_pipeline()
            logger.info(f"Completed scheduled pipeline update at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Error in scheduled pipeline update: {e}")
    
    def _run_whitehouse_update(self):
        """Run just the White House update"""
        logger.info(f"Starting scheduled White House update at {datetime.now().isoformat()}")
        try:
            self.pipeline.collect_whitehouse_data()
            logger.info(f"Completed White House update at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Error in White House update: {e}")
    
    def _run_news_update(self):
        """Run just the news update"""
        logger.info(f"Starting scheduled news update at {datetime.now().isoformat()}")
        try:
            self.pipeline.collect_news_data()
            logger.info(f"Completed news update at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Error in news update: {e}")

# Factory function for dependency injection
def get_scheduler(pipeline: TariffDataPipeline):
    return TariffScheduler(pipeline)