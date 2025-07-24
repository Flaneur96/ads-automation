"""
Scheduler dla automatycznej synchronizacji danych
"""
import os
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import db
from sync.ads_sync import sync_all_clients as sync_google_ads
from sync.meta_sync import sync_all_meta_accounts as sync_meta_ads  # ← ZMIANA TUTAJ!

logger = logging.getLogger(__name__)

# Globalny scheduler
scheduler = None

def daily_sync_job():
    """Codzienna synchronizacja wszystkich platform"""
    logger.info(f"=== Starting daily sync at {datetime.now()} ===")
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'google_ads': {'success': 0, 'failed': 0, 'total_rows': 0},
        'meta_ads': {'success': 0, 'failed': 0, 'total_rows': 0}
    }
    
    try:
        # 1. Google Ads
        logger.info("Starting Google Ads sync...")
        google_results = sync_google_ads()
        results['google_ads'] = google_results
        logger.info(f"Google Ads sync completed: {google_results}")
        
    except Exception as e:
        logger.error(f"Google Ads sync failed: {e}")
        results['google_ads']['error'] = str(e)
    
    try:
        # 2. Meta Ads
        logger.info("Starting Meta Ads sync...")
        meta_results = sync_meta_ads()
        results['meta_ads'] = meta_results
        logger.info(f"Meta Ads sync completed: {meta_results}")
        
    except Exception as e:
        logger.error(f"Meta Ads sync failed: {e}")
        results['meta_ads']['error'] = str(e)
    
    # 3. GA4 - nie robimy nic, Google sam eksportuje
    logger.info("GA4 is handled by BigQuery Export automatically")
    
    logger.info(f"=== Daily sync completed: {results} ===")
    return results

def init_scheduler():
    """Inicjalizuje scheduler"""
    global scheduler
    
    scheduler = BackgroundScheduler()
    
    # Codzienna synchronizacja o 8:00
    scheduler.add_job(
        daily_sync_job,
        CronTrigger(hour=8, minute=0),
        id='daily_sync',
        name='Daily sync all platforms',
        replace_existing=True
    )
    
    # Opcjonalnie: Test sync co 6 godzin
    if os.environ.get('ENABLE_FREQUENT_SYNC', 'false').lower() == 'true':
        scheduler.add_job(
            daily_sync_job,
            'interval',
            hours=6,
            id='frequent_sync',
            name='Frequent sync (test mode)'
        )
    
    scheduler.start()
    logger.info("Scheduler started successfully")

def get_scheduler():
    """Zwraca instancję schedulera"""
    return scheduler

def get_scheduler_status():
    """Status schedulera i zadań"""
    if not scheduler:
        return {"status": "not_initialized"}
    
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    
    return {
        "status": "running" if scheduler.running else "stopped",
        "jobs": jobs
    }

def trigger_manual_sync():
    """Ręczne uruchomienie synchronizacji"""
    logger.info("Manual sync triggered")
    return daily_sync_job()
