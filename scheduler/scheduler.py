from apscheduler.schedulers.background import BackgroundScheduler
from controllers.stats_controllers import reload_stats_data
from services.data_pipeline.stats_api_service import stats_api_refresh
import pytz

data_scheduler = BackgroundScheduler(timezone = pytz.timezone('Pacific/Auckland'))
data_scheduler.add_job(func=reload_stats_data, 
    trigger='cron', 
    minute='5',
    hour='3',
    day='*',
    month='*',
    week='*',
    id='data_reload')

data_scheduler.add_job(func=stats_api_refresh, 
    trigger='cron', 
    minute='5',
    hour='1',
    day='*',
    month='*',
    week='*',
    id='data_refresh')
data_scheduler.start()