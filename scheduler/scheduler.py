from apscheduler.schedulers.background import BackgroundScheduler
from controllers.nz_data_controllers import reload_stats_data
import pytz

nz_data_scheduler = BackgroundScheduler(timezone = pytz.timezone('Pacific/Auckland'))
nz_data_scheduler.add_job(func=reload_stats_data, 
    trigger='cron', 
    minute='5',
    hour='3',
    day='*',
    month='*',
    week='*',
    id='data_reload')
nz_data_scheduler.start()