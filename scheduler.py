import schedule
import time
from script import fetch_and_save_tickers
from datetime import datetime

def basic_job():
    print(f"Job started at {datetime.now()}")

    #Run every minute
schedule.every().minutes.do(basic_job)
    #Run every minute
schedule.every().minutes.do(fetch_and_save_tickers)

while True:
    schedule.run_pending()
    time.sleep(1)

    print(f"Job finished at {datetime.now()}")