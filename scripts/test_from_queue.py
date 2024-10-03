import argparse
import logging
import sys
import os
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)


from utils import run_task_on_queue, init_logging

def test_task(args, queue_item):
    from time import sleep
    from random import randint
    sleep_time = 10 + randint(1,5)
    logger.info(f"Running test task on {queue_item} for {sleep_time}")
    sleep(sleep_time)
    return "COMPLETE"

def main():
    parser = argparse.ArgumentParser(description='Test running a task on a work queue in nautilus.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="Name of the work queue to work off of.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--queue_timeout', type = int,default=30, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=Path, default="test_task_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=Path, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--local", type=Path, default = None, help="Run in local test config, which does not download data if it already is present and moves data to a given directory when done.")
    args = parser.parse_args()

    try:
        init_logging(args.adap_root_dir / args.logfile)
        run_task_on_queue(args, test_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

