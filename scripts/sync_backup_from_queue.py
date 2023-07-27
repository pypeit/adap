import argparse
import logging
import sys
import os
from pathlib import Path


logger = logging.getLogger(__name__)

from astropy.table import Table

from utils import run_task_on_queue, RCloneLocation, run_script


def sync_dataset(args, dataset):

    dataset_relative_path = Path(dataset ,"complete")
    dataset_local_path = Path(args.adap_root_dir, dataset_relative_path)
    for file in dataset_local_path.iterdir():
        run_script(['rclone', '--config', args.rclone_conf, '-P', '--stats-one-line', '--stats', '60s', '--stats-unit', 'bits', 'sync', str(file),  "gdrive:backups/" + str(dataset_relative_path / file.name)], log_output=True)
    return "COMPLETE"

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument("--logfile", type=str, default="sync_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    args = parser.parse_args()

    try:
        source_loc = RCloneLocation(args, "s3", "{}/complete/reduce*", glob=True)
        log_backup_loc = RCloneLocation(args, "gdrive", "{}/complete/", glob=False)
        dest_loc = None
        backup_loc = None
        run_task_on_queue(args, sync_dataset, source_loc, dest_loc, backup_loc, cleanup=False, log_backup_loc=log_backup_loc)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

