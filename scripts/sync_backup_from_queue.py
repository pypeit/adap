import argparse
import logging
import sys
import os
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

from utils import init_logging, run_task_on_queue
from rclone import RClonePath, get_cloud_path


def sync_dataset(args, dataset):

    root_path = Path(args.adap_root_dir)
    # Download data from s3 to sync. get_cloud_path is what the reduce stage writes
    # through, so this stays in step with it rather than hardcoding a root.
    source_loc = get_cloud_path(args, "s3") / dataset
    for reduce_path in source_loc.glob("reduce*"):

        # Download the reduce path from s3
        relative_path = Path(dataset, reduce_path.path.name)
        local_path = root_path / relative_path
        reduce_path.download(local_path)

        # Sync the google drive with the data downloaded from s3        
        gdrive_path = RClonePath(args.rclone_conf, "gdrive", "backups", relative_path)
        gdrive_path.sync_from(local_path)

        # Clean up local data to avoid filling up space.
        logger.info(f"Cleaning up local copy of {local_path}")
        shutil.rmtree(local_path)

    return "COMPLETE"



def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="Name of the work queue on the redis server.")
    parser.add_argument('--queue_timeout', type = int,default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="sync_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    args = parser.parse_args()

    init_logging(args.logfile)

    try:
        run_task_on_queue(args, sync_dataset)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

