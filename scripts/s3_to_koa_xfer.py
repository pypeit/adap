#!/usr/bin/env python3

import os
from pathlib import Path
import shutil
import traceback
import datetime
import argparse
import sys


from rclone import get_cloud_path, RClonePath
from utils import claim_datasets, set_dataset_status, init_logging, run_script
import logging
logger = logging.getLogger(__name__)
   
    
def download_datasets(args, remote_source, dirs_to_scan):
    
    for dir in dirs_to_scan:
        source_path = remote_source / dir
        dest_path = Path(args.local_dir,dir)
        print(f"Downloading {dir}")
        set_dataset_status(args, str(dir), "DOWNLOADING")
        source_path.download(dest_path)
        set_dataset_status(args, str(dir), "DOWNLOADED")



def get_parser():

    parser = argparse.ArgumentParser(Path(sys.argv[0]).name, description='Create a remote archive of fits files and metadata for submission to KOA.')


    parser.add_argument('remote_source_root', type=str, default=None, help="Cloud location of the files to send to KOA.")
    parser.add_argument('rsync_url', type=str, default=None, help="rsync URL to use when sending the files to KOA.")
    parser.add_argument('local_dir', type=str, help="Local directory to temporarily contain the files being transferred..")
    parser.add_argument('--logfile', type=str, default="adap_koa_xfer.log", help="Location of the log file. Defaults to adap_koa_xfer.log")
    parser.add_argument('--queue_url', type=str, default=None,  help="Host and port of a redis queue server.")
    parser.add_argument('--work_queue', type=str, default=None, help="Name of the queue to pull sub directories from.")
    parser.add_argument('--queue_batch', type=int, default=3, help="Number of subdirs to pull from the queue at one time.")
    parser.add_argument('--queue_timeout', type=int, default=30, help="Timeout to wait for items to populate the queue.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--subdirs", type=str, nargs="*", default=[], help="List of subdirectories of archive_dir to limit the search for files to.")
    parser.add_argument("--local", action="store_true", default=False, help="Running a local non-cloud test, do not delete old directories.")
    return parser


def main(args):
    exit_status = 0
    start_time = datetime.datetime.now()
    init_logging(args.logfile)
    try:
        if ':' in args.remote_source_root:
            remote_platform, remote_path = args.remote_source_root.split(':',maxsplit=1)
            remote_source_root = RClonePath(args.rclone_conf, remote_platform, remote_path)
        else:
            logger.error(f"Remote source must be have an rclone remote prepended with a ':' - {args.remote_source_root}")

        local_archive_root = Path(args.local_dir)
        local_archive_root.mkdir(parents=True, exist_ok=True)

        done_with_queue = True
        dirs_to_scan = []

        # Grab any directories from our arguments
        if len(args.subdirs) > 0:
            dirs_to_scan += [Path(local_archive_root) / subdir for subdir in args.subdirs]

        if args.queue_url is not None and args.work_queue is not None:
            done_with_queue = False
        
        if not args.rsync_url.endswith("/"):
            args.rsync_url += "/"
        
        while len(dirs_to_scan) > 0 or not done_with_queue:

            if len(dirs_to_scan) == 0:
                if not done_with_queue:
                    logger.info("Attempting to claim datasets")
                    claimed_datasets = claim_datasets(args, os.environ["POD_NAME"], True, args.queue_batch)
                    logger.info(f"Found: {claimed_datasets}")

                    if len(claimed_datasets) < args.queue_batch:
                        done_with_queue = True

                    dirs_to_scan = [Path(local_archive_root, x) for x in claimed_datasets]
                    if len(dirs_to_scan) > 0:
                        download_datasets(args, remote_source_root, claimed_datasets)

            if len(dirs_to_scan) > 0:
                dir = dirs_to_scan.pop()
                dataset = str(dir.relative_to(local_archive_root))
                source = str(dir)
                if not source.endswith("/"):
                    source += "/"
                dest = args.rsync_url
                if not dest.endswith("/"):
                    dest += "/" + dataset
                else:
                    dest += dataset
                if not dest.endswith("/"):
                    dest += "/"
                logger.info(f"Syncing {source} to {dest}")
                set_dataset_status(args, dataset, "IN_PROGRESS")
                try:
                    # Note we need to set RSYNC_PASSWORD for this to work
                    run_script(["rsync", "--partial", "-avr", f"{source}",  f"{dest}"], log_output=True)
                    set_dataset_status(args, dataset, "COMPLETE")
                except Exception as e:
                    logger.error(f"Failed syncing {dir}.", exc_info=True)
                    set_dataset_status(args, dataset, "FAILED")
                if not args.local:
                    shutil.rmtree(dir)



    except Exception as e:
        logger.error(f"Failed KOA xfer.", exc_info=True)
        exit_status = 1

    end_time = datetime.datetime.now()

    logger.info("----------------------------------")
    logger.info(f"Started {start_time.isoformat()}")
    logger.info(f"Finished {end_time.isoformat() }")
    logger.info(f"Duration {end_time - start_time }")

    return exit_status

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))


