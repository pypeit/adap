#!/usr/bin/env python3

import os
from glob import glob
import re
from functools import partial
from pathlib import Path
import shutil
import traceback
import datetime
import argparse
import sys

from astropy.io import fits
from astropy.table import Table

from pypeit import msgs
from pypeit.scripts import scriptbase
from pypeit.archive import ArchiveDir
from pypeit.core.collate import SourceObject
from pypeit.specobjs import SpecObjs
from pypeit.spectrographs.util import load_spectrograph

from rclone import get_cloud_path, RClonePath
from utils import claim_datasets, set_dataset_status
import logging
logger = logging.getLogger(__name__)

from archive import Messages, create_metadata_archives, populate_archive, write_messages, init_logging
    
class RemoteArchiveDir(ArchiveDir):

    def __init__(self, local_archive_root, dest_archive_root, remote_source_root, metadata_archives):
        super().__init__(dest_archive_root, metadata_archives, copy_to_archive=True)
        self.local_archive_root = local_archive_root
        self.remote_source_root = remote_source_root

    def _archive_file(self, orig_file, dest_file):
        """Override the parent _archive_file to transfer the file to a remote
        location.
        
        orig_file (str): Path to the file to copy.
        dest_file (str): Relative path within the archive to copy the file to.
        
        Returns:
            str: The full path to the new copy in the archive.
        """
        if orig_file is None:
            return orig_file
        
        if not os.path.exists(orig_file):
            raise ValueError(f"File {orig_file} does not exist.")
        
        source_relative_path = orig_file.relative_to(self.local_archive_root)
        remote_source = self.remote_source_root / source_relative_path
        full_dest_path = self.archive_root / dest_file
        full_dest_path._copy(remote_source, full_dest_path, file=True)

    
def download_datasets(args, dirs_to_scan):
    s3_loc = get_cloud_path(args, "s3")
    for dir in dirs_to_scan:
        source_path = s3_loc / dir
        dest_path = Path(args.archive_dir,dir)
        print(f"Downloading {dir}")
        set_dataset_status(args, str(dir), "DOWNLOADING")
        source_path.download(dest_path)
        set_dataset_status(args, str(dir), "DOWNLOADED")



def get_parser():

    parser = argparse.ArgumentParser(Path(sys.argv[0]).name, description='Create a remote archive of fits files and metadata for submission to KOA.')


    parser.add_argument('archive_dir', type=str, help="Directory containing the files being sent to KOA.")
    parser.add_argument('remote_dest', type=str, default=None, choices=['s3', 'gdrive'],  help="Cloud destination to mirror changes.")
    parser.add_argument('remote_source_root', type=str, default=None, help="Cloud location of the mirrored source.")
    parser.add_argument('remote_root', type=str, help="Root path of remote archive directory")
    parser.add_argument("--verbose", default=False, action="store_true", help="Display extra status information.")
    parser.add_argument('--report', type=str, default="report.txt", help="Location of a report file indicating any missing files. Defaults to report.txt.")
    parser.add_argument('--queue_url', type=str, default=None,  help="Host and port of a redis queue server.")
    parser.add_argument('--work_queue', type=str, default=None, help="Name of the queue to pull sub directories from.")
    parser.add_argument('--queue_batch', type=int, default=3, help="Number of subdirs to pull from the queue at one time.")
    parser.add_argument('--queue_timeout', type=int, default=30, help="Timeout to wait for items to populate the queue.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--subdirs", type=str, nargs="*", default=[], help="List of subdirectories of archive_dir to limit the search for files to.")
    parser.add_argument("--local", action="store_true", default=False, help="Running a local non-cloud test, do not delete old directories.")
    return parser


def main(args):
    extra_message_lines = []
    exit_status = 0
    start_time = datetime.datetime.now()
    messages = Messages()

    init_logging(args)
    try:

        dest_archive_root = RClonePath(args.rclone_conf, args.remote_dest, args.remote_root)
        remote_source_root = RClonePath(args.rclone_conf, args.remote_dest, args.remote_source_root)
        source_archive_root = Path(args.archive_dir)
        metadata_archives = create_metadata_archives(source_archive_root)
        archive = RemoteArchiveDir(source_archive_root, dest_archive_root, remote_source_root, metadata_archives)
        done_with_queue = True
        dirs_to_scan = []

        # Grab any directories from our arguments
        if len(args.subdirs) > 0:
            dirs_to_scan += [Path(source_archive_root) / subdir for subdir in args.subdirs]

        if args.queue_url is not None and args.work_queue is not None:
            done_with_queue = False
        
        
        while len(dirs_to_scan) > 0 or not done_with_queue:

            if len(dirs_to_scan) == 0:
                if not done_with_queue:
                    logger.info("Attempting to claim datasets")
                    claimed_datasets = claim_datasets(args, os.environ["POD_NAME"], True, args.queue_batch)
                    logger.info(f"Found: {claimed_datasets}")
                    download_datasets(args, claimed_datasets)
                    dirs_to_scan = [Path(source_archive_root, x) for x in claimed_datasets]
                    if len(dirs_to_scan) < args.queue_batch:
                        done_with_queue = True

            if len(dirs_to_scan) > 0:
                dir = dirs_to_scan.pop()
                dataset = str(dir.relative_to(source_archive_root))
                logger.info(f"Scanning {dir}")
                set_dataset_status(args, dataset, "IN_PROGRESS")
                messages += populate_archive(archive,source_archive_root,[dir])
                set_dataset_status(args, dataset, "COMPLETE")
                if not args.local:
                    shutil.rmtree(dir)



    except Exception as e:
        extra_message_lines += traceback.format_exc().splitlines()
        exit_status = 1

    end_time = datetime.datetime.now()

    timing_strings = [ "----------------------------------",
                      f"Started {start_time.isoformat()}",
                      f"Finished {end_time.isoformat() }",
                      f"Duration {end_time - start_time }"]
    all_extra_messages = extra_message_lines + timing_strings
    write_messages(args.report, messages, all_extra_messages)

    for s in all_extra_messages:
        logging.info(s)

    return exit_status

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))


