import argparse
import logging
import sys
import os
from pathlib import Path
import shutil
from pypeit import msgs
from pypeit.spec2dobj import AllSpec2DObj;
from pypeit.io import fits_open
from pypeit.spectrographs.util import load_spectrograph
from pypeit.inputfiles import Coadd2DFile
from pypeit.utils import recursive_update

logger = logging.getLogger(__name__)

from astropy.table import Table
import numpy as np

from utils import run_task_on_queue, RClonePath, run_script, init_logging, get_reduce_params


def trimming_setup_task(args, dataset):

    root_path = Path(args.adap_root_dir)
    rel_complete_path = Path(dataset) / "complete"
    rel_raw_path = rel_complete_path / "raw"
    local_complete_path = root_path / rel_complete_path
    local_raw_path = root_path / rel_raw_path
    dest_loc = None
    bkup_loc = None
    try:
        if args.gdrive_bkup:
            bkup_loc = RClonePath(args.rclone_conf, "gdrive", "backups", rel_complete_path)

        # Download data 
        if args.source == "gdrive":
            dest_loc = RClonePath(args.rclone_conf, "gdrive", "backups", rel_complete_path)
        else:
            dest_loc = RClonePath(args.rclone_conf, "s3", "pypeit", "adap", "raw_data_reorg", rel_complete_path)

        source_loc = dest_loc / "raw"

        if not args.test or not local_raw_path.exists():
            local_raw_path.mkdir(parents=True, exist_ok=True)
            source_loc.download(local_raw_path)
        mask = rel_raw_path.parts[0]

        # Run trimming setup
        logger.info(f"Running coadd2d setup on {dataset}")
        run_script(["python", "scripts/trimming_setup.py", "--adap_root_dir", args.adap_root_dir, mask], save_output="trimming_setup.log")
    finally:
        
        # Always try to upload results and logs even on failures
        if not args.test and dest_loc is not None:
            # Upload the results
            generated_reduce_dirs = [file.parent.relative_to(local_complete_path) for file in local_complete_path.rglob("*.pypeit")]
            logger.info(f"Uploading {len(generated_reduce_dirs)} output files to {dest_loc}")
            for reduce_dir in generated_reduce_dirs:
                reduce_dest_loc = dest_loc / reduce_dir
                reduce_dest_loc.upload(local_complete_path / reduce_dir)
                # Backup if requested
                if bkup_loc is not None:
                    reduce_bkup_loc = bkup_loc / reduce_dir
                    reduce_bkup_loc.upload(local_complete_path / reduce_dir)

            # Upload our logs
            logfile = root_path / args.logfile
            if logfile.exists():
                logger.info(f"Uploading {logfile}")
                dest_loc.upload(logfile)

                # Remove our log file so it doesn't grow too large over the lifetime of the pod
                logfile.unlink()
            else:
                logger.warning(f"Logfile {logfile} does not exist?")


        # Cleanup local data to keep the pod storage from growing too large
        if not args.test:
            logger.info(f"Removing local data in {local_complete_path}")
            shutil.rmtree(local_complete_path,ignore_errors=True)

    return "COMPLETE"

def main():
    parser = argparse.ArgumentParser(description='Generate pypeit files for the datasets in the ADAP work queue.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--gdrive_bkup', action="store_true", default=False, help="Backup the pypeit files to google drive.")
    parser.add_argument("--logfile", type=str, default="coadd2d_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--test", action="store_true", default = False, help="Run in test config, which does not download data if it already is present and doesn't clear data when done.")
    args = parser.parse_args()

    try:
        init_logging(Path(args.adap_root_dir, args.logfile))
        run_task_on_queue(args, trimming_setup_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

