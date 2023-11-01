import argparse
import logging
import sys
import os
from pathlib import Path
import shutil

from pykoa.koa import Koa 


logger = logging.getLogger(__name__)

from astropy.table import Table
import numpy as np
from rclone import RClonePath
from utils import run_task_on_queue, run_script, init_logging

def parse_reorg_contents(reorg_contents, observing_config):

    observing_config_raw_path = Path(observing_config) / "complete" / "raw"
    with open(reorg_contents, "r") as f:
        for line in f:
            if len(line.strip()) > 0:
                p = Path(line.strip()).relative_to("raw_data_reorg")
                if p.name.endswith(".fits"):
                    if p.parent == observing_config_raw_path:
                        yield p.name

def get_pykoa_metadata(koa_output_local, raw_files, raw_files_koa_metadata):
    all_metadata = Table.read(koa_output_local,format="csv")
    indx = [id in raw_files for id in all_metadata['koaid']]
    all_metadata[indx].write(str(raw_files_koa_metadata),format="csv", overwrite=True)

def stage_task(args, observing_config):

    root_path = Path(args.adap_root_dir)
    config_path = Path(observing_config)
    local_config_path = root_path / observing_config
    dest_loc = None
    s3_pypeit_root = RClonePath(args.rclone_conf, "s3", "pypeit", "adap_2020")
    try:
        if args.dest == "gdrive":
            dest_loc = RClonePath(args.rclone_conf, "gdrive", "backups", observing_config)
        else:
            dest_loc = s3_pypeit_root /  "raw_data_reorg" 

        dest_config_path = dest_loc / observing_config
        dest_raw_data_path = dest_config_path / "complete" / "raw"
        
        logger.info(f"Removing any prior uploads.")
        # Remove prior results
        try:
            dest_raw_data_path.unlink()
        except Exception as e:
            # This could be failing due to the directory not existing so we ignore this.
            logger.warning(f"Failed to remove prior results in {dest_loc}")
            pass

        logger.info(f"Identifying raw files.")
        reorg_contents = s3_pypeit_root / "raw_data_reorg_contents.txt"
        local_reorg_contents = root_path / "raw_data_reorg_contents.txt"
        if not local_reorg_contents.exists():
            reorg_contents.download(root_path)

        instrument = config_path.parents[-2].name # -1 is ".", -2 is the first directory
        raw_files = list(parse_reorg_contents(local_reorg_contents, observing_config))
        if len(raw_files) == 0:
            raise ValueError(f"No raw files found for {observing_config}")
        
        logger.info(f"Get the KOA metadata for the {len(raw_files)} raw_files")
        koa_output_filename = f"{instrument.lower()}_all_semid_progpi.csv"
        koa_output_local = root_path / koa_output_filename
        if not koa_output_local.exists():
            koa_output_source = s3_pypeit_root / "koa_output" / koa_output_filename
            koa_output_source.download(root_path)
        raw_files_koa_metadata = root_path / "raw_files_koa.csv"
        get_pykoa_metadata(koa_output_local, raw_files, raw_files_koa_metadata) 

        logger.info("Downloading files from KOA")
        Koa.download(str(raw_files_koa_metadata), "csv", str(local_config_path), calibfile=0)

        logger.info(f"Uploading files to {dest_loc}")
        # Note KOA puts files into a "lev0" directory
        dest_raw_data_path.upload(local_config_path / "lev0")



    finally:
        
        # Always try to upload results and logs even on failures
        if not args.test and dest_loc is not None:
            # Upload the results
            if local_config_path.exists():
                logger.info(f"Uploading output to {dest_loc}")
                dest_raw_data_path.upload(local_config_path)

            # Upload our logs
            logfile = root_path / args.logfile
            if logfile.exists():
                logger.info(f"Uploading {logfile}")
                dest_config_path.upload(logfile)

                # Remove our log file so it doesn't grow too large over the lifetime of the pod
                logfile.unlink()
            else:
                logger.warning(f"Logfile {logfile} does not exist?")


        # Cleanup local data to keep the pod storage from growing too large
        if not args.test:
            logger.info(f"Removing local data in {local_config_path}")
            shutil.rmtree(local_config_path,ignore_errors=True)

    return "COMPLETE"

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('dest', type=str, help="Where to place the data, either 's3' or 'gdrive'.")
    parser.add_argument("--logfile", type=str, default="stage_raw_data_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--test", action="store_true", default = False, help="Run in test config, which does not download data if it already is present and doesn't clear data when done.")
    args = parser.parse_args()

    try:
        init_logging(Path(args.adap_root_dir, args.logfile))
        run_task_on_queue(args, stage_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

