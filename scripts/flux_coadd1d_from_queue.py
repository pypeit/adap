import argparse
import logging
import sys
import os
from pathlib import Path
import shutil
import contextlib

logger = logging.getLogger(__name__)

import numpy as np
from astropy.table import Table

from pypeit.inputfiles import FluxFile, Coadd1DFile
from pypeit.specobjs import SpecObjs


from utils import run_task_on_queue, run_script, init_logging
from rclone import get_cloud_path
import metadata_info


def run_flux_coadd1d_task(args, dataset_prefix):
    root_path = Path(args.adap_root_dir)
    spec_name = metadata_info.dataset_to_spec(dataset_prefix)

    # Download data from either s3 or google as requested
    s3_loc = get_cloud_path(args, "s3")
    gdrive_loc = get_cloud_path(args, "gdrive")
    dest_locs =  [gdrive_loc, s3_loc]

    status = "COMPLETE"

    if args.source == "s3":
        source_loc = s3_loc / dataset_prefix
    elif args.source == "gdrive":
        source_loc = gdrive_loc / dataset_prefix
    else:
        logger.error(f"Unrecognized source: {args.source}")
        return "FAILED"

    try:
        logger.info(f"Searching for reduce paths under {source_loc}")
        reduce_paths = list(source_loc.rglob("reduce*"))

        # Make sure there's data 
        if len(reduce_paths) == 0:
            logger.error(f"No reduce paths found for {dataset_prefix}")
            return "FAILED"
        
        # Download the reduced data
        local_reduce_paths = []
        for reduce_path in reduce_paths:
            relative_path = reduce_path.path.relative_to(source_loc.path)
            local_path = root_path / dataset_prefix / relative_path
            logger.info(f"Downloading data to {local_path}")
            reduce_path.download(local_path)
            local_reduce_paths.append(local_path)
    
            # Try to create backups of any spec1ds
            logger.info(f"Looking for spec1ds that need to be backed up")
            science_relative_path = Path(dataset_prefix, relative_path ,"Science")
            local_science_path = root_path / science_relative_path
        
            spec1ds_to_backup = local_science_path.glob("spec1d_*.fits")
            for spec1d in spec1ds_to_backup:
                backup_name = "orig_" + spec1d.name
                local_backup = spec1d.parent / backup_name
                if not local_backup.exists():
                    # It has not been backed up, create a remote backup
                    for dest in dest_locs:
                        remote_backup = dest / science_relative_path / backup_name
                        try:
                            logger.info(f"Backing up spec1d to {remote_backup}")
                            remote_backup.upload(spec1d)
                        except Exception as e:
                            logger.errpr(f"Failed to backup {spec1d.name}",exc_info=True)
                            return "FAILED"
                            
        # Create 1D_Coadd directory to flux/coadd from
        relative_coadd_path = Path(dataset_prefix, "1D_Coadd")
        local_coadd_dir = root_path / relative_coadd_path
        local_coadd_dir.mkdir(exist_ok=True)

        # Run everything with the coadd dir as the current directory.
        with contextlib.chdir(local_coadd_dir):
            # Filenames for pypeit_flux_setup outputs and the coadding output
            dataset_file_name = "_".join(dataset_prefix.replace("/", "_").split("_")[1:])
            flux_file_name =  f"{spec_name}.flux"
            coadd_file_name = f"{spec_name}.coadd1d"
            coadd_output_name = f"coadd1d_{dataset_file_name}.fits"

            logger.info("Running pypeit_flux_setup...")
            run_script(["pypeit_flux_setup", "--name", spec_name, "--skip_standards", "--recursive", "--coadd_output", coadd_output_name, str(root_path / dataset_prefix)], save_output=str(local_coadd_dir / "flux_setup_output.txt"))

            # Now flux
            try:
                logger.info("Running pypeit_flux_calib...")
                output_file = local_coadd_dir / "pypeit_flux_calib_output.txt"
                run_script(["pypeit_flux_calib", flux_file_name], save_output=output_file)
            except Exception as e:
                logger.error(f"Failed to run pypeit_flux_calib on {flux_file_name}.", exc_info=True)
                status = "FAILED"
                
            try:
                logger.info("Running pypeit_coadd_1dspec...")
                output_file = local_coadd_dir / "pypeit_coadd_1dspec_output.txt"
                run_script(["pypeit_coadd_1dspec", coadd_file_name], save_output=output_file)
            except Exception as e:
                logger.error(f"Failed to run pypeit_coadd_1dspec on {coadd_file_name}.", exc_info=True)
                status = "FAILED"


        for dest_loc in dest_locs:
            dest_coadd_dir = dest_loc / relative_coadd_path
            # Cleanup old results in the cloud
            try:
                dest_coadd_dir.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete old results in {dest_coadd_dir}, continuing anyway")

            # Upload 1D_Coadd dir
            try:
                dest_coadd_dir.upload(local_coadd_dir)
            except Exception as e:
                logger.error(f"Failed to upload results to {dest_coadd_dir}.")
                status = "FAILED"

            # Upload fluxed spec1ds
            for local_reduce_path in local_reduce_paths:
                spec1d_files = local_reduce_path.rglob("spec1d_*.fits")
                for spec1d_file in spec1d_files:
                    relative_path = spec1d_file.relative_to(root_path)
                    dest_spec1d = dest_loc / relative_path

                    try:
                        logger.info(f"Uploading {spec1d_file}")
                        dest_spec1d.upload(spec1d_file)
                    except Exception as e:
                        logger.error(f"Failed to upload {spec1d_file}", exc_info=True)
                        status = "FAILED"
    except Exception as e:
        status = "FAILED"
        logger.error("Failed flux/coadd task.", exc_info=True)

    finally:
        # Always clean up local data to avoid filling up space.
        if args.local:
            logger.info("Not cleaning up on local test run")
        else:
            logger.info(f"Cleaning up local copy of {dataset_prefix}")
            shutil.rmtree(str(root_path / dataset_prefix))
    
    return status




def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--queue_timeout', type = int,default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="flux_coadd1d_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--local", default = False, action="store_true", help="Run in local test config, which does not download data if it already is present and does not cleanup when done.")
    args = parser.parse_args()

    init_logging(args.logfile)

    try:
        run_task_on_queue(args, run_flux_coadd1d_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

