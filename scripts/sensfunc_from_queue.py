"""
"""
import argparse
import os
import sys
from pathlib import Path
import subprocess as sp
import shutil

from astropy.table import Table
import numpy as np

from pypeit.inputfiles import PypeItFile, SensFile

import metadata_info

from utils import run_task_on_queue, init_logging, run_script
from rclone import get_cloud_path

import logging
logger = logging.getLogger(__name__)


def clear_log(args):
    try:
        os.remove(args.logfile)
    except FileNotFoundError as e:
        print(f"{args.logfile} not found")



def cleanup_old_results(args, cloud, dataset, reduce_dir):
    """Clean up old results from a previous reduction of this dataset."""
    logger.info(f"Cleaning up old results...")

    source_reduce_dir = get_cloud_path(args, cloud) / Path(dataset, reduce_dir)
    
    for file in source_reduce_dir.glob("sens*"):
        try:
            file.unlink()
        except Exception as e:
            # If the delete fails, continue to next item
            logger.error(f"Failed to remove: {file}",exc_info=True)
                


def download_dataset(args, dataset):
    # Download dataset reduce results
    source_loc = get_cloud_path(args, args.source)
    reduce_path = source_loc / dataset / "reduce"

    relative_path = reduce_path.path.relative_to(source_loc.path)
    local_path = args.adap_root_dir / relative_path 

    reduce_path.download(local_path)

def get_standards(pypeit_file):
    """
    Return the filenames of the standard frames within a .pypeit file.
    
    Args:
        pypeit_file (Path or str): Path of the pypeit file

    Return (list of str):
        List of standard files, not including the path.
    """
    pf = PypeItFile.from_file(pypeit_file)
    standard_index = pf.data['frametype'] == 'standard'
    return list(pf.data[standard_index]['filename'])

def find_spec1d_file(pypeit_path, raw_data_file_name):
    """Find a spec1d file for a given raw data file.
    
    Args:
        pypeit_path (Path): The path of the pypeit file used for the reduction.
        raw_data_file_name (str): The name of the raw data file

    Return (Path):
        The path of the spec1d file generated from the raw data file.
    """
    # Remove any .fits or .fits.gz suffix
    raw_data_stem=  raw_data_file_name.split(".fits")[0]

    # Find spec1d files that contain the raw data file's base name
    spec1d_files = list((pypeit_path / 'Science').glob(f"spec1d_{raw_data_stem}*.fits"))
    if len(spec1d_files) == 1:
        return spec1d_files[0]
    elif len(spec1d_files) > 1:
        raise ValueError(f"Found multiple spec1d files for standard {raw_data_file_name}")
    else:
        raise ValueError(f"Could not find spec1d for standard file {raw_data_file_name}")

def get_senfunc_args(args, dataset, file_name):
    # Look up the sensfunc arguments from out config file
    config_file = args.adap_root_dir / "adap" / "config" / "sensfunc_config.ecsv"
    t = Table.read(config_file,format="ascii.ecsv")

    # Fill any empty values with empty strings
    config_table = t.filled(fill_value="")

    # First look for a specific file config
    indx =  config_table["id"] == file_name
    
    # If there wasn't a specific file config, look for the dataset, or a prefix of the dataset
    dataset_prefix = dataset
    while not np.any(indx) and len(dataset_prefix) >0:

        indx = config_table["id"] == dataset_prefix
        if not np.any(indx):
            # No match, trim off the last portion to make a new prefix
            dataset_prefix = "/".join(dataset_prefix.split("/")[0:-1])

    if not np.any(indx):
        # Couldn't find one based on the dataset either, use the default
        indx = config_table["id"] == "DEFAULT"
        if not np.any(indx):
            raise ValueError(f'Could not find "DEFAULT" entry in config file {config_file}')
            
    config_row = config_table[indx][0]

    # Build the command line arguments based on the config row
    sensfunc_args = []    
    for name in config_row.colnames:
        if name == "id":
            # Not an actual argument, just the id used to match a dataset or file
            continue
        elif config_row.dtype[name] == 'bool':
            # Boolean argument
            if config_row[name]:
                sensfunc_args += ["--" + name]
        elif config_row[name] != "":
            # String argument
            sensfunc_args += ["--" + name, str(config_row[name])]
 
    return sensfunc_args

def run_pypeit_sensfunc(args, spec1d_file, sensfunc_args):
    """
    Run pypeit_sensfunc. 

    Args:
        args:
            Arguments to reduce_from_queue as returned by argparse.
        spec1d_file (Path):
            Path of the spec1d file of a standard star observation.
        sens_file (Path):
            Path to a sens func configuration file.

    """
    logger.info(f"Running pypeit_sensfunc on {spec1d_file.name}")
    pypeit_dir = spec1d_file.parent.parent
    stdout_file = pypeit_dir.joinpath(f"sensfunc_{spec1d_file.stem}_stdout.txt")
    output_file = pypeit_dir / spec1d_file.name.replace("spec1d_", "sens_")
    run_script(["pypeit_sensfunc", str(spec1d_file), "-o", str(output_file)] + sensfunc_args, save_output=stdout_file)

def upload_results(args, cloud, dataset):
    dataset_local_path = args.adap_root_dir / dataset 
    dataset_remote_path = get_cloud_path(args, cloud) / dataset
    reduce_path = dataset_local_path / "reduce"

    failed = False

    pypeit_dir = reduce_path
    dest_path = dataset_remote_path / reduce_path.name
    logger.info(f"Uploading results in {pypeit_dir} to {dest_path}...")

    for sensfunc_result in pypeit_dir.glob("sens*"):
        try:
            dest_file = dest_path / sensfunc_result.name
            dest_file.upload(sensfunc_result)
        except Exception as e:
            logger.error(f"Failed to upload {sensfunc_result}",exc_info=True)
            failed = True

    if failed:
        raise RuntimeError("Failed to upload results.")

def backup_log(args, dataset):
    # Upload the log for this run

    log_sourcefile = Path(args.logfile)
    log_dest = get_cloud_path(args, "s3") / Path(dataset, "reduce")
    if log_sourcefile.exists():
        logger.info(f"Uploading log to {log_dest}.")
        try:
            log_dest.upload(log_sourcefile)
        except Exception as e:
            logger.error(f"Failed to upload log to {log_dest}", exc_info=True)
    else:
        logger.info(f"No log to backup.")

def cleanup(args, dataset):
    """Clean up after running a job and uploading its results"""

    try:
        # Clear the log so it doesn't grow forever. upload_results will have uploaded it to S3
        clear_log(args)
        
        if args.local is None:
            shutil.rmtree(Path(args.adap_root_dir) / dataset)
        else:
            dest_dir = args.local / dataset
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            else:
                os.makedirs(dest_dir.parent,exist_ok=True)
            shutil.move(Path(args.adap_root_dir) / dataset, dest_dir.parent)
    except FileNotFoundError:
        logger.debug(f"Failed to cleanup {dataset}")

def gen_sensfunc_task(args, dataset):

    status = 'COMPLETE'
    try:
        download_dataset(args, dataset)
    except Exception as e:
        logger.error(f"Failed during prepwork for {dataset}.",exc_info=True)
        status = 'FAILED'


    if status != 'FAILED':
        try:
            # For each pypeit file
            for pypeit_file in (args.adap_root_dir/dataset).rglob("*.pypeit"):

                # load the dataset to get a list of standards
                standard_files = get_standards(pypeit_file)
                logger.info(f"Found {len(standard_files)} standard files.")
                for standard_file in standard_files:
                    spec1d_file = find_spec1d_file(pypeit_file.parent, standard_file)
                    sensfunc_args = get_senfunc_args(args, dataset, spec1d_file.name)
                    try:
                        run_pypeit_sensfunc(args, spec1d_file, sensfunc_args)
                    except Exception as e:
                        logger.error(f"Failed to gen sensfunc for {standard_file}. Continuing to next standard. ")
                        status = "FAILED"
                
            if status != "FAILED":
                # Cleanup results from an old run
                cleanup_old_results(args, "s3", dataset, "reduce")
                cleanup_old_results(args, "gdrive", dataset, "reduce")

            backup_log(args, dataset)


        except Exception as e:
            logger.error(f"Failed processing {dataset}", exc_info=True)
            status = 'FAILED'

        # Try to upload any results regardless of status
        try:            
            upload_results(args, "s3", dataset)
        except Exception as e:
            logger.error(f"Failed uploading results for {dataset}.",exc_info=True)
            status = 'FAILED'

        # Try to backup results to gdrive
        try:            
            upload_results(args, "gdrive", dataset)
        except Exception as e:
            logger.error(f"Failed backup up results for {dataset}.", exc_info=True)
            if status != 'FAILED':
                status = "WARNING"

       
    # Cleanup before moving to the next dataset
    cleanup(args, dataset)
    return status

def main():
    parser = argparse.ArgumentParser(description='Generate a sensfunc for datasets on a work queue..')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--queue_timeout', type = int,default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="sensfunc_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=Path, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--endpoint_url", type=str, default = os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io"), help="The URL used to access S3. Defaults $ENDPOINT_URL, or the PRP Nautilus external URL.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--local", type=Path, default = None, help="Run in local test config, which does not download data if it already is present and moves data to a given directory when done.")
    args = parser.parse_args()

    try:
        init_logging(args.adap_root_dir / args.logfile)
        run_task_on_queue(args, gen_sensfunc_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

