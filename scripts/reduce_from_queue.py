"""
"""
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
import subprocess as sp
import shutil
import psutil
from extended_spec_mixins import get_lris_spec_name

from utils import run_task_on_queue, init_logging, run_script
from rclone import get_cloud_path

import logging
logger = logging.getLogger(__name__)


def clear_log(args):
    try:
        logfile = args.adap_root_dir / args.logfile
        os.remove(logfile)
    except FileNotFoundError as e:
        print(f"{logfile} not found")


def run_pypeit_onfile(args, file):
    """
    Run PypeIt on one pypeit file. It makes sure to change the current directory to that
    containing the passed in pypeit file.

    Args:
        args():
            Arguments to reduce_from_queue as returned by argparse.
        file (Path):
            Path of the .pypeit file to run PypeIt on

    Returns:
        str : The status of the pypeit run. Either "COMPLETED" or "FAILED".
        int : The maximum memory used by PypeIt.
    """

    pypeit_dir = file.parent
    stdout = pypeit_dir.joinpath("run_pypeit_stdout.txt")
    with open(stdout, "w") as stdout_file:
        child_env = os.environ.copy()
        # Set the OMP_NUM_THREADS to 1 to prevent numpy multithreading from competing for resources
        # with the multiple processes started by this script
        child_env['OMP_NUM_THREADS'] = '1'

        logger.info(f"Starting PypeIt run on {file}")

        # Run PypeIt on the pypeit file, using the additional arguments from our command line,
        # with stdout and stderr going to a text file, from the directory of the pypeit file, with
        # the environment set to be single threaded
        cp = sp.Popen(["run_pypeit", file.name] + args.pypeit_args, stdout=stdout_file, stderr=sp.STDOUT, cwd=pypeit_dir, env=child_env)
        #cp = sp.run(["python", f"{args.adap_root_dir}/adap/scripts/mem_profile_pypeit", str(pypeit_dir / "cloud_develop"), file.name, "-o"], stdout=stdout_file, stderr=sp.STDOUT, cwd=pypeit_dir, env=child_env)

        returncode = None
        process = psutil.Process(cp.pid)

        max_mem = 0

        while returncode is None:
            # Try to get memory usage information for the child,
            # ignore errors if we can't
            try:
                mem = process.memory_full_info().uss
                if max_mem < mem:
                    max_mem = mem
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass
            # Wait 2 seconds for the child before checking the memory again
            try:
                returncode = cp.wait(2)
            except sp.TimeoutExpired:
                # Normal, means the child didn't finish within the 2 second timeout
                pass

    logger.info(f"PypeIt Max Memory Usage for {file}: {max_mem}")
    if returncode != 0:
        logger.error(f"PypeIt returned non-zero status {returncode}, setting status to FAILED")
        return "FAILED", max_mem
    else:
        logger.info(f"PypeIt returned successful status.")

    return "COMPLETE", max_mem



def cleanup_old_results(args, dataset, reduce_dir):
    """Clean up old results from a previous reduction of this dataset."""
    logger.info(f"Cleaning up old results...")


    source_reduce_dir = get_cloud_path(args, "s3") / Path(dataset, reduce_dir)

    for file in source_reduce_dir.ls(recursive=True):
        try:
            file.unlink()
        except Exception as e:
            # If the delete fails, continue to next item
            logger.error(f"Failed to remove: {file}",exc_info=True)



def download_dataset(args, dataset):
    # Download data 
    relative_path = Path(dataset, "raw")

    source_loc = get_cloud_path(args, args.source) / relative_path
    local_path = args.adap_root_dir / relative_path

    source_loc.download(local_path)
    count = len(list(local_path.glob("*")))
    logger.info(f"Downloaded {count} raw files for {dataset}.")
    return count

def upload_results(args, dest, dataset):
    local_reduce_path = args.adap_root_dir / dataset / "reduce"
    remote_reduce_path = get_cloud_path(args, dest) / dataset / "reduce"
    failed = False

    logger.info(f"Uploading results in {local_reduce_path} to {remote_reduce_path}...")

    try:
        remote_reduce_path.upload(local_reduce_path)
    except Exception as e:
        logger.error(f"Failed to upload {local_reduce_path}",exc_info=True)
        failed = True

    if failed:
        raise RuntimeError("Failed to upload results.")

def backup_log(args, dataset):
    # Upload the log for this run

    log_sourcefile = args.adap_root_dir / args.logfile
    log_dest = get_cloud_path(args, "s3") / Path(dataset, "reduce", args.logfile)

    logger.info(f"Uploading log to {log_dest}.")
    try:
        log_dest.upload(log_sourcefile)
    except Exception as e:
        logger.error(f"Failed to upload log to {log_dest}", exc_info=True)


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

def reduce_dataset_task(args, dataset):

    max_mem = 0
    status = 'COMPLETE'
    dataset_path = Path(dataset)
    try:
        target, date_str, color_dir = dataset_path.parts
        obs_date = datetime.strptime(date_str, "%Y%m%d").date()
        if color_dir == "blue":
            instrument = "LRISBLUE"
        else:
            instrument = "LRIS"
        spec = get_lris_spec_name(obs_date = obs_date, instrument=instrument)
    except Exception as e:
        logger.error(f"Failed parsing dataset name {dataset}.", exc_info=True)
        status = 'FAILED'

    if status != 'FAILED':
        scripts_dir = args.adap_root_dir / "adap" / "scripts"
        try:
            count = download_dataset(args, dataset)
            if count == 0:
                logger.error(f"No files found to download for {dataset}.")    
                status = 'FAILED'
            else:
                run_script(["python",  str(scripts_dir / "trimming_setup.py"), "--adap_root_dir", str(args.adap_root_dir), spec, dataset])
        except Exception as e:
            logger.error(f"Failed during prepwork for {dataset}.",exc_info=True)
            status = 'FAILED'


    if status != 'FAILED':
        try:
            # Run each pypeit file
            for pypeit_file in (args.adap_root_dir/dataset).rglob("*.pypeit"):

                # Run PypeIt
                status, max_mem = run_pypeit_onfile(args, pypeit_file)

                # Find warnings in log file
                #logfile = pypeit_file.parent / "keck_deimos_A.log"
                #run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "useful_warnings.py"), str(logfile), "--req_warn_file", os.path.join(args.adap_root_dir, "adap", "config", "required_warnings.txt")])

                # Cleanup results from an old run
                cleanup_old_results(args, dataset, "reduce")

                backup_log(args, dataset)

                run_script(["bash", str(scripts_dir / "tar_qa.sh"), str(pypeit_file.parent)])

            scorecard_cmd = ["python", str(scripts_dir / "scorecard.py"), spec, str(args.adap_root_dir), str(args.adap_root_dir / dataset / "reduce" / f"scorecard.csv"), "--status", status, "--mem", str(max_mem)]
            if 'PYPEIT_COMMIT' in os.environ:
                scorecard_cmd += ["--commit", os.environ['PYPEIT_COMMIT']]

            run_script(scorecard_cmd)

        except Exception as e:
            logger.error(f"Failed processing {dataset}", exc_info=True)
            status = 'FAILED'

        # Try to upload any results regardless of status
        try:            
            upload_results(args, "s3", dataset)
        except Exception as e:
            logger.error(f"Failed uploading results for {dataset} to s3.",exc_info=True)
            status = 'FAILED'

        # Try to backup results to gdrive
        try:            
            upload_results(args, "gdrive", dataset)
        except Exception as e:
            logger.error(f"Failed copying results for {dataset} to gdrive.", exc_info=True)
            if status != 'FAILED':
                status = "WARNING"

        # Update the scorecard results
        try:
            run_script(["python", str(scripts_dir / "update_gsheet_scorecard.py"), args.gsheet.split("/")[0], str(args.adap_root_dir / dataset / "reduce" / "scorecard.csv"), str(args.scorecard_max_age)])
        except Exception as e:
            logger.error(f"Failed to update scorecard results for {dataset}.",exc_info=True)

    # Cleanup before moving to the next dataset
    cleanup(args, dataset)
    return status

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--queue_timeout', type = int,default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="reduce_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=Path, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--scorecard_max_age", type=int, default=7, help="Max age of items in the scorecard's latest spreadsheet")
    parser.add_argument("--endpoint_url", type=str, default = os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io"), help="The URL used to access S3. Defaults $ENDPOINT_URL, or the PRP Nautilus external URL.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("pypeit_args", type=str, nargs="*", default=["-o"], help="Arguments to pass to run_pypeit")
    parser.add_argument("--local", type=Path, default = None, help="Run in local test config, which does not download data if it already is present and moves data to a given directory when done.")
    args = parser.parse_args()

    try:
        init_logging(args.adap_root_dir / args.logfile)
        run_task_on_queue(args, reduce_dataset_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())
