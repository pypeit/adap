"""
"""
import argparse
import os
import csv
import io
import sys
from pathlib import Path
from contextlib import contextmanager
import subprocess as sp
import time
from datetime import datetime, timezone
import gspread
import traceback
import random
import shutil

import cloudstorage

def log_message(args, msg):
    """Print a message to stdout and to a log file"""
    msg_with_time = datetime.now(timezone.utc).isoformat() + " " + msg
    with open(args.logfile, "a") as f:
        print(msg_with_time, file=f, flush=True)

    print(msg_with_time, flush=True)

def clear_log(args):
    os.remove(args.logfile)

def retry_gspread_call(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if i == len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
            time.sleep(retry_delays[i] + random.randrange(1, retry_jitter+1))

def retry_cloud(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except Exception:
            if i == len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
            time.sleep(retry_delays[i] + random.randrange(1, retry_jitter+1))

@contextmanager
def lock_workqueue(work_queue_file):
    """
    Open the work queue with a file lock to prevent race conditions between pods running in parallel.
    This function can be used as a context manager, keeping the file locked within a "with" block.

    Args:
    work_queue_file(str or pathlib.Path):  The full path to the work queue file.

    Returns:
    file-like object For the work queue file.

    """
    fd = os.open(work_queue_file, os.O_RDWR)
    # The documentation is unclear if this can return -1 like the underlying C call, so I test for it
    if fd == -1:
        raise RuntimeError("Failed to open file")

    # Lock the file
    os.lockf(fd, os.F_LOCK, 0)

    # Return a nice file like object to wrap the file descriptor
    file_object_wrapper = open(fd, "r+", closefd=False)
    try:
        yield file_object_wrapper
    finally:
        # Presumably the file object's close would also release the lock, but I didn't trust it so 
        # I closed it and the file descriptor separately
        file_object_wrapper.close()
        os.close(fd)


def update_gsheet_status(args, dataset, status):
    # Note need to retry Google API calls due to rate limits
    source_spreadsheet, source_worksheet = args.gsheet.split('/')

    # This relies on the service json in ~/.config/gspread
    account = retry_gspread_call(lambda: gspread.service_account())

    # Get the spreadsheet from Google sheets
    spreadsheet = retry_gspread_call(lambda: account.open(source_spreadsheet))

    # Get the worksheet
    worksheet = retry_gspread_call(lambda: spreadsheet.worksheet(source_worksheet))

    work_queue = retry_gspread_call(lambda: worksheet.col_values(1))

    if len(work_queue) > 1:
        found = False
        for i in range(0, len(work_queue)):
            if work_queue[i].strip() == dataset:
                log_message(args, f"Updating {dataset} status with {status}")
                retry_gspread_call(lambda: worksheet.update(f"B{i+1}", status + "-" + os.getenv("POD_NAME", "unknown pod")))
                found = True
                break
        if not found:
            log_message(args, f"Did not find {dataset} to update status!")
    else:
        log_message(args, f"Could not update {dataset}, spreadsheet is empty!")

def claim_dataset(args, my_pod):

    dataset = None

    with lock_workqueue(args.work_queue) as wq_file:
        csv_reader = csv.reader(wq_file)
        rows = []
        found=False        
        for row in csv_reader:
            rows.append(row)
            if not found and row[1] == 'IN QUEUE':
                row[1] = my_pod
                dataset = row[0]
                found = True

        if found:
            # Rewrite the work queue file with the new info
            wq_file.truncate(0)
            wq_file.seek(0,io.SEEK_SET)
            csv_writer = csv.writer(wq_file)
            csv_writer.writerows(rows)

            # Update the scorecard. This is done within the lock on the work queue
            # To prevent against race conditions accessing it
            update_gsheet_status(args, dataset, "In Progress: " + my_pod)
            log_message(args, f"Pod: {my_pod} has claimed dataset {dataset}")
        else:
            log_message(args, "Work Queue is empty")

    return dataset

def run_script(command):
    cp = sp.run(command)
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to run '{' '.join(command)}', return code: {cp.returncode}.")


def run_pypeit_onfile(args, file):
    """
    Run PypeIt on one pypeit file. It makes sure to change the currenct directory to that
    containing the passed in pypeit file.

    Args:
        args():
            Arguments to reduce_from_queue as returned by argparse.
        file (Path):
            Path of the .pypeit file to run PypeIt on

    Returns:
        Popen : Popen object created when creating the child process to run PypeIt.
    """
       
    pypeit_dir = file.parent
    stdout = pypeit_dir.joinpath("run_pypeit_stdout.txt")
    with open(stdout, "w") as stdout_file:
        child_env = os.environ.copy()
        # Set the OMP_NUM_THREADS to 1 to prevent numpy multithreading from competing for resources
        # with the multiple processes started by this script
        child_env['OMP_NUM_THREADS'] = '1'

        log_message(args, f"Starting PypeIt run on {file}")
    
        # Run PypeIt on the pypeit file, using the additional arguments from our command line,
        # with stdout and stderr going to a text file, from the directory of the pypeit file, with
        # the environment set to be single threaded
        cp = sp.run(["run_pypeit", file.name, "-o"], stdout=stdout_file, stderr=sp.STDOUT, cwd=pypeit_dir, env=child_env)

        if cp.returncode != 0:
            log_message(args, f"PypeIt returned non-zero status {cp.returncode}, setting status to FAILED")
            return "FAILED"
        else:
            log_message(args, f"PypeIt returned successful status.")

    return "COMPLETE"


def update_dataset_status(args, dataset, status):

    with lock_workqueue(args.work_queue) as wq_file:
        csv_reader = csv.reader(wq_file)
        rows = []
        found=False        
        for row in csv_reader:
            rows.append(row)
            if not found and row[0] == dataset:
                row[1] = status
                found = True

        if found:
            wq_file.truncate(0)
            wq_file.seek(0,io.SEEK_SET)
            csv_writer = csv.writer(wq_file)
            csv_writer.writerows(rows)

        try:
            update_gsheet_status(args, dataset, status)
        except Exception as e:
            log_message(args, f"Failed to update scorecard work queue status for {dataset}. Exception {e}")

        try:
            run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "update_gsheet_scorecard.py"), args.gsheet.split("/")[0], os.path.join(args.adap_root_dir, dataset, "complete", "reduce", "scorecard.csv"), str(args.scorecard_max_age)])
        except Exception as e:
            log_message(args, f"Failed to update scorecard results for {dataset}. Exception {e}")


def backup_old_results(args, dataset, reduce_dir):
    """Back up old results from a previous reduction of this dataset."""
    log_message(args, f"Backing up old results...")
    s3_reduce_dir = f"s3://pypeit/adap/raw_data_reorg/{dataset}/complete/{reduce_dir}/"
    s3_backup_dir = f"s3://pypeit/adap/raw_data_reorg/{dataset}/complete/{reduce_dir}_old/"
    
    try:
        for (url, size) in cloudstorage.list_objects(s3_reduce_dir):
            relative_path = url.removeprefix(s3_reduce_dir)
            try:
                retry_cloud(lambda: cloudstorage.copy(url, s3_backup_dir + relative_path))
            except Exception as e:
                # If the copy fails, do not do the delete, but still continue on to the next item
                log_message(args, f"Failed to backup: {url}, error: {e}")
                continue

            try:
                retry_cloud(lambda: cloudstorage.delete(url))
            except Exception as e:
                # If the copy fails, do not do the delete, but still continue on to the next item
                log_message(args, f"Failed to remove: {url}, after backup, error: {e}")
                
    except Exception as e:
        # If this fails, we still want to continue as we don't want to lose the current results
        log_message(args, f"Failed to backup results: {e}")

def download_dataset(args, dataset):
    dataset_raw_path = f"{dataset}/complete/raw/"
    local_path = Path(args.adap_root_dir) / dataset_raw_path 
    remote_source = f"s3://pypeit/adap/raw_data_reorg/{dataset_raw_path}"
    os.makedirs(local_path, exist_ok=True)

    for (url, size) in cloudstorage.list_objects(remote_source):
        try:
            retry_cloud(lambda: cloudstorage.download(url, local_path))
        except Exception as e:
            log_message(args, f"Failed to download {url}, error: {e}")
            raise

def upload_results(args, dataset):
    dataset_local_path = Path(args.adap_root_dir) / dataset / "complete"
    dataset_remote_path = f"s3://pypeit/adap/raw_data_reorg/{dataset}/complete"
    dataset_reduce_paths = list(dataset_local_path.glob("reduce*"))
    failed = False
    if len(dataset_reduce_paths) == 0:
        log_message(args, "No reduce results to upload.")
    else:
        for reduce_path in dataset_reduce_paths:
            log_message(args, f"Uploading results in {reduce_path} to {dataset_remote_path}...")
            for file in reduce_path.rglob('*'):
                if not file.is_file():
                    # Skip directories
                    continue
    
                dest = f"{dataset_remote_path}/{reduce_path.name}/{file.relative_to(reduce_path)}"
    
                try:
                    # Use longer retry delays to give results a good chance of beng uploaded
                    retry_cloud(lambda: cloudstorage.upload(file, dest), retry_delays = [30, 120, 300, 90])
                except Exception as e:
                    log_message(args, f"Failed to upload {file}, error: {e}")
                    failed = True

        # Upload the log for this run
        log_destfile = dataset_remote_path + "/reduce/" + Path(args.logfile).name
        log_message(args, f"Uploading log to {log_destfile}.")
        retry_cloud(lambda: cloudstorage.upload(args.logfile, log_destfile), retry_delays = [30, 120, 300, 90])


    if failed:
        raise RuntimeError("Failed to upload results.")


def cleanup(args, dataset):
    """Clean up after running a job and uploading its results"""
    # Clear the log so it doesn't grow forever. upload_results will have uploaded it to S3
    clear_log(args)

    shutil.rmtree(Path(args.adap_root_dir) / dataset)


def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.\n Authentication requres a "service_account.json" file in "~/.config/gspread/".')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument("--logfile", type=str, default="reduce_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--scorecard_max_age", type=int, default=7, help="Max age of items in the scorecard's latest spreadsheet")
    parser.add_argument("--endpoint_url", type=str, default = os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io"), help="The URL used to access S3. Defaults $ENDPOINT_URL, or the PRP Nautilus external URL.")
    args = parser.parse_args()
    try:
        my_pod = os.environ["POD_NAME"]
        cloudstorage.initialize_cloud_storage("s3://pypeit", args)
        dataset = claim_dataset(args, my_pod)
        while dataset is not None:
            mask = dataset.split("/")[0]
            status = 'COMPLETE'
            try:
                download_dataset(args, dataset)
                run_script(["python",  os.path.join(args.adap_root_dir, "adap", "scripts", "trimming_setup.py"), "--adap_root_dir", args.adap_root_dir, mask])
            except Exception as e:
                log_message(args, f"Failed during prepwork for {dataset}. Exception {e}")
                status = 'FAILED'

            if status != 'FAILED':
                try:
                    for pypeit_file in Path(args.adap_root_dir).rglob("*.pypeit"):

                        # Run PypeIt
                        if run_pypeit_onfile(args, pypeit_file) == 'FAILED':
                            status = 'FAILED'

                        # Find warnings in log file
                        logfile = pypeit_file.parent / "keck_deimos_A.log"
                        run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "useful_warnings.py"), str(logfile), "--req_warn_file", os.path.join(args.adap_root_dir, "adap", "config", "required_warnings.txt")])

                        # Backup any results from an old run
                        backup_old_results(args, dataset, pypeit_file.parent.parent.name)

                        run_script(["bash", os.path.join(args.adap_root_dir, "adap", "scripts", "tar_qa.sh"), str(pypeit_file.parent)])

                    scorecard_cmd = ["python", os.path.join(args.adap_root_dir, "adap", "scripts", "scorecard.py"), args.adap_root_dir, os.path.join(args.adap_root_dir, dataset, "complete", "reduce", f"scorecard.csv"), "--status", status, "--masks", mask]
                    if 'PYPEIT_COMMIT' in os.environ:
                        scorecard_cmd += ["--commit", os.environ['PYPEIT_COMMIT']]

                    run_script(scorecard_cmd)


                except Exception as e:
                    log_message(args, f"Failed processing {dataset}. Exception {e}")
                    status = 'FAILED'

            # Try to upload any results regardless of status
            try:            
                upload_results(args, dataset)
            except Exception as e:
                log_message(args, f"Failed uploading results for {dataset}. Exception: {e}")
                status = 'FAILED'

            update_dataset_status(args, dataset, status)
            
            # Cleanup before moving to the next dataset
            cleanup(args, dataset)

            # Done with this dataset, more to the next
            dataset = claim_dataset(args, my_pod)
        log_message(args, "No more datasets in queue, exiting")
    except:
        exc_lines = traceback.format_exc()
        log_message(args, "Exception caught in main, existing")        
        log_message(args, exc_lines)
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

