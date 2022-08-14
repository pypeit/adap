"""
"""
import argparse
import os
import csv
import io
import shutil
import sys
from pathlib import Path
from contextlib import contextmanager
import subprocess as sp
import time
from datetime import datetime, timezone
import gspread

def log_message(args, msg):
    """Print a message to stdout and to a log file"""
    msg_with_time = datetime.now(timezone.utc).isoformat() + " " + msg
    with open(args.logfile, "a") as f:
        print(msg_with_time, file=f, flush=True)

    print(msg_with_time, flush=True)

def clear_log(args):
    os.remove(args.logfile)

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
    # Note need to retry with with 60s delay due to google rate limits
    success = False
    attempts = 1
    while not success:
        try:

            source_spreadsheet, source_worksheet = args.spreadsheet.split('/')

            # This relies on the service json in ~/.config/gspread
            account = gspread.service_account()

            # Get the spreadsheet from Google sheets
            spreadsheet = account.open(source_spreadsheet)

            # Get the worksheet
            worksheet = spreadsheet.worksheet(source_worksheet)

            work_queue = worksheet.col_values(1)

            if len(work_queue) > 1:
                found = False
                # Note first row will be the title "dataset"
                for i in range(1, len(work_queue)):
                    if work_queue[i].strip() == dataset:
                        log_message(args, f"Updating {dataset} status with {status}")
                        worksheet.update(f"B{i+1}", status)
                        found = True
                        break
                if not found:
                    log_message(args, f"Did not find {dataset} to update status!")
            else:
                log_message(args, f"Could not update {dataset}, spreadsheet is empty!")
            success = True
        except Exception as e:
            if attempts < 5:
                log_message(args, "Retrying Google API task")
                time.sleep(60)
                attempts += 1
            else:
                log_message(args, f"Too many retries updating worksheet. Exception: {e}")
                raise

def claim_dataset(args, my_pod):

    dataset = None

    with lock_workqueue(args.work_queue) as wq_file:
        csv_reader = csv.reader(wq_file, newline='\n')
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
            csv_writer = csv.writer(wq_file, newline='\n')
            csv_writer.writerows(rows)

            # Update the scorecard. This is done within the lock on the work queue
            # To prevent against race conditions accessing it
            update_gsheet_status(args.gsheet, dataset, "In Progress: " + my_pod)
            log_message(args, f"Pod: {my_pod} has claimed dataset {dataset}")
        else:
            log_message(args, "Work Queue is empty")

    return dataset

def run_script(command):
    cp = sp.run(command)
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to run '{' '.join(command)}'.")


def run_pypeit_onfile(file, arguments):
    """
    Run PypeIt on one pypeit file. It makes sure to change the currenct directory to that
    containing the passed in pypeit file.

    Args:
        file (Path):
            Path of the .pypeit file to run PypeIt on
        arguments(list of str):
            Arguments to run_pypeit_parallel from command line.

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

        # Run PypeIt on the pypeit file, using the additional arguments from our command line,
        # with stdout and stderr going to a text file, from the directory of the pypeit file, with
        # the environment set to be single threaded
        cp = sp.run(["run_pypeit", file.name] + arguments.pypeit_args, stdout=stdout_file, stderr=sp.STDOUT, cwd=pypeit_dir, env=child_env)

        if cp.returncode != 0:
            log_message(f"PypeIt returned non-zero status {cp.returncode}, setting status to FAILED")
            return "FAILED"

    return "COMPLETE"


def update_dataset_status(args, dataset, status):

    with lock_workqueue(args.work_queue) as wq_file:
        csv_reader = csv.reader(wq_file, newline='\n')
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
            csv_writer = csv.writer(wq_file, newline='\n')
            csv_writer.writerows(rows)

        update_gsheet_status(args, dataset, status)

        run_script(["python", "update_gsheet_scorecard.py", args.gsheet.split("/")[0], os.path.join(args.adap_root_dir, dataset, "complete", "reduce", "scorecard.csv"), args.scorecard_max_age])



def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.\n Authentication requres a "service_account.json" file in "~/.config/gspread/".')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument("--logfile", type=str, default="reduce_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--scorecard_max_age", type=int, default=7, help="Max age of items in the scorecard's latest spreadsheet")
    args = parser.parse_args()

    my_pod = os.environ["POD_NAME"]

    dataset = claim_dataset(args, my_pod)
    mask = dataset.split("/")[0]
    while dataset is not None:
        status = 'COMPLETE'
        try:
            run_script(["adap/download_dataset.sh", args.adap_root_dir, dataset])
            run_script(["python",  "adap/trimming_setup.py", "--adap_root_dir", args.adap_root_dir, mask])
        except Exception as e:
            log_message(f"Failed during prepwork for {dataset}. Exception {e}")
            status = 'FAILED'

        if status != 'FAILED':
            try:
                for pypeit_file in Path(args.adap_root_dir).rglob("*.pypeit"):
                    if run_pypeit_onfile(pypeit_file, "-o") == 'FAILED':
                        status = 'FAILED'

                scorecard_cmd = ["python", "adap/scorecard.py", args.adap_root_dir, os.path.join(args.adap_root_dir, dataset, "complete", "reduce", f"scorecard.csv"), "--status", status]
                if 'PYPEIT_COMMIT' in os.environ:
                    scorecard_cmd += ["--commit", os.environ['PYPEIT_COMMIT']]

                run_script(scorecard_cmd)
            except Exception as e:
                log_message(f"Failed processing {dataset}. Exception {e}")
                status = 'FAILED'

        # Try to upload any results regardless of status
        try:            
            run_script(["adap/upload_results.sh", args.adap_root_dir, dataset, args.logfile])
            # This will upload the log for the dataset, so clear it to keep it from growing forever
            clear_log(args)

        except Exception as e:
            log_message(f"Failed uploading results for {dataset}. Exception: {e}")
            status = 'FAILED'

        update_dataset_status(args, dataset, status)

        # Done with this dataset, more to the next
        dataset = claim_dataset(args, my_pod)

    log_message("No more datasets in queue, exiting")
    return 0

if __name__ == '__main__':    
    sys.exit(main())

