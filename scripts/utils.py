"""
"""
import os
import csv
import io
import sys
from pathlib import Path, PosixPath
from contextlib import contextmanager
import subprocess as sp
import time
import gspread_utils
import random
import logging
import logging.handlers

import configobj

from pypeit.inputfiles import PypeItFile, InputFile

logger = logging.getLogger(__name__)

def init_logging(logfile):
    """Sets up logging to logfile and log level, with a mirror of the output going to stderr"""

    # Format for logging to file
    formatter = logging.Formatter(fmt="{levelname:8} {asctime} {message}", style='{')

    formatter.converter=time.gmtime
    formatter.default_msec_format = "%s.%03d"

    # Configure a file handler to write detailed information to the log file
    file_handler = logging.handlers.WatchedFileHandler(logfile)
    file_handler.setLevel("DEBUG")
    file_handler.setFormatter(formatter)

    # Setup a basic formatter for output to stderr
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel("INFO")
    stream_handler.setFormatter(logging.Formatter())

    logging.basicConfig(handlers=[stream_handler, file_handler], force=True,  level="DEBUG")


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

    spreadsheet, worksheet, status_col = gspread_utils.open_spreadsheet(args.gsheet)
    if status_col is None:
        # Default to B if no status column was given
        status_col = "B"


    work_queue = gspread_utils.retry_gspread_call(lambda: worksheet.col_values(1))

    if len(work_queue) > 1:
        found = False
        for i in range(0, len(work_queue)):
            if work_queue[i].strip() == dataset:
                logger.info(f"Updating {dataset} status with {status}")
                gspread_utils.retry_gspread_call(lambda: worksheet.update(range_name=f"{status_col}{i+1}", values=[[status]]))
                found = True
                break
        if not found:
            logger.error(args, f"Did not find {dataset} to update status!")
    else:
        logger.error(args, f"Could not update {dataset}, spreadsheet is empty!")

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
            logger.info(f"Pod: {my_pod} has claimed dataset {dataset}")
        else:
            logger.info("Work Queue is empty")

    return dataset

def run_script(command, return_output=False, save_output=None, log_output=False):
    logger.debug(f"Running: '{' '.join(command)}'")

    if save_output is not None:
        with open(save_output, "w") as f:
            cp = sp.run(command, stdout=f, stderr=sp.STDOUT, encoding='UTF-8', errors='replace')

    elif return_output or log_output:       
        cp = sp.run(command, stdout=sp.PIPE, stderr=sp.STDOUT, encoding='UTF-8', errors='replace')

        if cp.returncode == 0:
            if log_output:
                for line in cp.stdout.splitlines():
                    logger.info(line)
            if return_output:
                return cp.stdout.splitlines()
    else:
        cp = sp.run(command)

    if cp.returncode != 0:
        if log_output:
            logger.error(f"Failed to run {command[0]}, return code: {cp.returncode}.")
            if cp.stdout is not None and len(cp.stdout) != 0:
                for line in cp.stdout.splitlines():
                    logger.error(line)
            else:
                logger.error(f"No output from {command[0]}")
                
        raise RuntimeError(f"Failed to run '{' '.join(command)}', return code: {cp.returncode}.")

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
            logger.error(f"Failed to update scorecard work queue status for {dataset}.", exc_info=True)

def backup_task_log(log_manager, backup_loc):
    # Cleanup the local task log
    try:
        log_manager.close()
        try:
            backup_loc.upload(log_manager.logfile)
        except Exception as e:
            logger.error("Failed to backup task logs", exc_info=True)
        log_manager.clear()
        log_manager.open()
    except Exception as e:
        logger.error("Failed to clean up and re-initialize task logs.", exc_info=True)
        # Treat an inability to access/clean up logs as fatal
        return


def run_task_on_queue(args, task):

    try:
        my_pod = os.environ["POD_NAME"]
        logger.info(f"Started on pod {my_pod} and python {sys.implementation}")

        dataset = claim_dataset(args, my_pod)

        if args.adap_root_dir is not None:
            root_dir = Path(args.adap_root_dir)
        else:
            root_dir = Path(".")
    except Exception as e:
        logger.error(f"Failed initializing.", exc_info=True)
        return

    # Go through the queue and run the task on each dataset
    while dataset is not None:
        status = 'COMPLETE'

        # Run the task
        try:
            status = task(args, dataset)
        except Exception as e:
            logger.error(f"Failed processing {dataset}.", exc_info=True)
            status = f'FAILED on {my_pod}'

        try:
            update_dataset_status(args, dataset, status)
        except Exception as e:
            logger.error(f"Failed to update dataset status for {dataset} to {status}.", exc_info=True)
        

        # Done with this dataset, move to the next
        try:
            dataset = claim_dataset(args, my_pod)
        except Exception as e:
            logger.error("Failed to claim dataset.", exc_info=True)
            dataset = None

def get_reduce_params(dataset_prefix):
    config_path = Path(__file__).parent.parent / "config"

    # Look for custom files of the dataset
    dataset_prefix_pattern = dataset_prefix.replace("/", "_") + "*"
    custom_files = list(config_path.glob(dataset_prefix_pattern))

    if len(custom_files) > 1:
        raise ValueError(f"Can't find reduce parameters for {dataset_prefix} because there was more than one custom parameter match")
    elif len(custom_files) == 0:
        # Just use the default
        param_file = config_path / "default_pypeit_config"
    else:
        param_file = custom_files[0]

    if param_file.suffix == ".pypeit":
        # A custom PypeIt File
        custom_pypeit_file = PypeItFile.from_file(param_file, preserve_comments=True)
        return custom_pypeit_file.config
    else:
        # A ini file. Read it and build a ConfigObj from it
        lines=list(InputFile.readlines(param_file))
        return configobj.ConfigObj(lines)

