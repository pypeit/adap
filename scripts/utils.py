"""
"""
import os
import sys
from pathlib import Path
import subprocess as sp
import time
import gspread_utils
import logging
import logging.handlers

import configobj

from pypeit.inputfiles import PypeItFile, InputFile

import redis


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

def signal_proof_sleep(seconds):
    # I've noticed the time.sleep() function doesn't alway sleep as long as I want. My theory,
    # based on the docs, is that some network errors contacting S3/Google Drive cause a signal
    # which raises an exception. In any event this code make sure that the retries sleep for
    # the desired # of seconds.
    start_time = time.time()
    current_time = start_time
    while current_time < start_time + seconds:
        time.sleep(1)
        current_time = time.time()



def lock_workqueue(redis_server, args):
    """
    Open the work queue with a file lock to prevent race conditions between pods running in parallel.
    This function can be used as a context manager, keeping the file locked within a "with" block.

    Args:
    work_queue_file(str or pathlib.Path):  The full path to the work queue file.

    Returns:
    file-like object For the work queue file.

    """
    return redis_server.lock(args.work_queue + "_lock")


def update_gsheet_status(args, dataset, status, pod):
    # Note need to retry Google API calls due to rate limits
    spreadsheet, worksheet, status_col = gspread_utils.open_spreadsheet(args.gsheet)
    if status_col is None:
        # Default to B if no status column was given
        status_col = "B"
        
    status_index = gspread_utils.column_name_to_index(status_col)
    pod_col = gspread_utils.index_to_column_name(status_index+1)


    work_queue = gspread_utils.retry_gspread_call(lambda: worksheet.col_values(1))

    if len(work_queue) > 1:
        found = False
        for i in range(0, len(work_queue)):
            if work_queue[i].strip() == dataset:
                logger.info(f"Updating {dataset} status with {status}")
                gspread_utils.retry_gspread_call(lambda: worksheet.update(range_name=f"{status_col}{i+1}:{pod_col}{i+1}", values=[[status,pod]]))
                found = True
                break
        if not found:
            logger.error(args, f"Did not find {dataset} to update status!")
    else:
        logger.error(args, f"Could not update {dataset}, spreadsheet is empty!")

def claim_dataset(args, my_pod, blocking=False):

    dataset = None

    redis_server = redis.Redis.from_url(args.queue_url,decode_responses=True)
    redis_server.ping()

    if blocking:
        dataset = redis_server.brpop(args.work_queue + "_q", timeout = args.queue_timeout)
        # The blocking version of rpop returns the queuename and the dataset
        dataset = dataset[1] if dataset is not None else None
    else:
        dataset = redis_server.rpop(args.work_queue + "_q")

    if dataset is not None and dataset != "init":
        logger.info(f"Claimed dataset {dataset} for pod {my_pod}")
        with lock_workqueue(redis_server, args):
            update_gsheet_status(args, dataset, "In Progress", my_pod)


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

def update_dataset_status(args, dataset, status, pod):

    redis_server = redis.Redis.from_url(args.queue_url)
    redis_server.ping()

    with lock_workqueue(redis_server, args) as wq:
        try:
            update_gsheet_status(args, dataset, status, pod)
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

def init_work_queue(args):
    """Initialize the work queue from a Google Docs sheet"""

    logger.info("Initializing work queue")
    redis_server = redis.Redis.from_url(args.queue_url)
    redis_server.ping()

    queue_name = args.work_queue + "_q"

    with lock_workqueue(redis_server,args):
        spreadsheet, worksheet, col_name = gspread_utils.open_spreadsheet(args.gsheet)
        if col_name is None:
            # Default to B if no status column was given
            col_name = "B"

        status_col = gspread_utils.column_name_to_index(col_name)

        work_queue_datasets = worksheet.col_values(1)
        work_queue_status = worksheet.col_values(status_col)

        if len(work_queue_datasets) > 1:
            update_values = []
            start_row = 4
            end_row = len(work_queue_datasets)

            # Note first row will be the title "dataset"
            for i in range(start_row-1, len(work_queue_datasets)):
                if work_queue_datasets[i] is not None and len(work_queue_datasets[i].strip()) > 0:
                    # Only add datasets with blank statuses
                    if i >= len(work_queue_status) or work_queue_status[i].strip() == '':                     
                        # Add to queue
                        redis_server.lpush(queue_name, work_queue_datasets[i].strip())
                        # Update spreadsheet to indicate the item has ben queued
                        update_values.append(["IN QUEUE"])
                    else:
                        # If the status isn't blank, leave it as is
                        update_values.append([work_queue_status[i]])
                else:
                    update_values.append([None])
            
            worksheet.batch_update([{'range': f'{col_name}{start_row}:{col_name}{end_row}',
                                    'values': update_values}])


def run_task_on_queue(args, task):

    try:
        my_pod = os.environ["POD_NAME"]
        logger.info(f"Started on pod {my_pod} and python {sys.implementation}")

        dataset = claim_dataset(args, my_pod, blocking=True)

        if dataset == "init":
            dataset = None
            init_work_queue(args)
            dataset = claim_dataset(args, my_pod)

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
            status = f'FAILED'

        try:
            update_dataset_status(args, dataset, status,my_pod)
        except Exception as e:
            logger.error(f"Failed to update dataset status for {dataset} to {status}.", exc_info=True)
        

        # Done with this dataset, move to the next
        try:
            dataset = claim_dataset(args, my_pod)
        except Exception as e:
            logger.error("Failed to claim dataset.", exc_info=True)
            dataset = None
        
    logger.info("No more datasets in queue, exiting")

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

