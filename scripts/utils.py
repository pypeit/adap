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


# The work queue lock is held across Google Sheets calls, and retry_gspread_call can sleep
# for around four minutes before giving up, so the lease has to comfortably outlast that.
# It does have to expire, though: with redis-py's default of no timeout, a pod killed
# between taking the lock and releasing it wedges every other pod permanently, and nothing
# in any log says why. A bounded blocking_timeout matters for the same reason -- a waiter
# that gives up raises LockError, which the callers log, instead of hanging forever.
WORKQUEUE_LOCK_TIMEOUT = 600
WORKQUEUE_LOCK_WAIT = 600


def lock_named_queue(redis_server, queue_name):
    """
    Take the lock belonging to a named work queue.

    Stages that write to a sheet owned by a *different* queue need that queue's lock
    rather than their own -- the KOA download stage runs off the target queue but appends
    rows to the dataset queue's tab, and only the dataset queue's lock guards it.
    """
    return redis_server.lock(queue_name + "_lock",
                             timeout=WORKQUEUE_LOCK_TIMEOUT,
                             blocking_timeout=WORKQUEUE_LOCK_WAIT)


def lock_workqueue(redis_server, args):
    """
    Open the work queue with a lock to prevent race conditions between pods running in parallel.
    This function can be used as a context manager, keeping the queue locked within a "with" block.

    Args:
    redis_server (redis.Redis): The redis server hosting the queue.
    args: The parsed command line, which supplies the work queue name.

    Returns:
    A redis lock, usable as a context manager.

    """
    return lock_named_queue(redis_server, args.work_queue)


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


def claim_next(args, my_pod, blocking=False):
    """
    Claim the next item from the queue, initializing the queue if the init sentinel
    turns up.

    The sentinel used to be recognised only on a pod's very first claim. Popped at any
    other time it was handed to the task as though it were a real item, so a pod already
    working the queue would try to process a dataset called "init", fail it, and leave
    the queue unseeded. The loop below also covers the case of two sentinels next to each
    other in the queue.
    """
    item = claim_dataset(args, my_pod, blocking=blocking)

    while item == "init":
        init_work_queue(args)
        item = claim_dataset(args, my_pod)

    return item


def run_task_on_queue(args, task):

    try:
        my_pod = os.environ["POD_NAME"]
        logger.info(f"Started on pod {my_pod} and python {sys.implementation}")

        dataset = claim_next(args, my_pod, blocking=True)

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
            dataset = claim_next(args, my_pod)
        except Exception as e:
            logger.error("Failed to claim dataset.", exc_info=True)
            dataset = None
        
    logger.info("No more datasets in queue, exiting")

# How long a pod will wait for a KOA download slot before giving up. Long enough to sit
# behind a genuinely slow target, short enough that a leaked token surfaces as a clear
# failure in the log rather than a pod that hangs until the Job's deadline.
KOA_SLOT_WAIT = 3600


def seed_koa_slots(redis_server, key, max_concurrent):
    """
    Create the pool of KOA download slots, once.

    The obvious test -- does the pool key exist yet? -- is wrong. Redis deletes a list key
    when its last element is popped, so a pool with every slot checked out is
    indistinguishable from one that was never created, and a pod arriving at that moment
    would seed a second set of tokens and silently double the limit. A separate marker
    key records that the pool has been created and survives the pool being emptied.

    SET NX is atomic, so of two pods starting together exactly one seeds the pool and the
    other goes straight to waiting for a token from it.
    """
    marker = key + "_seeded"

    if redis_server.set(marker, max_concurrent, nx=True):
        tokens = [f"slot{i + 1}" for i in range(max_concurrent)]
        redis_server.rpush(key, *tokens)
        logger.info(f"Seeded {key} with {max_concurrent} KOA download slots")


@contextmanager
def koa_download_slot(args):
    """
    Hold one of a limited number of KOA download slots for the duration of a "with" block.

    KOA should not be asked to serve many simultaneous requests. Koa.download is a serial
    loop over the frames in a table, so one pod holds exactly one connection at a time and
    the number of running pods *is* the concurrency -- which makes the parallelism field
    of the Job the primary throttle. This semaphore is the backstop for what parallelism
    cannot see: a second download Job, or a hand-run retry, alongside the first.

    Set --max_koa_downloads to 0 to disable it and rely on parallelism alone.

    A pod killed while holding a token leaks it, permanently lowering the limit. That is
    why the wait is bounded: the pod fails with a message naming the fix rather than
    hanging. Re-seeding after a leak means clearing the pool *and* its marker::

        kubectl exec $POD -- redis-cli del adap_2023_targets_koa_slots \
                                          adap_2023_targets_koa_slots_seeded
    """
    max_concurrent = getattr(args, "max_koa_downloads", 0)

    if max_concurrent is None or max_concurrent <= 0:
        yield
        return

    key = args.work_queue + "_koa_slots"

    redis_server = redis.Redis.from_url(args.queue_url, decode_responses=True)
    redis_server.ping()

    seed_koa_slots(redis_server, key, max_concurrent)

    logger.info(f"Waiting for a KOA download slot from {key}")
    token = redis_server.blpop(key, timeout=KOA_SLOT_WAIT)

    if token is None:
        raise RuntimeError(
            f"No KOA download slot available from {key} after {KOA_SLOT_WAIT}s. "
            f"If no other download is running, a token was leaked by a pod that died "
            f"holding one; clear the pool with "
            f"'redis-cli del {key} {key}_seeded' and the next pod will re-seed it.")

    # blpop returns (key, value)
    slot = token[1]
    logger.info(f"Holding KOA download slot {slot}")

    try:
        yield slot
    finally:
        try:
            redis_server.rpush(key, slot)
            logger.info(f"Released KOA download slot {slot}")
        except Exception:
            logger.error(f"Failed to release KOA download slot {slot}; it is leaked "
                         f"until {key} is deleted.", exc_info=True)


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

