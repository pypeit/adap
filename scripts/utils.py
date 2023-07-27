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
import gspread
import random
import shutil
import logging 
from fnmatch import fnmatch

logger = logging.getLogger(__name__)

class LogManager:
    
    
    def __init__(self, logfile):
        """Sets up logging to logfile and log level, with a mirror of the output going to stderr"""
        self.logfile = logfile

        # Format for logging to file
        self.formatter = logging.Formatter(fmt="{levelname:8} {asctime} {message}", style='{')

        self.formatter.converter=time.gmtime
        self.formatter.default_msec_format = "%s.%03d"

        # Configure a file handler to write detailed information to the log file
        self.open()

        # Setup a basic formatter for output to stderr
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel("INFO")
        stream_handler.setFormatter(logging.Formatter())

        logger.setLevel("DEBUG")
        logger.addHandler(stream_handler)

    def close(self):
        self._file_handler.close()
        logger.removeHandler(self._file_handler)

    def clear(self):
        os.unlink(self.logfile)

    def open(self):
        self._file_handler = logging.FileHandler(self.logfile)
        self._file_handler.setLevel("DEBUG")
        self._file_handler.setFormatter(self.formatter)
        logger.addHandler(self._file_handler)

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


def retry_gspread_call(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if i < len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
        except:
            # an exception type we don't want to retry
            raise
        
        # A failure happened, sleep before retrying
        signal_proof_sleep(retry_delays[i] + random.randrange(1, retry_jitter+1))


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

    # This relies on a service account json
    account = retry_gspread_call(lambda: gspread.service_account(filename=args.google_creds))

    # Get the spreadsheet from Google sheets
    spreadsheet = retry_gspread_call(lambda: account.open(source_spreadsheet))

    # Get the worksheet
    worksheet = retry_gspread_call(lambda: spreadsheet.worksheet(source_worksheet))

    work_queue = retry_gspread_call(lambda: worksheet.col_values(1))

    if len(work_queue) > 1:
        found = False
        for i in range(0, len(work_queue)):
            if work_queue[i].strip() == dataset:
                logger.info(f"Updating {dataset} status with {status}")
                retry_gspread_call(lambda: worksheet.update(f"B{i+1}", status))
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

def run_script(command, capture_output=False, log_output=False):
    logger.debug(f"Running: '{' '.join(command)}'")
    if capture_output or log_output:       
        cp = sp.run(command, stdout=sp.PIPE, stderr=sp.STDOUT, encoding='UTF-8', errors='replace')

        if cp.returncode == 0:
            if log_output:
                for line in cp.stdout.splitlines():
                    logger.info(line)
            if capture_output:
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



class RCloneLocation():
    def __init__(self, args, service, path_template, glob=False, recursive=False, exclude=None):
        self.service=service
        if service not in ['s3', 'gdrive']:
            raise ValueError(f"Unknown service {service}")
        self.path_template = path_template
        self.glob=glob
        self.root_dir = Path(args.adap_root_dir)
        self.cloud_root_dirs = {"s3": "pypeit/adap/raw_data_reorg/",
                                "gdrive": "backups/"}
        self.rclone_config = args.rclone_conf
        self.recursive = recursive
        self.exclude=exclude

    def ls(self, path):
        paths_to_search = [Path(path)]
        combined_results = []
        while len(paths_to_search) != 0:
            path = paths_to_search.pop()
            results = run_script(["rclone", '--config', self.rclone_config, 'lsf', self.service + ":" + self.cloud_root_dirs[self.service] + str(path)], capture_output=True)
            for result in results:                    
                if self.recursive and results.endswith("/"):
                    paths_to_search.append(path / result)
                combined_results.append(path / result)
        return combined_results

    def copy(self, source, dest):
        # Run rclone copy with nice looking progress
        run_script(["rclone", '--config', self.rclone_config,  'copy', '-P', '--stats-one-line', '--stats', '60s', '--stats-unit', 'bits', str(source), str(dest)])

    def rm(self, path):
        run_script(["rclone", '--config', self.rclone_config,  'delete', str(path)], log_output=True)


    def expand_path_template(self, dataset):
        expanded_paths = []
        path = Path(self.path_template.format(dataset))

        if self.glob:
            pattern = path.name
            for file in self.ls(path.parent):
                logger.debug(f"Testing {file.name} against {pattern}")
                if fnmatch(file.name, pattern):
                    exclude = False
                    if self.exclude is not None:
                        for exclude_pattern in self.exclude:
                            logger.debug(f"Testing {file.name} against exclude pattern {exclude_pattern}")
                            if fnmatch(file.name, exclude_pattern):
                                exclude=True
                                break
                    
                    if not exclude:
                        expanded_paths.append(file)

        else:
            expanded_paths.append(path)
        return expanded_paths

    def expand_local_path_template(self, dataset):
        path = Path(self.path_template.format(dataset))
        if self.glob:
            if self.recursive:
                results = list(path.parent.rglob(path.name))
            else:
                results = list(path.parent.glob(path.name))
        else:
            results = [path]

        if self.exclude is not None:
            filtered_results = []
            for result in results:
                for exclude_pattern in self.exclude:
                    exclude=False
                    if fnmatch(result.name, exclude_pattern):
                        exclude = True
                        break
                if not exclude:
                    filtered_results.append(result)
            return filtered_results
        else:
            return results


    def download(self, dataset):
        
        relative_paths = self.expand_path_template(dataset)
        if len(relative_paths) == 0:
            logger.warning(f"No data to download for {dataset}")
            return relative_paths
        else:        
            for path in relative_paths:
                logger.info(f"Downloading {path}")
                self.copy(self.service + ":" + self.cloud_root_dirs[self.service] + str(path), self.root_dir / path)

            return relative_paths

    def cleanup(self, dataset):
        relative_paths = self.expand_path_template(dataset)
        if len(relative_paths) == 0:
            logger.info(f"No data to cleanup for {dataset}")
        else:
            for path in relative_paths:
                logger.info(f"Cleaning up {path}")
                self.rm(self.service + ":" + self.cloud_root_dirs[self.service] + str(path))

    def upload(self, dataset):
        relative_paths = self.local_path_template(dataset)
        if len(relative_paths) == 0:
            logger.warning(f"No data to upload for {dataset}")
        else:
            for path in relative_paths:
                logger.info(f"Uploading {path}")
                self.copy(self.root_dir / path, self.service + ":" + self.cloud_root_dirs[self.service] + str(path))

    def upload_file(self, local_path, dest_path):
        self.copy(str(local_path), self.service + ":" + self.cloud_root_dirs[self.service] + str(dest_path))

def run_task_on_queue(args, task, source_loc, dest_loc, backup_loc, cleanup, log_backup_loc=None):

    try:
        log_manager = LogManager(args.logfile)

        if log_backup_loc is None:
            log_backup_loc = backup_loc

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

        downloaded_paths = []
        # Download data the task may need
        try:
            if source_loc is not None:
                downloaded_paths = source_loc.download(dataset)
                
        except Exception as e:
            logger.error(f"Failed downloading source data for {dataset}.", exc_info=True)
            status = 'FAILED'

        # Run the task
        if status != 'FAILED':
            try:
                status = task(args, dataset)
            except Exception as e:
                logger.error(f"Failed processing {dataset}.", exc_info=True)
                status = 'FAILED'

        # Cleanup old results, if needed
        if cleanup and dest_loc is not None:
            try:
                # Cleanup results from an old run
                dest_loc.cleanup(dataset)
            except Exception as e:
                logger.error(f"Failed to cleanup old results for dataset{dataset}.", exc_info=True)

        # Upload any results. We do this regardless of status because partial results can help debug
        # failures
        try:            
            if dest_loc is not None:
                dest_loc.upload(dataset)
        except Exception as e:
            logger.error(f"Failed uploading results for {dataset}.", exc_info=True)
            status = 'FAILED'

        # Backup results
        try:            
            if backup_loc is not None:
                backup_loc.upload(dataset)
        except Exception as e:
            logger.error(f"Failed backup up results for {dataset}.", exc_info=True)
            if status != 'FAILED':
                status = "WARNING"

        try:
            update_dataset_status(args, dataset, status)
        except Exception as e:
            logger.error(f"Failed to update dataset status for {dataset} to {status}.", exc_info=True)
        
        # Cleanup locally before moving to the next dataset
        try:
            for local_path in downloaded_paths:
                shutil.rmtree(Path(root_dir) / local_path)
        except Exception as e:
            logger.error(f"Failed to clean up local storage for {dataset}.")

        # Cleanup the local task log
        try:
            log_manager.close()
            try:
                log_backup_loc.upload_file(log_manager.logfile, Path(dataset, "complete"))
            except Exception as e:
                logger.error("Failed to backup task logs", exc_info=True)
            log_manager.clear()
            log_manager.open()
        except Exception as e:
            logger.error("Failed to clean up and re-initialize task logs.", exc_info=True)
            # Treat an inability to access/clean up logs as fatal
            return

        # Done with this dataset, move to the next
        try:
            dataset = claim_dataset(args, my_pod)
        except Exception as e:
            logger.error("Failed to claim dataset.", exc_info=True)
            dataset = None
