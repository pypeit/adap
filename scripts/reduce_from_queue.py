"""
"""
import argparse
import os
import csv
import io
import sys
from pathlib import Path, PosixPath
from contextlib import contextmanager
import subprocess as sp
import time
from datetime import datetime, timezone
import traceback
import random
import shutil
import psutil
from utils import claim_dataset, update_dataset_status
import cloudstorage
import metadata_info

def log_message(args, msg):
    """Print a message to stdout and to a log file"""
    msg_with_time = datetime.now(timezone.utc).isoformat() + " " + msg
    with open(args.logfile, "a") as f:
        print(msg_with_time, file=f, flush=True)

    print(msg_with_time, flush=True)

def clear_log(args):
    try:
        os.remove(args.logfile)
    except FileNotFoundError as e:
        print(f"{args.logfile} not found")

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


def retry_cloud(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except Exception:
            if i == len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
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

        log_message(args, f"Starting PypeIt run on {file}")
    
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

    log_message(args, f"PypeIt Max Memory Usage for {file}: {max_mem}")
    if returncode != 0:
        log_message(args, f"PypeIt returned non-zero status {returncode}, setting status to FAILED")
        return "FAILED", max_mem
    else:
        log_message(args, f"PypeIt returned successful status.")

    return "COMPLETE", max_mem




def cleanup_old_results(args, s3_storage, dataset, reduce_dir):
    """Back up old results from a previous reduction of this dataset."""
    log_message(args, f"Cleaning up old results...")
    s3_reduce_dir = f"pypeit/adap/raw_data_reorg/{dataset}/complete/{reduce_dir}/"
    
    try:
        for (url, size) in s3_storage.list_objects(s3_reduce_dir):

            try:
                retry_cloud(lambda: s3_storage.delete(url))
            except Exception as e:
                # If the copy fails, do not do the delete, but still continue on to the next item
                log_message(args, f"Failed to remove: {url}: {e}")
                
    except Exception as e:
        # If this fails, we still want to continue as we don't want to lose the current results
        log_message(args, f"Failed to backup results: {e}")

def download_dataset(args, s3_storage, dataset):
    dataset_raw_path = f"{dataset}/complete/raw/"
    local_path = Path(args.adap_root_dir) / dataset_raw_path 
    remote_source = f"pypeit/adap_2020/raw_data_reorg/{dataset_raw_path}"
    os.makedirs(local_path, exist_ok=True)
    count = 0
    for (url, size) in s3_storage.list_objects(remote_source):
        try:
            start_time = time.time()
            retry_cloud(lambda: s3_storage.download(url, local_path))
            end_time = time.time()
            log_message(args, f"Downloaded {url} in {end_time-start_time:.2f} s ({float(size*8)/(10**6*(end_time-start_time)):.2f} Mb/s)")
            count+=1
        except Exception as e:
            log_message(args, f"Failed to download {url}, error: {e}")
            raise
    return count

def upload_results(args, s3_storage, dataset):
    dataset_local_path = Path(args.adap_root_dir) / dataset / "complete"
    dataset_remote_path = f"pypeit/adap_2020/raw_data_reorg/{dataset}/complete"
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
                    start_time = time.time()
                    size = file.stat().st_size
                    retry_cloud(lambda: s3_storage.upload(file, dest), retry_delays = [30, 120, 300, 90])
                    end_time = time.time()
                    log_message(args, f"Uploaded {file} in {end_time-start_time:.2f} s ({float(size*8)/(10**6*(end_time-start_time)):.2f} Mb/s)")
                except Exception as e:
                    log_message(args, f"Failed to upload {file}, error: {e}")
                    failed = True

    if failed:
        raise RuntimeError("Failed to upload results.")

def backup_log(args, s3_storage, dataset):
    # Upload the log for this run
    log_destfile = f"pypeit/adap_2020/raw_data_reorg/{dataset}/complete/reduce/{Path(args.logfile).name}"
    log_message(args, f"Uploading log to s3 {log_destfile}.")
    try:
        retry_cloud(lambda: s3_storage.upload(args.logfile, log_destfile))
    except Exception as e:
        log_message(args, f"Failed to upload log to s3://{log_destfile}, error: {e}")


def backup_results_to_gdrive(args, dataset):
    # Upload the results for this run to Google Drive
    dataset_local_path = Path(args.adap_root_dir) / dataset / "complete"
    dataset_gdrive_path = PosixPath("backups") / dataset / "complete"
    dataset_backup_paths = list(dataset_local_path.glob("reduce*"))
    if args.backup_raw:
        dataset_backup_paths.append(dataset_gdrive_path / "raw")

    if len(dataset_backup_paths) == 0:
        log_message(args, f"No results backup to Google Drive.")
    else:
        log_message(args, f"Backing up results to Google Drive.")
        for reduce_path in dataset_backup_paths:
            reduce_folder = reduce_path.name
            gdrive_dest = "gdrive:" + str(dataset_gdrive_path / reduce_folder)
            log_message(args, f"Backing up {reduce_path} to {gdrive_dest} .")
            run_script(['rclone', '--config', Path(args.adap_root_dir) / "adap" / "config" / "rclone.conf", 'copy', '-P', '--stats-one-line', '--stats', '60s', '--stats-unit', 'bits', reduce_path, gdrive_dest])


def cleanup(args, dataset):
    """Clean up after running a job and uploading its results"""
    # Clear the log so it doesn't grow forever. upload_results will have uploaded it to S3
    clear_log(args)

    shutil.rmtree(Path(args.adap_root_dir) / dataset)
    #shutil.move(Path(args.adap_root_dir) / dataset, Path("/media/dusty/novus/adap_2020/raw_data_reorg_backup") / dataset)

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument("--logfile", type=str, default="reduce_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--backup_raw", default=False, action="store_true", help="Whether to also backup the raw data to google drive.")
    parser.add_argument("--scorecard_max_age", type=int, default=7, help="Max age of items in the scorecard's latest spreadsheet")
    parser.add_argument("--endpoint_url", type=str, default = os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io"), help="The URL used to access S3. Defaults $ENDPOINT_URL, or the PRP Nautilus external URL.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("pypeit_args", type=str, nargs="*", default=["-o"], help="Arguments to pass to run_pypeit")
    args = parser.parse_args()

    try:
        my_pod = os.environ["POD_NAME"]
        log_message(args, f"Started on pod {my_pod} and python {sys.implementation}")
        s3_storage = cloudstorage.initialize_cloud_storage("s3", args.endpoint_url)
        dataset = claim_dataset(args, my_pod)
        max_mem = 0
        while dataset is not None:
            status = 'COMPLETE'
            spec = metadata_info.dataset_to_spec(dataset)
            try:
                count = download_dataset(args, s3_storage, dataset)
                if count == 0:
                    log_message(args, f"No files found to download for {dataset}.")    
                    status = 'FAILED'
                else:
                    run_script(["python",  os.path.join(args.adap_root_dir, "adap", "scripts", "trimming_setup.py"), "--adap_root_dir", args.adap_root_dir, spec, dataset])
            except Exception as e:
                log_message(args, f"Failed during prepwork for {dataset}. Exception {e}")
                status = 'FAILED'

            if status != 'FAILED':
                try:
                    for pypeit_file in (Path(args.adap_root_dir)/dataset).rglob("*.pypeit"):

                        # Run PypeIt
                        status, max_mem = run_pypeit_onfile(args, pypeit_file)

                        # Find warnings in log file
                        #logfile = pypeit_file.parent / "keck_deimos_A.log"
                        #run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "useful_warnings.py"), str(logfile), "--req_warn_file", os.path.join(args.adap_root_dir, "adap", "config", "required_warnings.txt")])

                        # Cleanup results from an old run
                        cleanup_old_results(args, s3_storage, dataset, pypeit_file.parent.parent.name)

                        backup_log(args, s3_storage, dataset)

                        #backup_logs_to_gdrive(args, gdrive_storage, dataset, pypeit_file.parent.parent.name)

                        run_script(["bash", os.path.join(args.adap_root_dir, "adap", "scripts", "tar_qa.sh"), str(pypeit_file.parent)])

                    scorecard_cmd = ["python", os.path.join(args.adap_root_dir, "adap", "scripts", "scorecard.py"), spec, args.adap_root_dir, os.path.join(args.adap_root_dir, dataset, "complete", "reduce", f"scorecard.csv"), "--status", status, "--mem", str(max_mem)]
                    if 'PYPEIT_COMMIT' in os.environ:
                        scorecard_cmd += ["--commit", os.environ['PYPEIT_COMMIT']]

                    run_script(scorecard_cmd)


                except Exception as e:
                    log_message(args, f"Failed processing {dataset}. Exception {e}")
                    status = 'FAILED'

            # Try to upload any results regardless of status
            try:            
                upload_results(args, s3_storage, dataset)
            except Exception as e:
                log_message(args, f"Failed uploading results for {dataset}. Exception: {e}")
                status = 'FAILED'

            # Try to backup results to gdrive
            try:            
                backup_results_to_gdrive(args, dataset)
            except Exception as e:
                log_message(args, f"Failed backup up results for {dataset}. Exception: {e}")
                if status != 'FAILED':
                    status = "WARNING"

            # Update the work queue status
            update_dataset_status(args, dataset, status)

            # Update the scorecard results
            try:
                run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "update_gsheet_scorecard.py"), args.gsheet.split("/")[0], os.path.join(args.adap_root_dir, dataset, "complete", "reduce", "scorecard.csv"), str(args.scorecard_max_age)])
            except Exception as e:
                log_message(args, f"Failed to update scorecard results for {dataset}. Exception {e}")
            
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

