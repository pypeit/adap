import argparse
import logging
import sys
import os
from pathlib import Path
import shutil
from pypeit import msgs

logger = logging.getLogger(__name__)


from utils import run_task_on_queue, run_script, init_logging
from rclone import RClonePath, get_cloud_path

def filter_output(source_log, dest_log):
    with open(source_log, "r") as source:
        with open(dest_log, "w") as dest:
            for line in source:
                if "does not match version used to write your HDU" in line:
                    continue

                # Use existing pypmsgs code to clean out ansi escape sequences
                clean_line = msgs._cleancolors(line)
                dest.write(clean_line)

def collate1d_task(args, observing_config):

    root_path = Path(args.adap_root_dir)
    local_config_path = root_path / observing_config
    output_path = local_config_path / "1D_Coadd"
    dest_loc = None
    original_path = Path.cwd()
    spec1d_files = []

    try:
        # Download data 
        source_loc = get_cloud_path(args, args.source) / observing_config
        dest_loc = get_cloud_path(args, "s3") / observing_config / "1D_Coadd"
        backup_loc = get_cloud_path(args, "gdrive") / observing_config / "1D_Coadd"

        reduce_paths = list(source_loc.rglob("Science"))
        for reduce_path in reduce_paths:
            # Download the reduce path from s3
            relative_path = reduce_path.path.relative_to(source_loc.path)
            local_path = local_config_path / relative_path
            if not args.test or not local_path.exists():
                reduce_path.download(local_path)


        # Remove prior results
        try:
            if not args.test:
                dest_loc.unlink()
        except Exception as e:
            # This could be failing due to the directory not existing so we ignore this.
            logger.warning(f"Failed to remove prior results in {dest_loc}")
            pass


        logger.info("Creating 1D_Coadd dir")
        output_path.mkdir(exist_ok=True)    

        os.chdir(output_path)

        # Find the spec1d files, but ignore anything from the 2D Coadd
        spec1d_files = [file for file in local_config_path.rglob("spec1d_*.fits") if file.parent.name=="Science"]

        collate1d_command = ["pypeit_collate_1d", str(root_path / "adap/config/default.collate1d"), "--spec1d_files" ] + [str(file) for file in spec1d_files]

        # Run setup
        logger.info(f"Running collate 1d on {len(spec1d_files)} in {observing_config}")
        
        try:
            run_script(collate1d_command, save_output="pypeit_collate_1d.log")
        finally:
            # Always try to cleanup log
            if Path("pypeit_collate_1d.log").exists():
                # Remove ansi escape code and unwanted log messages from log
                filter_output("pypeit_collate_1d.log", "pypeit_collate_1d.log.txt")

    finally:
        
        # Make sure we return to the original current working directory and cleanup after ourselves.
        os.chdir(original_path)

        # Always try to upload results and logs even on failures
        if not args.test and dest_loc is not None:
            # Upload the results
            if output_path.exists():
                logger.info(f"Uploading output to {dest_loc}")
                dest_loc.upload(output_path)

                logger.info(f"Backing up output to {backup_loc}")
                backup_loc.upload(output_path)

            # Upload the spec1d files now that they've been fluxed
            logger.info(f"Uploading fluxed spec1ds.")
            spec1d_dest_loc = get_cloud_path(args, "s3") / observing_config
            for spec1d in spec1d_files:
                relative_path = spec1d.relative_to(local_config_path).parent
                spec1d_dest = spec1d_dest_loc / relative_path     
                spec1d_dest.upload(spec1d)

            logger.info(f"Backing up fluxed spec1ds.")
            spec1d_dest_loc = get_cloud_path(args, "gdrive") / observing_config
            for spec1d in spec1d_files:
                relative_path = spec1d.relative_to(local_config_path).parent
                spec1d_dest = spec1d_dest_loc / relative_path     
                spec1d_dest.upload(spec1d)

            # Upload our logs
            logfile = root_path / args.logfile
            if logfile.exists():
                logger.info(f"Uploading {logfile}")
                dest_loc.upload(logfile)

                # Remove our log file so it doesn't grow too large over the lifetime of the pod
                logfile.unlink()
            else:
                logger.warning(f"Logfile {logfile} does not exist?")


        # Cleanup local data to keep the pod storage from growing too large
        if not args.test:
            logger.info(f"Removing local data in {local_config_path}")
            shutil.rmtree(local_config_path,ignore_errors=True)

    return "COMPLETE"

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="Work queue name.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument('--queue_timeout', type = int,default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="collate1d_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--test", action="store_true", default = False, help="Run in test config, which does not download data if it already is present and doesn't clear data when done.")
    args = parser.parse_args()

    try:
        init_logging(Path(args.adap_root_dir, args.logfile))
        run_task_on_queue(args, collate1d_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

