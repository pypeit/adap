import argparse
import logging
import sys
import os
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

from astropy.table import Table

from utils import run_task_on_queue, run_script, RClonePath, init_logging


def run_scorecard_task(args, dataset):

    root_path = Path(args.adap_root_dir)

    # Download data from either s3 or google as requested
    if args.source == "s3":
        source_loc = RClonePath(args.rclone_conf, "s3", "pypeit", "adap", "raw_data_reorg", dataset, "complete")
    else:
        source_loc = RClonePath(args.rclone_conf, "gdrive", "backups", dataset, "complete")

    reduce_paths = list(source_loc.glob("reduce*"))
    for reduce_path in reduce_paths:

        # Download the reduce path from s3
        relative_path = Path(dataset, "complete", reduce_path.path.name)
        local_path = root_path / relative_path
        reduce_path.download(local_path)

    # Make sure there's data to make a scorecard out of
    if len(reduce_paths) == 0:
        logger.error(f"No reduce paths found for {dataset}")
        return "FAILED"
    
    # Make sure there's a "reduce" path 
    scorecard_path = root_path / dataset / "complete" / "reduce"
    if not scorecard_path.is_dir():
        logger.error(f"Couldn't find reduce path to place scorecard for {dataset}.")
        return "FAILED"

    # This process can't fill in all of the data available during reduction.
    # So if there's a pre-existing scorecard, load it and save those values
    scorecard_file = scorecard_path / "scorecard.csv"
    if scorecard_file.exists():
        orig_scorecard_data = Table.read(scorecard_file, format="csv")
        
        mem_usage = orig_scorecard_data['mem_usage'][0]
        status = orig_scorecard_data['status'][0]
        git_commit = orig_scorecard_data['git_commit'][0]
        date_reduced = orig_scorecard_data['date'][0]
    else:
        mem_usage = 0
        status='COMPLETE'
        git_commit = None
        date_reduced = None
        
    # Build command, adding the commit if available
    mask = dataset.split("/")[0]
    scorecard_cmd = ["python", os.path.join(args.adap_root_dir, "adap", "scripts", "scorecard.py"), args.adap_root_dir, str(scorecard_file), "--status", status, "--mem", str(mem_usage), "--masks", mask]
    if git_commit is not None:
        scorecard_cmd += ['--commit', git_commit]

    if date_reduced is not None:
        scorecard_cmd += ['--date_reduced', date_reduced]

    logger.info(f"Running scorecard on {dataset}")
    run_script(scorecard_cmd)

    # Upload scorecard output to the same place we downloaded the data from
    if args.source == "s3":
        dest_loc = RClonePath(args.rclone_conf, "s3", "pypeit", "adap", "raw_data_reorg", dataset, "complete", "reduce")
    else:
        dest_loc = RClonePath(args.rclone_conf, "gdrive", "backups", dataset, "complete", "reduce")

    for file in scorecard_path.glob("scorecard*.csv"):
        dest_loc.upload(file)

    # Update the scorecard in google sheets. We set the maximum age to 10,000 because we don't want to change the 
    # latest tab, as this task doesn't re-run any pypeit reductions anyway.
    logger.info(f"Updating scorecard spreadsheet on {dataset}")
    run_script(["python", os.path.join(args.adap_root_dir, "adap", "scripts", "update_gsheet_scorecard.py"), args.gsheet.split("/")[0], os.path.join(args.adap_root_dir, dataset, "complete", "reduce", "scorecard.csv"), "10000"])


    # Clean up local data to avoid filling up space.
    logger.info(f"Cleaning up local copy of {dataset}")
    shutil.rmtree(str(root_path / dataset))
    
    return 'COMPLETE'



def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")
    parser.add_argument('source', type=str, help="Where to pull data, either 's3' or 'gdrive'.")
    parser.add_argument("--logfile", type=str, default="scorecard_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    args = parser.parse_args()

    init_logging(args.logfile)

    try:
        run_task_on_queue(args, run_scorecard_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

