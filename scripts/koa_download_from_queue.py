"""
Download raw LRIS data from KOA for targets on a work queue.

This is the stage that turns a list of targets into a list of datasets. It pops a target
name off the target queue, looks its coordinates up in the targets tab, asks KOA what was
observed, downloads the science, arc and flat frames for every night it finds, uploads the
result to S3, and appends the datasets it discovered to the dataset queue's tab so that
the reduce stage can pick them up.

It is a separate queue from the dataset queue on purpose. A dataset is
"<target>/<date>/<instrument>", and the date and the arm are *discovered* by the KOA
query -- there is no way to enqueue a dataset before the query that finds it has run.
See koa_download_design.rst.
"""

import argparse
import logging
import os
import re
import shutil
import sys
from pathlib import Path

import redis

logger = logging.getLogger(__name__)

# The download_lib modules import each other by bare name, so their directory has to be
# importable. Appended rather than inserted: scripts/ has a tell_remove.py of its own that
# must keep winning over download_lib's.
sys.path.append(str(Path(__file__).parent / "download_lib"))

import gspread_utils
from utils import run_task_on_queue, init_logging, koa_download_slot, lock_named_queue
from rclone import RClonePath, get_cloud_path

import Query
import DownloadUtils
import TargetList

# Where the KOA query tables are cached between attempts. The prefix tracks the default
# in get_cloud_path, which is what decides where the raw data itself goes.
QUERY_CACHE_PREFIX = ("pypeit", "adap_2023", "koa_queries")


def query_cache_location(args, target_name):
    """The remote directory holding a target's cached KOA query tables."""
    return RClonePath(args.rclone_conf, "s3", *QUERY_CACHE_PREFIX, target_name)


def restore_query_cache(args, target_name, outdir):
    """
    Pull a target's cached KOA query tables down, if there are any.

    Query writes its results to .tbl files in outdir and reads them back instead of
    re-querying, but outdir is on the pod's ephemeral storage, so without this a retry on
    a fresh pod re-runs every KOA query from scratch. Restoring them first makes a retry
    cost only the frames that are actually missing, which matters much more once
    downloads are throttled.
    """
    if args.source != "s3":
        return

    try:
        query_cache_location(args, target_name).download(str(outdir))
        logger.info(f"Restored cached KOA query tables for {target_name}")
    except Exception:
        # rclone fails if the remote directory does not exist, which is the ordinary
        # case the first time a target is downloaded.
        logger.info(f"No cached KOA query tables for {target_name}, querying from scratch")


def save_query_cache(args, target_name, outdir):
    """Upload a target's KOA query tables so the next attempt does not have to re-query."""
    if args.source != "s3":
        return

    tables = sorted(Path(outdir).glob("*.tbl"))
    if len(tables) == 0:
        return

    # rclone copies whole directories, and outdir holds the downloaded frames as well, so
    # stage just the tables rather than pushing the raw data into the cache too.
    staging = Path(args.adap_root_dir, "koa_query_cache", target_name)
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    try:
        for table in tables:
            shutil.copy2(table, staging / table.name)
        query_cache_location(args, target_name).upload(str(staging))
        logger.info(f"Cached {len(tables)} KOA query tables for {target_name}")
    except Exception:
        # Losing the cache costs time on a retry but nothing else, so it is not fatal.
        logger.error(f"Failed to cache KOA query tables for {target_name}", exc_info=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def record_datasets(args, datasets):
    """
    Append newly downloaded datasets to the dataset queue's tab, with a blank status.

    A blank status is what the init sentinel looks for, so this is what puts a dataset in
    line to be reduced. The lock taken is the *dataset* queue's, not this stage's: it is
    that queue's tab being written, and its lock is what other writers to it hold.
    """
    if len(datasets) == 0:
        return []

    redis_server = redis.Redis.from_url(args.queue_url)
    redis_server.ping()

    with lock_named_queue(redis_server, args.dataset_work_queue):
        added = gspread_utils.append_datasets(args.dataset_gsheet, datasets)

    if len(added) > 0:
        logger.info(f"Added {len(added)} dataset(s) to {args.dataset_gsheet}: "
                    f"{', '.join(added)}")
    else:
        logger.info(f"All {len(datasets)} dataset(s) were already in "
                    f"{args.dataset_gsheet}")

    return added


def download_target_task(args, target_name):
    """
    Download everything KOA has for one target.

    Returns COMPLETE, NO DATA or FAILED. NO DATA is not a failure -- plenty of targets
    were simply never observed with LRIS -- but it is worth being able to find those rows
    in the sheet later, so it is kept distinct from COMPLETE.
    """
    raw_root = Path(args.adap_root_dir, "RAW_DATA")
    raw_root.mkdir(parents=True, exist_ok=True)

    row = gspread_utils.find_target_coords(args.gsheet, target_name)
    if row is None:
        logger.error(f"{target_name} has no row in {args.gsheet}, so its coordinates "
                     f"cannot be looked up.")
        return "FAILED"

    try:
        target = TargetList.make_target(row['name'], row['ra'], row['dec'])
    except ValueError as e:
        logger.error(f"Cannot download {target_name}: {e}")
        return "FAILED"

    query = Query.Query(target, topdir=str(raw_root))

    try:
        restore_query_cache(args, target_name, query.outdir)

        # Everything that talks to KOA happens inside the slot: the position query, the
        # per-night configuration queries, and the frame downloads.
        with koa_download_slot(args):
            query.query_position()

            if query.query_error is not None:
                logger.error(f"KOA query failed for {target_name}: {query.query_error}")
                return "FAILED"

            if query.obj_results is None or len(query.obj_results) == 0:
                logger.info(f"KOA has no LRIS data for {target_name} at "
                            f"{target.ra} {target.dec}")
                save_query_cache(args, target_name, query.outdir)
                return "NO DATA"

            results = DownloadUtils.fill_dates(query, target)

        save_query_cache(args, target_name, query.outdir)

        if len(results) == 0:
            logger.info(f"No usable nights for {target_name}")
            return "NO DATA"

        complete = [dataset for dataset, status, detail in results if status == "COMPLETE"]
        bad = [(dataset, status, detail) for dataset, status, detail in results
               if status != "COMPLETE"]

        try:
            dest = get_cloud_path(args, args.source) / target_name
            dest.upload(str(query.outdir))
        except Exception:
            logger.error(f"Failed to upload {target_name}", exc_info=True)
            return "FAILED"

        # Only datasets that verified complete are offered to the reduce stage. The rest
        # stay out of the queue until a retry finishes them.
        record_datasets(args, complete)

        for dataset, status, detail in bad:
            logger.error(f"{status} {dataset}: {detail}")

        if len(bad) > 0:
            return "FAILED"

        return "COMPLETE"

    finally:
        cleanup(query.outdir)


def cleanup(outdir):
    """Remove a target's local data once it is in S3, to free the pod's ephemeral disk."""
    try:
        shutil.rmtree(outdir, ignore_errors=True)
    except Exception:
        logger.error(f"Failed to clean up {outdir}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description='Download KOA data for targets on a work queue.')
    parser.add_argument('gsheet', type=str, help="Targets Google Spreadsheet and Worksheet. For example: Scorecard/targets")
    parser.add_argument('queue_url', type=str, help="URL of the redis server hosting the work queue.")
    parser.add_argument('work_queue', type=str, help="Name of the target work queue to work off of.")
    parser.add_argument('source', type=str, help="Where to push data, either 's3' or 'gdrive'.")
    parser.add_argument('--dataset_gsheet', type=str, default=None, help="Spreadsheet and worksheet holding the dataset work queue. Defaults to the WorkQueue tab of the same spreadsheet as gsheet.")
    parser.add_argument('--dataset_work_queue', type=str, default=None, help="Name of the dataset work queue, whose lock guards its tab. Defaults to work_queue with a trailing '_targets' removed.")
    parser.add_argument('--max_koa_downloads', type=int, default=2, help="Maximum number of pods downloading from KOA at once, enforced with a semaphore in redis. 0 disables it and relies on the job's parallelism alone.")
    parser.add_argument('--queue_timeout', type=int, default=120, help="Number of seconds to wait for the work queue to initialize.")
    parser.add_argument("--logfile", type=str, default="koa_download_from_queue.log", help="Log file.")
    parser.add_argument("--adap_root_dir", type=Path, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--endpoint_url", type=str, default=os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io"), help="The URL used to access S3. Defaults $ENDPOINT_URL, or the PRP Nautilus external URL.")
    parser.add_argument("--rclone_conf", type=str, default=f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    args = parser.parse_args()

    if args.dataset_gsheet is None:
        args.dataset_gsheet = args.gsheet.split("/")[0] + "/WorkQueue"

    if args.dataset_work_queue is None:
        args.dataset_work_queue = re.sub(r'_targets$', '', args.work_queue)

    try:
        init_logging(Path(args.adap_root_dir, args.logfile))
        logger.info(f"Targets from {args.gsheet}, datasets to {args.dataset_gsheet}, "
                    f"guarded by the {args.dataset_work_queue} lock")
        run_task_on_queue(args, download_target_task)
    except:
        logger.error("Exception caught in main, exiting", exc_info=True)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
