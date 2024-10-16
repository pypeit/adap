import argparse
import logging
import sys
import os
from pathlib import Path
import shutil
from pypeit import msgs
from pypeit.spec2dobj import AllSpec2DObj;
from pypeit.io import fits_open
from pypeit.spectrographs.util import load_spectrograph
from pypeit.inputfiles import Coadd2DFile
from pypeit.utils import recursive_update

logger = logging.getLogger(__name__)

from astropy.table import Table
import numpy as np

from utils import run_task_on_queue, run_script, init_logging, get_reduce_params
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

def get_detectors(spec2d_file, bad_slits):
    if not spec2d_file.exists():
        raise ValueError("Could not find a spec2d file to get detectors from")
    detectors = []
    # Get the spectrograph to parse mosaics
    with  fits_open(str(spec2d_file)) as hdul:
        header = hdul[0].header
    spectrograph = load_spectrograph(header['PYP_SPEC'])
    allowed_mosaics = spectrograph.allowed_mosaics
    all2dspec = AllSpec2DObj.from_fits(str(spec2d_file),chk_version=False)
    logger.info(f"Found detectors {all2dspec.detectors} in {spec2d_file.name}")

    for detector in all2dspec.detectors:
        # Find out if there are any good slits in this detector
        excluded_slit_mask = np.array([f'{detector}:{slit_id}' in bad_slits for slit_id in all2dspec[detector].slits.slitord_id ])
        good_mask = np.logical_not((all2dspec[detector].slits.mask !=0) | excluded_slit_mask)
        if len(all2dspec[detector].slits.slitord_id[good_mask]) == 0:
            # Skip this detector, it has no good slits
            logger.info(f"Skipping {detector} because it has no good slits")

            # Filter out bad_slits that are in this detector, as they don't need to be passed to
            # pypeit_setup_coadd2d
            bad_slits = [slit_id for slit_id in bad_slits if slit_id.split(':')[0] != detector]
            continue

        if detector.startswith("DET"):
            detectors.append(str(int(detector[3:])))
        elif detector.startswith("MSC"):
            mosaic =  int(detector[3:]) - 1
            if mosaic < len(allowed_mosaics):
                detectors.append(",".join([str(det) for det in allowed_mosaics[mosaic]]))
            else:
                logger.warning(f"Unrecognized mosaic {detector}, not passing detectors to pypeit_setup_coadd2d")    
                return []
        else:
            logger.warning(f"Unrecognized detector {detector}, not passing detectors to pypeit_setup_coadd2d")
            return []
    
    return detectors, bad_slits

def update_coadd2d_params(args, coadd2d_filepath, reduce_params):

    # Read in the coadd2d file and update it's params
    coadd2d_file = Coadd2DFile.from_file(coadd2d_filepath, preserve_comments=True)
    
    # Update with the default coadd2d params
    default_config_path = Path(args.adap_root_dir, "adap", "config", "default_2dcoadd_config")
    coadd2d_defaults = Coadd2DFile.from_file(str(default_config_path), preserve_comments=True, vet=False)

    coadd2d_file.config.merge(coadd2d_defaults.config)

    # Merge in the reduce params from the pypeit file
    if 'reduce' in reduce_params:
        if 'reduce' not in coadd2d_file.config:
            coadd2d_file.config['reduce'] = reduce_params['reduce']
        else:
            coadd2d_file.config['reduce'].merge(reduce_params['reduce'])
    

    # Save a backup of the original file
    orig_file = coadd2d_filepath.with_name(coadd2d_filepath.name + ".orig")
    coadd2d_filepath.rename(orig_file)

    # Rewrite the file with the new params
    coadd2d_file.write(coadd2d_filepath)

def coadd2d_task(args, observing_config):

    root_path = Path(args.adap_root_dir)
    local_config_path = root_path / observing_config
    coadd2d_output = local_config_path / "2D_Coadd"
    dest_loc = None
    original_path = Path.cwd()

    try:
        # Download data 
        source_loc = get_cloud_path(args, args.source) / observing_config
        dest_loc = get_cloud_path(args, "s3") / observing_config / "2D_Coadd"
        backup_loc = get_cloud_path(args, "gdrive") / observing_config / "2D_Coadd"


        reduce_paths = list(source_loc.rglob("Science"))
        for reduce_path in reduce_paths:
            # Download the reduce path from s3
            relative_path = reduce_path.path.relative_to(source_loc.path)
            local_path = local_config_path / relative_path
            if not args.test or not local_path.exists():
                reduce_path.download(local_path)

        #Get the bad slits for pypeit_setup_coadd2d
        bad_slit_files = list(source_loc.rglob("*_bad_slits.csv"))
        for bad_slit_file in bad_slit_files:
            # Download the reduce path from s3
            relative_path = bad_slit_file.path.relative_to(source_loc.path)
            local_path = local_config_path / relative_path
            if not args.test or not local_path.exists():
                bad_slit_file.download(local_path.parent)


        # Remove prior results
        try:
            dest_loc.unlink()
        except Exception as e:
            # This could be failing due to the directory not existing so we ignore this.
            logger.warning(f"Failed to remove prior results in {dest_loc}")
            pass


        logger.info("Creating 2D_Coadd dir")
        coadd2d_output.mkdir(exist_ok=True)    

        os.chdir(coadd2d_output)

        # Build coadd2d setup command
        setup_2d_command = ["pypeit_setup_coadd2d",  "--spat_toler", "20"]

        # Find the science diretories
        sci_dirs = []
        for sci_dir in local_config_path.rglob("Science"):
            if sci_dir.is_dir():
                sci_dirs.append(str(sci_dir))
        setup_2d_command += ["-d"] + sci_dirs

        # Find the unique set of bad slits for all of the datasets in this config
        bad_slits = set()
        for bad_slit_file in local_config_path.rglob("*_bad_slits.csv"):
            logger.info(f"Loading bad slits from {bad_slit_file}")
            t = Table.read(str(bad_slit_file), format='csv')
            for slit_id in t['slit_id']:
                bad_slits.add(slit_id)

        # Find the first spec2d and get the list of detectors from it. The bad_slits are used
        # to filter out detectors that have no good slits, and any bad slits for skipped detectors
        # are filtered out of bad_slits
        spec2d_file = list(local_config_path.rglob("spec2d*.fits"))[0]        
        detectors, bad_slits = get_detectors(spec2d_file, bad_slits)
        if len(detectors) > 0:
            setup_2d_command += ["--det"] + detectors

        # Filter out bad slits for detectors that aren't going to be passed to pypeit_setup_coadd2d
        if len(bad_slits) > 0:
            setup_2d_command += ["--exclude_slits"] + list(bad_slits)

        # Run setup
        logger.info(f"Running coadd2d setup on {observing_config}")
        try:
            run_script(setup_2d_command, save_output="pypeit_setup_coadd2d.log")
        finally:
            # Always try to cleanup log
            if Path("pypeit_setup_coadd2d.log").exists():
                # Remove ansi escape code and unwanted log messages from log
                filter_output("pypeit_setup_coadd2d.log", "pypeit_setup_coadd2d.log.txt")

        # The resulting output files need to have the reduce parameters from reduction
        # added to it
        reduce_params = get_reduce_params(observing_config)

        # Run the generated coadd2d files
        for coadd2d_file in coadd2d_output.glob("keck_deimos*.coadd2d"):
            logger.info(f"Updating coadd2d file {coadd2d_file.name}")
            update_coadd2d_params(args, coadd2d_file, reduce_params)
            logger.info(f"Running coadd2d file {coadd2d_file.name}")
            try:
                run_script(["pypeit_coadd_2dspec", coadd2d_file.name], save_output=f"{coadd2d_file.stem}.log")
            finally:
                if Path(f"{coadd2d_file.stem}.log").exists():
                    filter_output(f"{coadd2d_file.stem}.log", f"{coadd2d_file.stem}.log.txt")

    finally:
        
        # Make sure we return to the original current working directory and cleanup after ourselves.
        os.chdir(original_path)

        # Always try to upload results and logs even on failures
        if not args.test and dest_loc is not None:
            # Upload the results
            if coadd2d_output.exists():
                logger.info(f"Uploading output to {dest_loc}")
                dest_loc.upload(coadd2d_output)

                logger.info(f"Backing up output to {backup_loc}")
                backup_loc.upload(coadd2d_output)

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
    parser.add_argument("--logfile", type=str, default="coadd2d_from_queue.log", help= "Log file.")
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--google_creds", type=str, default = f"{os.environ['HOME']}/.config/gspread/service_account.json", help="Service account credentials for google drive and google sheets.")
    parser.add_argument("--rclone_conf", type=str, default = f"{os.environ['HOME']}/.config/rclone/rclone.conf", help="rclone configuration.")
    parser.add_argument("--test", action="store_true", default = False, help="Run in test config, which does not download data if it already is present and doesn't clear data when done.")
    args = parser.parse_args()

    try:
        init_logging(Path(args.adap_root_dir, args.logfile))
        run_task_on_queue(args, coadd2d_task)
    except:
        logger.error("Exception caught in main, exiting",exc_info=True)        
        return 1

    return 0

if __name__ == '__main__':    
    sys.exit(main())

