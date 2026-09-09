import os
import glob
from operator import is_
import argparse
from pathlib import Path
import sys
import numpy as np

from pypeit import pypeitsetup, msgs
from pypeit.par.pypeitpar import PypeItPar
from pypeit.inputfiles import PypeItFile
from IPython import embed

from metadata_info import exclude_pypeit_types, spec_to_instrument

'''
Arcs:

Use 1 file for each lamp set, provided at least one of the lamps is unique.  Prioritize files with the most lamps
When provided more than 1 file for a give lamp set, aim for 45 deg elevation and exposure time 1<t<15
If more than 1 exists meeting #2, take the last one in mjd

Flats:

Aim for exposure 5 < t < 30
Aim for 45deg elevation
Take the 3 last in mjd that meet the criteria
'''

# Test case

def find_none_rows(metadata):
    # Return indexes of rows in table that have None values.

    table = metadata.table
    # We only care about columns that go into the pypeit file
    pypeit_colnames = metadata.set_pypeit_cols()
    rows = None
    for col in pypeit_colnames:
        if rows is not None:
            rows = np.concatenate((np.where(np.vectorize(is_)(table[col], None))[0], rows))
        else:
            rows = np.where(np.vectorize(is_)(table[col], None))[0]

    return np.unique(rows)








    #arc_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 2.) 
    #flat_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 5.) 

    #arc_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 2.) 
    #flat_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 5.) 


def no_trimming(metadata, good_frames):
    # A trimming function that trims nothing
    return np.zeros_like(metadata['filename'],dtype=bool)

# Calibration frames are no longer trimmed: every arc and flat that survives
# exclude_files.txt and exclude_pypeit_types is left in the .pypeit file for PypeIt to
# combine.
trimming_functions = {"keck_lris_red_orig": no_trimming,
                      "keck_lris_red": no_trimming,
                      "keck_lris_red_mark4": no_trimming,
                      "keck_lris_blue": no_trimming,
                      "keck_lris_blue_orig": no_trimming,
                      }

def comment_out_filenames(metadata, files_idx):    
    metadata['filename'] = ['# ' + str(name) if files_idx[i] else name for i, name in enumerate(metadata['filename'])]



def make_trimmed_setup(spectrograph, lcl_path, raw_files_to_exclude, reduce_dir, config_lines, raw_dir):


    # Create a PypeItSetup object for the raw files, excluding any files if needed
    raw_path = lcl_path.resolve() / raw_dir
    file_list = [str(raw_file) for raw_file in raw_path.glob('*.fits')]
    file_list += [str(raw_file) for raw_file in raw_path.glob('*.fits.gz')]
    ps = pypeitsetup.PypeItSetup(file_list=file_list, 
                                 spectrograph_name = spectrograph)


    # Reduce dir
    target_dir = lcl_path / reduce_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    # Run setup
    ps.run(setup_only=True)

    # Remove rows with None, as these cause PypeIt to crash
    rows_with_none = find_none_rows(ps.fitstbl)
    ps.fitstbl.table.remove_rows(rows_with_none)

    # Remove rows with unknown 'None' frame types
    ps.fitstbl.table.remove_rows(ps.fitstbl.table["frametype"] == 'None')

    # Find excluded files, these will be commented out
    instrument = spec_to_instrument[spectrograph]
    excluded_files = np.isin(ps.fitstbl['filename'], raw_files_to_exclude) | np.isin(ps.fitstbl['frametype'], exclude_pypeit_types[instrument])
    comment_out_filenames(ps.fitstbl, excluded_files)

    # Do instrument specific trimming of calibration files
    files_to_trim = trimming_functions[spectrograph](ps.fitstbl, np.logical_not(excluded_files))

    # Comment out trimmed files
    comment_out_filenames(ps.fitstbl, files_to_trim)

    # The reorg script should make sure everything is in the same setup group. But
    # it may use different criteria than pypeit_setup, so force everything to be in group a,
    # calib group 0
    not_group_a = ps.fitstbl.table['setup'] != 'A'
    ps.fitstbl.table['setup'][not_group_a] = 'A'
    ps.fitstbl.table['calib'][not_group_a] = 0

    # Write trimmed setup
    ps.fitstbl.write_pypeit(target_dir,cfg_lines=config_lines, configs = ['A'])

def read_lines(file):
    """Short helper method to read lines from a text file into a list, removing newlines."""
    with open(file, "r") as f:
        lines = [line.rstrip() for line in f]
    return lines
    
def update_custom_pypeit(complete_path, spectrograph_name, reduce_dir, pypeit_file, raw_dir):
    # Create the destination directory
    dir_name = f"{spectrograph_name}_A"
    (complete_path / reduce_dir / dir_name).mkdir(parents=True, exist_ok=True)

    # Update the raw data directory in the pypeit file
    pypeit_file.file_paths = [str(complete_path / raw_dir)]
    pypeit_file.write(complete_path / reduce_dir / dir_name / f"{dir_name}.pypeit")


def main():
    parser = argparse.ArgumentParser(description='Build a trimmed down setup file for ADAP raw data. It assumes the ADAP directory structure.')
    parser.add_argument("spectrograph", type=str, )
    parser.add_argument("datasets", type=str, nargs='+', help="dataset(s) to run on" )
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")
    parser.add_argument("--raw_dir", type=str, default="raw", help="Name of the raw data directory within the dataset. For example 'raw_b' or 'raw_r'.")

    args = parser.parse_args()

    # Create a unique log file
    logname = Path("trimming_setup.log")
    i = 1
    while logname.exists():
        logname = Path(f"trimming_setup.{i}.log")
        i+=1

    msgs.reset_log_file(logname)

    for dataset in args.datasets:

        msgs.info(f"Creating trimmed setup for {dataset}.")
        config_path = Path(__file__).parent.parent /"config"
        raw_files_to_exclude = [line.strip() for line in read_lines(config_path / "exclude_files.txt")
                                if line.strip() and not line.strip().startswith('#')]

        default_config_file = config_path / f"{args.spectrograph}_default_pypeit_config"

        if not default_config_file.exists():
            default_config_lines = None
        else:
            default_config_lines = read_lines(default_config_file)

        dataset_path = Path(args.adap_root_dir) / dataset

        # Sanity check things
        if not dataset_path.is_dir():
            msgs.warn(f"Either non existant or invalid dataset path {dataset_path}")
            return 1

        msgs.info(f"Processing path {dataset_path}.")

        tailored_config_files = config_path.rglob(f"{dataset.replace('/', '_')}_*")
        reduce_configs= []
        for tailored_config_file in tailored_config_files:
            reduce_subdir = tailored_config_file.stem.split("_")[-1]
            msgs.info(f"Reading tailored config file: {tailored_config_file}")
            if tailored_config_file.suffix == ".ini":
                config_lines = read_lines(tailored_config_file)
                reduce_configs.append((reduce_subdir, config_lines))
            else:
                # A complete custom .pypeit file
                pf = PypeItFile.from_file(tailored_config_file, preserve_comments=True)
                reduce_configs.append((reduce_subdir, pf))

        if len(reduce_configs) == 0:
            msgs.info(f"Using default config file.")
            reduce_configs.append(("reduce", default_config_lines))
            

        for reduce_config in reduce_configs:
            if isinstance(reduce_config[1], PypeItFile):
                msgs.info(f"Updating custom pypeit file for {dataset_path/reduce_config[0]}")
                update_custom_pypeit(dataset_path, args.spectrograph, reduce_config[0], reduce_config[1], args.raw_dir)
            else:
                msgs.info(f"Creating setup for {dataset_path/reduce_config[0]}")
                make_trimmed_setup(args.spectrograph, dataset_path, raw_files_to_exclude, reduce_config[0], reduce_config[1], args.raw_dir)


if __name__ == '__main__':
    sys.exit(main())
