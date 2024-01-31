import os
import glob
from operator import is_
import argparse
from pathlib import Path
import sys
import numpy as np

from pypeit import pypeitsetup, msgs
from pypeit.inputfiles import PypeItFile
from IPython import embed
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

def find_none_rows(table):
    # Return indexes of rows in table that have None values.

    rows = None
    for col in table.colnames:
        if rows is not None:
            rows = np.concatenate((np.where(np.vectorize(is_)(table[col], None))[0], rows))
        else:
            rows = np.where(np.vectorize(is_)(table[col], None))[0]

    return np.unique(rows)


def trim_keck_deimos(metadata):
    """
    TODO restore this code
    mjd_order = np.argsort(ps.fitstbl['mjd'])

    # Criteria

    best_dec = np.abs(ps.fitstbl['dec']-45.) < 1.
    best_arc_exp = (ps.fitstbl['exptime'] >= 1.) & (ps.fitstbl['exptime'] <= 15.) 
    best_flat_exp = (ps.fitstbl['exptime'] > 5.) & (ps.fitstbl['exptime'] <= 30.) 

    test_criterion = (ps.fitstbl['exptime'] > 50000.)

    arc_type = 'arc,tilt'
    flat_type = 'pixelflat,illumflat,trace'
    min_arc_set = 1
    min_flats = 3

    # ########################
    # Arcs
    arcs = ps.fitstbl['frametype'] == arc_type 
    not_excluded_arcs = arcs & np.isin(ps.fitstbl['filename'],raw_files_to_exclude, invert=True)

    arc_criteria = [best_arc_exp, best_dec]

    # Lamps
    unique_lamps = np.unique(ps.fitstbl['lampstat01'][arcs].data)
    indiv_lamps = [item.split(' ') for item in unique_lamps]
    nlamps = [len(item) for item in indiv_lamps]

    lamp_order = np.argsort(nlamps)[::-1]

    all_keep_arcs = []
    all_lamps = []
    # Loop on the lamp sets
    for ilamp in lamp_order:
        # Any new lamps?
        new = False
        for lamp in indiv_lamps[ilamp]:
            if lamp not in all_lamps:
                new = True
        if not new:
            msgs.info(f"No new lamps in {indiv_lamps[ilamp]}")
            continue
        
        # All matching this set
        arc_set = not_excluded_arcs & (ps.fitstbl['lampstat01'].data == unique_lamps[ilamp])
        
        # Criteria
        while True:
            criteria = np.stack([arc_set]+arc_criteria)
            gd_arcs = np.all(criteria, axis=0)
            # Have enough?
            if np.sum(gd_arcs) >= min_arc_set or len(arc_criteria) == 0:
                break
            # Remove a criterion
            arc_criteria.pop()

        # Take the latest entries in time
        keep_arcs = np.where(gd_arcs)[0]
        sort_mjd_arcs = np.argsort(ps.fitstbl['mjd'].data[keep_arcs])
        keep_arcs = keep_arcs[sort_mjd_arcs[-min_arc_set:]]

        all_keep_arcs += keep_arcs.tolist()

        # Record the lamps
        all_lamps += indiv_lamps[ilamp]
        all_lamps = np.unique(all_lamps).tolist()
        
    # Keep em
    all_arcs = np.where(arcs)[0]
    sort_mjd_arcs = np.argsort(ps.fitstbl['mjd'].data[keep_arcs])
    for idx in all_arcs:
        if idx not in all_keep_arcs:
            filenames[idx] = '#'+filenames[idx]

    # #################
    # Flats
    flats = ps.fitstbl['frametype'] == flat_type 
    not_excluded_flats = flats  & np.isin(ps.fitstbl['filename'],raw_files_to_exclude, invert=True)

    flat_criteria = [best_flat_exp, best_dec]

    while True:
        criteria = np.stack([not_excluded_flats]+flat_criteria)
        gd_flats = np.all(criteria, axis=0)
        # Have enough?
        if np.sum(gd_flats) > min_flats or len(flat_criteria) == 0:
            break
        # Remove a criterion
        msgs.info("Removing an flat criterion")
        flat_criteria.pop()

    # Keep the last ones
    all_flats = np.where(flats)[0]
    keep_flats = np.where(gd_flats)[0]
    sort_mjd_flats = np.argsort(ps.fitstbl['mjd'].data[keep_flats])
    keep_flats = keep_flats[sort_mjd_flats[-min_flats:]]
    for idx in all_flats:
        if idx not in keep_flats:
            filenames[idx] = '#'+filenames[idx]
    """
    pass

def trim_criteria(metadata, sort_fields, max, initial_files, all_criteria):

    
    orig_num = np.sum(initial_files) 
    trimmed_files = np.zeros_like(initial_files, dtype=bool)
    while len(all_criteria) > 0:
        # The remaining criteria from the initial_files, by using AND
        trimmed_files = np.logical_and.reduce([initial_files] + all_criteria)
        if orig_num - np.sum(trimmed_files) >= max:
            # There are still enough acceptable files, so we're done trimming
            # by criteria
            break
        else:
            # Too many files were trimmed, remove a criteria and try again
            all_criteria.pop()
    
    remaining_num = orig_num - np.sum(trimmed_files)
    
    if remaining_num < max:
        # We can't do any trimming, 
        trimmed_files = np.zeros_like(initial_files, dtype=bool)
        remaining_num = orig_num

    if remaining_num > max:
        # If there are still too many, sort the remaining files by the
        # requested sort fields and remove the extra from the start of those
        # files
        remaining_files = np.logical_xor(initial_files, trimmed_files)
        remaining_indices = np.where(remaining_files)[0]
        sort_indices = metadata.table[remaining_files].argsort(sort_fields)
        additional_indices_to_trim = remaining_indices[sort_indices][0:remaining_num-max]
        trimmed_files = trimmed_files | np.isin(np.arange(len(metadata)),additional_indices_to_trim)

    return trimmed_files        

def trim_keck_hires(metadata, good_frames):
    # Find the flats and arcs that have not already been excluded for some reason.
    all_flats = metadata.find_frames('pixelflat') & good_frames
    all_arcs = metadata.find_frames('arc') & good_frames
    all_darks = metadata.find_frames('dark') & good_frames

    # Trim things not close enough to 45
    dec_criteria = np.abs(metadata['dec']-45.) >= 1
    flats_to_trim = trim_criteria(metadata, ['mjd'], 5, all_flats, [dec_criteria])
    arcs_to_trim = trim_criteria(metadata, ['mjd'], 1, all_arcs, [dec_criteria])
    darks_to_trim = trim_criteria(metadata, ['mjd'], 1, all_darks, [dec_criteria])

    return flats_to_trim | arcs_to_trim | darks_to_trim

    #arc_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 2.) 
    #flat_exp_criteria = (metadata['exptime'] >= 1.) & (metadata['exptime'] <= 5.) 

trimming_functions = {"keck_deimos": trim_keck_deimos,
                      "keck_esi": lambda x: np.zeros_like(x['filename'],dtype=bool),
                      "keck_hires": trim_keck_hires}

def comment_out_filenames(metadata, files_idx):    
    metadata['filename'] = ['# ' + str(name) if files_idx[i] else name for i, name in enumerate(metadata['filename'])]

def make_trimmed_setup(spectrograph, lcl_path, raw_files_to_exclude, reduce_dir, config_lines):


    # Create a PypeItSetup object for the raw files, excluding any files if needed
    raw_path = lcl_path.resolve() / "raw"
    file_list = [str(raw_file) for raw_file in raw_path.glob('*.fits')]
    ps = pypeitsetup.PypeItSetup(file_list=file_list, 
                                 spectrograph_name = spectrograph)



    # Reduce dir
    target_dir = lcl_path / reduce_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    # Run setup
    ps.run(setup_only=True)

    # Remove rows with None, as these cause PypeIt to crash
    rows_with_none = find_none_rows(ps.fitstbl.table)
    ps.fitstbl.table.remove_rows(rows_with_none)

    # Remove rows with unknown 'None' frame types
    ps.fitstbl.table.remove_rows(ps.fitstbl.table["frametype"] == 'None')

    # Find excluded files, these will be commented out
    excluded_files = np.isin(ps.fitstbl['filename'], raw_files_to_exclude)
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
    
def update_custom_pypeit(complete_path, spectrograph_name, reduce_dir, pypeit_file):
    # Create the destination directory
    dir_name = f"{spectrograph_name}_A"
    (complete_path / reduce_dir / dir_name).mkdir(parents=True, exist_ok=True)

    # Update the raw data directory in the pypeit file
    pypeit_file.file_paths = [str(complete_path / "raw")]
    pypeit_file.write(complete_path / reduce_dir / dir_name / f"{dir_name}.pypeit")

def main():
    parser = argparse.ArgumentParser(description='Build a trimmed down setup file for ADAP raw data. It assumes the ADAP directory structure.')
    parser.add_argument("spectrograph", type=str, )
    parser.add_argument("datasets", type=str, nargs='+', help="dataset(s) to run on" )
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")

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
        raw_files_to_exclude = [line for line in read_lines(config_path / "exclude_files.txt") if not line.startswith('#')]

        default_config_file = config_path / f"{args.spectrograph}_default_pypeit_config"

        if not default_config_file.exists():
            default_config_lines = None
        else:
            default_config_lines = read_lines(default_config_file)

        complete_path = Path(args.adap_root_dir) / dataset / "complete"

        # Sanity check things
        if not complete_path.is_dir() or len(complete_path.parents) < 4:
            msgs.warn(f"Either non existant or invalid complete path {complete_path}")
            return 1

        msgs.info(f"Processing path {complete_path}.")

        tailored_config_files = config_path.glob(f"{dataset.replace('/', '_')}_*")
        reduce_configs= []
        for tailored_config_file in tailored_config_files:
            reduce_subdir = tailored_config_file.stem.split("_")[-1]
            msgs.info(f"Reading tailored config file: {tailored_config_file}")
            if tailored_config_file.suffix == ".ini":
                config_lines = read_lines(tailored_config_file)
                reduce_configs.append((reduce_subdir, config_lines))
            else:
                # A complete custom .pypeit file
                pf = PypeItFile.from_file(tailored_config_file)
                reduce_configs.append((reduce_subdir, pf))

        if len(reduce_configs) == 0:
            msgs.info(f"Using default config file.")
            reduce_configs.append(("reduce", default_config_lines))

        for reduce_config in reduce_configs:
            if isinstance(reduce_config[1], PypeItFile):
                msgs.info(f"Updating custom pypeit file for {complete_path/reduce_config[0]}")
                update_custom_pypeit(complete_path, args.spectrograph, reduce_config[0], reduce_config[1])
            else:
                msgs.info(f"Creating setup for {complete_path/reduce_config[0]}")
                make_trimmed_setup(args.spectrograph, complete_path, raw_files_to_exclude, reduce_config[0], reduce_config[1])


if __name__ == '__main__':
    sys.exit(main())