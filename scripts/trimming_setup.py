import os
import glob
from operator import is_
import argparse
from pathlib import Path

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


def make_trimmed_setup(lcl_path, raw_files_to_exclude, reduce_dir, config_lines):


    # Create a PypeItSetup object for the raw files, excluding any files if needed
    raw_path = lcl_path.resolve() / "raw"
    file_list = [str(raw_file) for raw_file in raw_path.glob('*.fits')]
    ps = pypeitsetup.PypeItSetup(file_list=file_list, 
                                 spectrograph_name = 'keck_deimos')



    # Reduce dir
    target_dir = lcl_path / reduce_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    # Run setup
    ps.run(sort_dir=str(target_dir), setup_only=True)

    # Remove rows with None, as these cause PypeIt to crash
    rows_with_none = find_none_rows(ps.fitstbl.table)
    ps.fitstbl.table.remove_rows(rows_with_none)

    # Remove rows with unknown 'None' frame types
    ps.fitstbl.table.remove_rows(ps.fitstbl.table["frametype"] == 'None')

    filenames = ps.fitstbl['filename'].data.tolist()

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


    # Science
    science = ps.fitstbl['frametype'] == 'science'
    keep_science = science & np.isin(ps.fitstbl['filename'],raw_files_to_exclude, invert=True)
    all_science = np.where(science)[0]
    all_sci_keep = np.where(keep_science)[0]
    for idx in all_science:
        if idx not in all_sci_keep:
            filenames[idx] = '#'+filenames[idx]


    # Finish
    ps.fitstbl.table['filename'] = filenames
    ps.fitstbl.write_pypeit(target_dir,cfg_lines=config_lines)

def read_lines(file):
    """Short helper method to read lines from a text file into a list, removing newlines."""
    with open(file, "r") as f:
        lines = [line.rstrip() for line in f]
    return lines
    

def main():
    parser = argparse.ArgumentParser(description='Build a trimmed down setup file for ADAP raw data. It assumes the ADAP directory structure.')
    parser.add_argument("masks", type=str, nargs='+', help="Masks to run on" )
    parser.add_argument("--adap_root_dir", type=str, default=".", help="Root of the ADAP directory structure. Defaults to the current directory.")

    args = parser.parse_args()

    # Create a unique log file
    logname = Path("trimming_setup.log")
    i = 1
    while logname.exists():
        logname = Path(f"trimming_setup.{i}.log")
        i+=1

    msgs.reset_log_file(logname)
    msgs.info(f"Creating trimmed setup for {args.masks}.")
    config_path = Path(args.adap_root_dir) / "adap" /"config"
    raw_files_to_exclude = [line for line in read_lines(config_path / "exclude_files.txt") if not line.startswith('#')]

    default_config_file = config_path / "default_pypeit_config"
    default_config_lines = read_lines(default_config_file)

    for mask in args.masks:
        mask_path = Path(args.adap_root_dir) / mask

        for complete_path in mask_path.rglob("complete"):
            # Sanity check things
            if not complete_path.is_dir() or len(complete_path.parents) < 4:
                msgs.warn(f"Either non existant or invalid complete path {complete_path}")
                continue
            msgs.info(f"Processing path {complete_path}.")

            setup = complete_path.parent.parent.name
            date = complete_path.parent.name
            tailored_config_files = config_path.glob(f"pypeit_config_{mask}_{setup}_{date}_*")
            reduce_configs= []
            for tailored_config_file in tailored_config_files:
                reduce_subdir = tailored_config_file.name.split("_")[-1]
                msgs.info(f"Reading tailored config file: {tailored_config_file}")
                config_lines = read_lines(tailored_config_file)
                reduce_configs.append((reduce_subdir, config_lines))

            if len(reduce_configs) == 0:
                msgs.info(f"Using default config file.")
                reduce_configs.append(("reduce", default_config_lines))

            for reduce_config in reduce_configs:
                msgs.info(f"Creating setup for {complete_path/reduce_config[0]}")
                make_trimmed_setup(complete_path, raw_files_to_exclude, reduce_config[0], reduce_config[1])


if __name__ == '__main__':
    main()