#!/usr/bin/env python3

import os
import argparse
from pathlib import Path
import datetime
from pypeit.pypmsgs import PypeItError
import numpy as np
from astropy.stats import mad_std
from astropy.table import Table
from pypeit.specobjs import SpecObjs
from pypeit.spec2dobj import AllSpec2DObj
from pypeit.slittrace import SlitTraceBitMask
from pypeit.inputfiles import PypeItFile

def get_1d_std_chis_out_of_range(sobjs, lower_thresh, upper_thresh):
    num_out_of_range = 0
    for sobj in sobjs:
        ratio = sobj['OPT_COUNTS'] / sobj['OPT_COUNTS_SIG']
        std_chi = mad_std(ratio)
        if std_chi < lower_thresh or std_chi > upper_thresh:
            num_out_of_range += 1
    
    return num_out_of_range

def split_to_csv_tabs(t, outpath):
    """Split the score card table into smaller CSVs that will go into tabs in the Google sheet.
       The datasets are split based on the first letter of the mask name, according to ranges 
       that were determined by looking at the number of raw images per datset.
       
       Args:
       t (astropy.table.Table): The complete scorecard table.

       outpath (pathlib.Path): The path where the csv files should be go.

       Returns None. (But the csv files are written).
       """
    alphabet_ranges = [('0', '9'),
                       ('A', 'B'),
                       ('C', 'C'),
                       ('D', 'F'),
                       ('G', 'H'),
                       ('I', 'L'),
                       ('M', 'M'),
                       ('N', 'R'),
                       ('S', 'S'),
                       ('T', 'V'),
                       ('W', 'Z')]

    for range in alphabet_ranges:
        idx =[x[0].upper() >= range[0] and x[0].upper() <= range[1] for x in t['dataset']]
        if np.sum(idx) > 0:
            # The file will be something like 'scorecard_A-B.csv' for multi letter ranges, or
            # 'scorecard_C.csv' for single letter ranges
            t[idx].write(outpath / f"scorecard_{f'{range[0]}-{range[1]}' if range[0] != range[1] else range[0]}.csv", overwrite=True)
        



def main():
    parser = argparse.ArgumentParser(description='Build score card for completed pypeit reductions.\nAssumes the directory structure created by adap_reorg_setup.py')
    parser.add_argument("reorg_dir", type=str, help = "Root of directory structure created by adap_reorg_setup.py")
    parser.add_argument("outfile", type=str, help='Output csv file.')
    parser.add_argument("--commit", type=str, default = "", help='Optional, git commit id for the PypeIt version used')
    parser.add_argument("--status", type=str, default = None, help='Status of running the reduction')
    parser.add_argument("--masks", type=str, nargs='+', help="Specific masks to run on" )
    parser.add_argument("--rms_thresh", type=float, default=0.4)
    parser.add_argument("--wave_cov_thresh", type=float, default=60.0)
    parser.add_argument("--lower_std_chi", type=float, default=0.6)
    parser.add_argument("--upper_std_chi", type=float, default=1.6)

    args = parser.parse_args()

    reorg_path = Path(args.reorg_dir)
    masks_to_scan = []
    for path in reorg_path.iterdir():
        if not path.is_dir():
            continue
        if args.masks is not None:
            if path.name in args.masks:
                masks_to_scan.append(path)
        else:
            masks_to_scan.append(path)

    reduce_paths = []
    for mask_path in masks_to_scan:
        for config_path in mask_path.iterdir():
            for date_path in config_path.iterdir():
                for reduce_path in date_path.joinpath('complete').iterdir():
                    if reduce_path.name.startswith('reduce'):
                        reduce_paths.append(reduce_path)

    
    columns = ['dataset', 'science_file', 'git_commit', 'status', 'bad_slit_count', 'det_count', 'slit_count', 'slit_std_chi_out_of_range', 
               'slit_wv_cov_under_thresh', 'slit_rms_over_thresh', 'total_bad_flags', 'bad_wv_count', 'bad_tilt_count', 'bad_flat_count', 
               'skip_flat_count', 'bad_reduce_count', 'object_count', 
               'obj_rms_over_thresh', 'object_without_opt_with_box', 'object_without_opt_wo_box', 
               'maskdef_extract_count', 'date', 'reduce_dir']

    data = Table(names = columns, dtype=['U64', 'U22', 'U40', 'U8'] + [int for x in columns[4:-2]] + ['datetime64[D]', 'U20'])
    stbm = SlitTraceBitMask()
    for reduce_path in reduce_paths:
        dataset = reduce_path.parent.relative_to(args.reorg_dir)
        pypeit_file = str(reduce_path / "keck_deimos_A" / "keck_deimos_A.pypeit")
        science_path = reduce_path / "keck_deimos_A" / "Science"
        pf = PypeItFile.from_file(pypeit_file)
        science_idx = pf.data['frametype'] == 'science'
        for science_file in pf.data['filename'][science_idx]:
            data.add_row()
            data[-1]['dataset'] = dataset.parent 
            data[-1]['science_file'] = science_file
            data[-1]['status'] = args.status
            data[-1]['git_commit'] = args.commit
            data[-1]['reduce_dir'] = reduce_path.name
            data[-1]['date'] = datetime.date.today()
            science_stem = Path(science_file).stem
            spec2d_files = list(science_path.glob(f"spec2d_{science_stem}*.fits"))
            if len(spec2d_files) == 0:
                print(f"Could not find spec2d for {science_file}.")
            elif len(spec2d_files) > 1:
                print(f"Found too many spec2d files for {science_file}?")
            else:            
                print(f"Processing file {spec2d_files[0]}")

                total_bad_coverage = set()
                total_bad_slit_rms = set()
                bad_chi_slits = set()
                total_bad_flag_slits = set()
                total_bad_wv_slits = set()
                total_bad_tilt_slits = set()
                total_bad_flat_slits = set()
                total_skip_flat_slits = set()
                total_bad_reduce_slits = set()
                all_slit_ids = set()

                # only count as bad if it's bad in all dets?
                # nooooo not quite. Only count as good if it's good in all dets.
                # should we count good instead of or in additon to bad?
                # or count as bad if it's bad in any detector
                # data[science_file] = {x: 0 for x in columns}
                try:
                    allspec2d = AllSpec2DObj.from_fits(spec2d_files[0], chk_version=False)
                    for det in allspec2d.detectors:
                        data[-1]['det_count'] += 1
                        spec2dobj = allspec2d[det]

                        # Process wavsol data, ignoring slits where RMS is 0
                        nonzero_rms_slits = spec2dobj.wavesol['RMS'] != 0.0

                        bad_coverage = spec2dobj.wavesol['IDs_Wave_cov(%)'][nonzero_rms_slits] < args.wave_cov_thresh
                        bad_slit_rms = spec2dobj.wavesol['RMS'][nonzero_rms_slits] > args.rms_thresh

                        # Process std_chis, ignoring slits where both std and med chi are 0
                        std_chis = spec2dobj['std_chis']
                        med_chis = spec2dobj['med_chis']                
                        no_chis = (std_chis == 0.0) & (med_chis==0.0)
                        chis_out_of_range = ((std_chis<args.lower_std_chi) | (std_chis>args.upper_std_chi)) & np.logical_not(no_chis)

                        # Process slit mask bitmask flags
                        bad_wv_slits = np.array([stbm.flagged(x, 'BADWVCALIB') for x in spec2dobj.slits.mask])
                        bad_tilt_slits = np.array([stbm.flagged(x, 'BADTILTCALIB') for x in spec2dobj.slits.mask])
                        bad_flat_slits = np.array([stbm.flagged(x, 'BADFLATCALIB') for x in spec2dobj.slits.mask])
                        skip_flat_slits = np.array([stbm.flagged(x, 'SKIPFLATCALIB') for x in spec2dobj.slits.mask])
                        bad_reduce_slits = np.array([stbm.flagged(x, 'BADREDUCE') for x in spec2dobj.slits.mask])

                        # Combine the results for this detector/mosaic with the 
                        # totals. A set of slitord_id (spat_id)s is used to prevent
                        # us from counting slits twice per science file
                        total_bad_coverage.update(spec2dobj.wavesol['SpatID'][nonzero_rms_slits][bad_coverage])
                        total_bad_slit_rms.update(spec2dobj.wavesol['SpatID'][nonzero_rms_slits][bad_slit_rms])
                        total_bad_wv_slits.update(spec2dobj.slits.slitord_id[bad_wv_slits])
                        total_bad_tilt_slits.update(spec2dobj.slits.slitord_id[bad_tilt_slits])
                        total_bad_flat_slits.update(spec2dobj.slits.slitord_id[bad_flat_slits])
                        total_skip_flat_slits.update(spec2dobj.slits.slitord_id[skip_flat_slits])
                        total_bad_reduce_slits.update(spec2dobj.slits.slitord_id[bad_reduce_slits])
                        bad_chi_slits.update(spec2dobj.slits.slitord_id[chis_out_of_range])
                        all_slit_ids.update(spec2dobj.slits.slitord_id)

                except PypeItError:
                    print(f"Failed to load spec2d {spec2d_files[0]}")
                    data[-1]['status'] = 'FAILED'

                total_bad_flag_slits = total_bad_wv_slits | total_bad_tilt_slits | total_bad_flat_slits | total_bad_reduce_slits
                bad_slits =  total_bad_coverage | total_bad_slit_rms | bad_chi_slits | total_bad_flag_slits

                data[-1]['bad_slit_count'] = len(bad_slits)
                data[-1]['slit_count'] = len(all_slit_ids)
                data[-1]['slit_wv_cov_under_thresh'] = len(total_bad_coverage)
                data[-1]['slit_rms_over_thresh'] = len(total_bad_slit_rms)
                data[-1]['slit_std_chi_out_of_range'] = len(bad_chi_slits)
                data[-1]['total_bad_flags'] = len(total_bad_flag_slits)
                data[-1]['bad_wv_count'] = len(total_bad_wv_slits)
                data[-1]['bad_tilt_count'] = len(total_bad_tilt_slits)
                data[-1]['bad_flat_count'] = len(total_bad_flat_slits)
                data[-1]['skip_flat_count'] = len(total_skip_flat_slits)
                data[-1]['bad_reduce_count'] = len(total_bad_reduce_slits)


            spec1d_files = list(science_path.glob(f"spec1d_{science_stem}*.fits"))

            for spec1d_file in spec1d_files:
                print(f"Processing {spec1d_file}")
                try:
                    sobjs = SpecObjs.from_fitsfile(str(spec1d_file),chk_version=False)
                    data[-1]['object_count'] += len(sobjs)
                    data[-1]['maskdef_extract_count'] += np.sum(sobjs['MASKDEF_EXTRACT'])            

                    # Object RMS will eventually be replaced with slit rms once that's added to the
                    # spec2d
                    for specobj in sobjs:
                        if specobj['OPT_COUNTS'] is None:
                            if specobj['BOX_COUNTS'] is not None:
                                data[-1]['object_without_opt_with_box'] += 1
                                print(f"File {spec1d_file} object {specobj['NAME']} is missing OPT_COUNTS but has BOX_COUNTS.")
                            else:
                                data[-1]['object_without_opt_wo_box'] += 1
                                print(f"File {spec1d_file} object {specobj['NAME']} is missing OPT_COUNTS and BOX_COUNTS.")

                        if specobj['WAVE_RMS'] > args.rms_thresh:
                            data[-1]['obj_rms_over_thresh'] += 1
                except PypeItError:
                    print(f"Failed to load spec1d {spec1d_file}")
                    data[-1]['status'] = 'FAILED'


    data.sort(['status', 'dataset', 'science_file'])

    data.write(args.outfile, format='csv', overwrite=True)


if __name__ == '__main__':    
    main()

