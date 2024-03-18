#!/usr/bin/env python3

import re
import argparse
from pathlib import Path
import datetime
from traceback import print_exc
from pypeit.pypmsgs import PypeItError
import numpy as np
from astropy.stats import mad_std
from astropy.table import Table, vstack
from pypeit.specobjs import SpecObjs
from pypeit.spec2dobj import AllSpec2DObj
from pypeit.slittrace import SlitTraceBitMask
from pypeit.inputfiles import PypeItFile
from pypeit.par import PypeItPar
from pypeit.spectrographs.util import load_spectrograph

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
        
def get_exec_time(log_file):
    """
    Return the execution time (in seconds) from a PypeIt log file.
    """
    regex = re.compile("Execution time: (\S.*)$")

    try:
        with open(log_file, "r") as f:
            for line in f:
                m = regex.search(line)
                if m is not None:
                    exec_time = m.group(1)
                    days = 0
                    hours = 0
                    minutes = 0
                    seconds = 0
                    for s in exec_time.split():
                        if s[-1] == "d":
                            days = int(s[0:-1])
                        elif s[-1] == "h":
                            hours = int(s[0:-1])
                        elif s[-1] == "m":
                            minutes = int(s[0:-1])
                        elif s[-1] == "s":
                            seconds = float(s[0:-1])

                    td = datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
                    return round(td.total_seconds())
    except Exception:        
        print(f"Failed to read from {log_file}")
        print_exc()
    return 0

def get_expected_det(par, args):

    # Get the default # of detectors from the spectrograph
    spec = load_spectrograph(args.spec_name)
    default_par = spec.default_pypeit_par()

    if default_par['rdx']['detnum'] is None or not isinstance(default_par['rdx']['detnum'], list):
        expected_det = 1
    else:
        expected_det = len(default_par['rdx']['detnum'])

    # If the PypeIt file specifies something other than the default, use that
    if par['rdx']['detnum'] is not None:
        if isinstance(par['rdx']['detnum'], list):
            expected_det = len(par['rdx']['detnum'])

    return expected_det

def main():
    parser = argparse.ArgumentParser(description='Build score card for completed pypeit reductions.\nAssumes the directory structure created by adap_reorg_setup.py')
    parser.add_argument("spec_name", type=str, help = "Name of the spectrograph for the reduction.")
    parser.add_argument("reorg_dir", type=str, help = "Root of directory structure created by adap_reorg_setup.py")
    parser.add_argument("outfile", type=str, help='Output csv file.')
    parser.add_argument("--commit", type=str, default = "", help='Optional, git commit id for the PypeIt version used')
    parser.add_argument("--mem", type=int, default=0, help="Optional, The maximum memory usage during the PypeIt reduction.")
    parser.add_argument("--status", type=str, default = None, help='Status of running the reduction')
    parser.add_argument("--subdirs", type=str, nargs='+', help="Specific subdirectories of the reorg_dir to work on." )
    parser.add_argument("--date_reduced", type=datetime.date.fromisoformat, default = datetime.date.today(), help="When the data was reduced. Defaults to today.")
    parser.add_argument("--rms_thresh", type=float, default=0.4)
    parser.add_argument("--flex_shift_thresh", type=float, default=4.0)
    parser.add_argument("--wave_cov_thresh", type=float, default=60.0)
    parser.add_argument("--lower_std_chi", type=float, default=0.6)
    parser.add_argument("--upper_std_chi", type=float, default=1.6)

    args = parser.parse_args()

    reorg_path = Path(args.reorg_dir)
    dirs_to_scan = []
    if args.subdirs is not None:
        dirs_to_scan = [reorg_path / subdir for subdir in args.subdirs]
    else:
        dirs_to_scan = reorg_path
        
    reduce_paths = []
    while len(dirs_to_scan) > 0:
        path = dirs_to_scan.pop()
        if not path.is_dir():
            continue
        if path.name.startswith("reduce"):
            reduce_paths.append(path)
        else:
            for child in path.iterdir():
                if child.is_dir():
                    dirs_to_scan.append(child)
    


    # Filename and table for writing the bad slit ids
    outpath = Path(args.outfile)
    bad_slits_outfile = outpath.parent / (outpath.stem + "_bad_slits" + outpath.suffix)
    bad_slits_data = Table(names=["slit_id", "spec2d"], dtype=["U11", "U80"])

    columns = ['dataset', 'science_file', 'date', 'status', 'bad_slit_count', 'det_count', 'slit_count', 'slit_std_chi_out_of_range', 
               'slit_wv_cov_under_thresh', 'slit_rms_over_thresh', 'total_bad_flags', 'bad_wv_count', 'bad_tilt_count', 'bad_flat_count', 
               'skip_flat_count', 'bad_reduce_count', 'object_count', 
               'obj_rms_over_thresh', 'object_flex_shift_over_thresh', 'object_without_opt_with_box', 'object_without_opt_wo_box', 
               'maskdef_extract_count', 'exec_time', 'mem_usage', 'git_commit', 'reduce_dir']
  
    pypeit_name = f"{args.spec_name}_A"

    data = Table(names = columns, dtype=['U64', 'U22', 'datetime64[D]', 'U8'] + [int for x in columns[4:-2]] + ['U40', 'U20'])
    stbm = SlitTraceBitMask()

    for reduce_path in reduce_paths:
        dataset = reduce_path.parent.relative_to(args.reorg_dir)
        pypeit_file = reduce_path / pypeit_name / f"{pypeit_name}.pypeit"
        log_path = str(reduce_path / pypeit_name / f"{pypeit_name}.log")
        science_path = reduce_path / pypeit_name / "Science"

        print(f"Searching {log_path} for execution time...")
        reduce_exec_time = get_exec_time(log_path)

        pf = PypeItFile.from_file(str(pypeit_file))
        par = PypeItPar.from_cfg_lines(pf.cfg_lines)

        expected_det = get_expected_det(par, args)

        science_idx = pf.data['frametype'] == 'science'
        for science_file in pf.data['filename'][science_idx]:
            data.add_row()
            data[-1]['dataset'] = dataset.parent 
            data[-1]['science_file'] = science_file
            data[-1]['status'] = args.status
            data[-1]['git_commit'] = args.commit
            data[-1]['reduce_dir'] = reduce_path.name
            data[-1]['date'] = args.date_reduced
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

                # Find slits that don't meet the wavelength coverage threshold, the rms threshold, or
                # that have bits set in the bitmask flags
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
                        # totals. A set of det:slitord_id (spat_id)s is used to prevent
                        # us from counting slits twice per science file
                        wave_sol_combined_ids = np.array([f"{det}:{spatid}" for spatid in spec2dobj.wavesol['SpatOrderID'][nonzero_rms_slits]])
                        total_bad_coverage.update(wave_sol_combined_ids[bad_coverage])
                        total_bad_slit_rms.update(wave_sol_combined_ids[bad_slit_rms])

                        combined_slit_ids = np.array([f"{det}:{spatid}" for spatid in spec2dobj.slits.slitord_id])
                        total_bad_wv_slits.update(combined_slit_ids[bad_wv_slits])
                        total_bad_tilt_slits.update(combined_slit_ids[bad_tilt_slits])
                        total_bad_flat_slits.update(combined_slit_ids[bad_flat_slits])
                        total_skip_flat_slits.update(combined_slit_ids[skip_flat_slits])
                        total_bad_reduce_slits.update(combined_slit_ids[bad_reduce_slits])
                        bad_chi_slits.update(combined_slit_ids[chis_out_of_range])
                        all_slit_ids.update(combined_slit_ids)

                except PypeItError:
                    print(f"Failed to load spec2d {spec2d_files[0]}")
                    data[-1]['status'] = 'FAILED'

                # Consider the dataset failed if the expected # of detectors were not reduced
                if data[-1]['det_count'] != expected_det:
                    print(f"Marking '{data[-1]['science_file']}' as failed. det_count {data[-1]['det_count']} does not match expected {expected_det}.")
                    data[-1]['status'] = 'FAILED'

                total_bad_flag_slits = total_bad_wv_slits | total_bad_tilt_slits | total_bad_flat_slits | total_bad_reduce_slits
                bad_slits =  total_bad_coverage | total_bad_slit_rms | bad_chi_slits | total_bad_flag_slits
                
                # Gather the bad_slits for writing out
                bad_slits_data = vstack((bad_slits_data, Table([list(bad_slits), [science_file] * len(bad_slits)], names=["slit_id", "spec2d"], dtype=["U11", "U80"])), join_type='exact')

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
                data[-1]['exec_time'] = reduce_exec_time
                data[-1]['mem_usage'] = args.mem


            spec1d_files = list(science_path.glob(f"spec1d_{science_stem}*.fits"))

            for spec1d_file in spec1d_files:
                print(f"Processing {spec1d_file}")
                try:
                    sobjs = SpecObjs.from_fitsfile(str(spec1d_file),chk_version=False)
                    data[-1]['object_count'] += len(sobjs)
                    data[-1]['maskdef_extract_count'] += np.count_nonzero(sobjs['MASKDEF_EXTRACT'])            

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

                        if specobj['FLEX_SHIFT_TOTAL'] is not None and np.fabs(specobj['FLEX_SHIFT_TOTAL']) > args.flex_shift_thresh:
                            data[-1]['object_flex_shift_over_thresh'] += 1

                except PypeItError:
                    print(f"Failed to load spec1d {spec1d_file}")
                    data[-1]['status'] = 'FAILED'


    data.sort(['status', 'dataset', 'science_file'])

    data.write(args.outfile, format='csv', overwrite=True)
    bad_slits_data.write(str(bad_slits_outfile), format='csv', overwrite=True)

if __name__ == '__main__':    
    main()

