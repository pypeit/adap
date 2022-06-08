#!/usr/bin/env python3

import os
import argparse
from pathlib import Path
from pypeit.pypmsgs import PypeItError
import numpy as np
from astropy.stats import mad_std

from pypeit.specobjs import SpecObjs
from pypeit.spec2dobj import AllSpec2DObj

def get_1d_std_chis_out_of_range(sobjs, lower_thresh, upper_thresh):
    num_out_of_range = 0
    for sobj in sobjs:
        ratio = sobj['OPT_COUNTS'] / sobj['OPT_COUNTS_SIG']
        std_chi = mad_std(ratio)
        if std_chi < lower_thresh or std_chi > upper_thresh:
            num_out_of_range += 1
    
    return num_out_of_range

def main():
    parser = argparse.ArgumentParser(description='Build score card for completed pypeit reductions.\nAssumes the directory structure created by adap_reorg_setup.py')
    parser.add_argument("reorg_dir", type=str, help = "Root of directory structure created by adap_reorg_setup.py")
    parser.add_argument("outfile", type=str, help='Output csv file.')
    parser.add_argument("--masks", type=str, nargs='+', help="Specific masks to run on" )
    parser.add_argument("--rms_thresh", type=float, default=0.2)
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
                reduce_path = date_path.joinpath('complete', 'reduce')
                if reduce_path.exists():
                    reduce_paths.append(reduce_path)

    columns = ['bad_slit_count', 'spec2d_count', 'slit_count', 'spec1d_slit_count', 'slit_rms_over_thresh', 'no_chi_value_slits' , 'slit_std_chi_out_of_range', 'spec1d_count',  'object_count', 'object_without_opt_with_box', 'object_without_opt_wo_box', 'maskdef_extract_count', 'rms_thresh_no_chi_overlap']
    data = dict()
    for reduce_path in reduce_paths:
        reduce_key = str(reduce_path.parent.parent)
        data[reduce_key] = {x: 0 for x in columns}
        for file in reduce_path.glob("Science/spec2d*.fits"):
            print(f"Processing file {file}")
            bad_slits = set()
            try:
                allspec2d = AllSpec2DObj.from_fits(str(file), chk_version=False)
                data[reduce_key]['spec2d_count'] += 1
                for det in allspec2d.detectors:
                    spec2dobj = allspec2d[det]
                    std_chis = spec2dobj['std_chis']
                    med_chis = spec2dobj['med_chis']                
                    data[reduce_key]['slit_count'] += len(std_chis)
                    no_chis = (std_chis == 0.0) & (med_chis==0.0)
                    num_no_chis = np.sum(no_chis)
                    data[reduce_key]['no_chi_value_slits'] += num_no_chis
                    chis_out_of_range = (std_chis<args.lower_std_chi) | (std_chis>args.upper_std_chi)
                    data[reduce_key]['slit_std_chi_out_of_range'] += np.sum(chis_out_of_range) - num_no_chis
                    bad_slits.update(spec2dobj.slits.slitord_id[no_chis | chis_out_of_range])
    
            except PypeItError:
                print(f"Failed to load spec2d {file}")

            spec1d_file = file.parent.joinpath(file.name.replace("spec2d_", "spec1d_", 1))

            print(f"Processing {file}")
            try:
                sobjs = SpecObjs.from_fitsfile(str(spec1d_file),chk_version=False)
                data[reduce_key]['spec1d_count'] += 1
                data[reduce_key]['object_count'] += len(sobjs)
                data[reduce_key]['maskdef_extract_count'] += np.sum(sobjs['MASKDEF_EXTRACT'])            

                # We want the slit RMS, but those aren't stored in the spec2d
                # In the spec1d they are stored per object, so there could be multiple (identical) RMS values
                # per slit. So we store the values in a map that filters out duplicates. We
                # also use the detector as part of the key for consistency for how we count slits
                # in the spec2d
                slit_rms_map = dict()
                for specobj in sobjs:
                    slit_rms_map[specobj['DET'] + '_' + str(specobj['SLITID'])] = specobj['WAVE_RMS']
                    if specobj['OPT_COUNTS'] is None:
                        if specobj['BOX_COUNTS'] is not None:
                            data[reduce_key]['object_without_opt_with_box'] += 1
                            print(f"File {file} object {specobj['NAME']} is missing OPT_COUNTS but has BOX_COUNTS.")
                        else:
                            data[reduce_key]['object_without_opt_wo_box'] += 1
                            print(f"File {file} object {specobj['NAME']} is missing OPT_COUNTS and BOX_COUNTS.")

                unique_rms_values = np.array(list(slit_rms_map.values()))
                unique_slit_ids =   np.array(list(slit_rms_map.keys()))
                data[reduce_key]['spec1d_slit_count'] += len(unique_slit_ids)
                slits_over_thresh = unique_rms_values > args.rms_thresh
                data[reduce_key]['slit_rms_over_thresh'] += np.sum(slits_over_thresh)
                rms_over_thresh = set(unique_slit_ids[slits_over_thresh])
                rms_over_thresh_and_no_chi = rms_over_thresh.intersection(bad_slits)
                data[reduce_key]['rms_thresh_no_chi_overlap'] += len(rms_over_thresh_and_no_chi)
                bad_slits.update(unique_slit_ids[slits_over_thresh])
            except PypeItError:
                print(f"Failed to load spec1d {spec1d_file}")


            data[reduce_key]['bad_slit_count'] += len(bad_slits)

    with open(args.outfile, "w") as f:
        print (f"dataset,{','.join(columns)}", file=f)
        for key in data.keys():
            print(f"{key},{','.join([str(data[key][x]) for x in columns])}", file=f)



if __name__ == '__main__':    
    main()

