import sys
import argparse
from pathlib import Path
from time import sleep
import re

import numpy as np
from astropy.table import Table, vstack
from astropy.time import Time

from extended_spec_mixins import ADAPSpectrographMixin
from date_group import DateGroup
from pykoa.koa import Koa
import metadata_info as mi

def parse_args():
    parser = argparse.ArgumentParser(description='Find and download raw data for science files queried from koa.')
    parser.add_argument('spectrograph_name', type=str, help="The name of the spectrograph.")
    parser.add_argument('out_dir', type=Path, help="Output directory where the query results are downloaded.")
    parser.add_argument('koaids', type=Path, default=None, help="File containing a list of koaids to of science frames to start with.")
    parser.add_argument('--date_window', type=float, default=3.0, help="How long a time range to use when grouping files. Measured in days. Defaults to 3 days.")
    parser.add_argument('--std_as_calib', default=False, action="store_true", help="Whether to consider standard observations as calibration files")
    parser.add_argument('--download', default=False, action="store_true", help="Whether or not to download the files after finding them. Defaults to False.")
    parser.add_argument('--raw_data_dir', type=Path, default=None, help="Where to download the raw data. If not specified, the out_dir argument is used.")
    return parser.parse_args()

def main(args):
    extended_spec = ADAPSpectrographMixin.load_extended_spectrograph(spec_name=args.spectrograph_name, matching_files = None)

    if not args.koaids.exists():
        print(f"Input koaids '{args.input_query_results}' file does not exist.", file=sys.stderr)
        return 1

    koaids = read_koaid_file(args.koaids)
    if len(koaids) == 0:
        print(f"Input koaids file is empty.",file=sys.stderr)
        return 1

    if not args.out_dir.exists():
        args.out_dir.mkdir(parents=True,exist_ok=True)
        
    if not args.raw_data_dir:
        args.raw_data_dir = args.out_dir            
    else:
        if not args.raw_data_dir.exists():
            args.raw_data_dir.mkdir(parents=True,exist_ok=True)
            
    # Query for the science files
    koaid_query_results = koa_query_koaids(koaids,extended_spec.instrument_name,args.out_dir)

    date_groups = list[DateGroup]()
    for i in range(len(koaid_query_results)):
        file_date = Time(koaid_query_results[i]["date_obs"],format="iso")
        found = False
        for dg in date_groups:
            if dg.is_date_in_window(file_date):
                dg.add_metadata_row(koaid_query_results[i],file_date)
                found=True
        if found is False:
            date_groups.append(DateGroup(args,file_date,koaid_query_results[i:i+1]))

    print(f"Grouped {len(koaid_query_results)} files into {len(date_groups)} date groups")

    if args.std_as_calib:
        types_to_exclude = mi.exclude_koa_types[extended_spec.instrument_name]
    else:
        types_to_exclude = mi.exclude_koa_types[extended_spec.instrument_name] + ['object']

    files_to_download = Table(names=["koaid","instrume","filehand","koaimtyp","targname","echangl","xdangl","binning"],dtype=[mi.KOA_ID_DTYPE, "<U10", "<U256", "<U6", "<U15",float,float,"<U6"])
    for dg in date_groups:
        # Put the science/standard files into the files to download
        for dg_file in dg.metadata:
            files_to_download.add_row([dg_file["koaid"], dg_file["instrume"], dg_file["filehand"], dg_file["koaimtyp"],dg_file["targname"],dg_file["echangl"],dg_file["xdangl"],dg_file["binning"]])

        query_output = koa_query_dg(extended_spec.instrument_name, args.out_dir, dg, types_to_exclude)

        query_output_table = Table.read(str(query_output), format="ipac")
        if args.std_as_calib:
            query_output_table = trim_std_files(query_output_table)

        trim_calib_files(extended_spec, files_to_download, query_output_table, dg.metadata)

    files_to_download.write(str(args.out_dir / "files_to_download.csv"),format="csv",overwrite=True)

def read_koaid_file(filename:Path):
    koaids = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if len(line) == 0:
                continue
            koaids.append(line)
    return koaids

def koa_query_koaids(koaids: list[str], instr: str, out_dir:Path) -> Table:
    fields = ["koaid"] + mi.common_observation_columns + mi.common_program_columns + mi.instr_columns[instr]
    batch_size = 100
    batch_start = 0
    next_batch = koaids[batch_start:batch_start+batch_size]
    result_table = None
    while len(next_batch) > 0:
        koaid_strings = ["'" + koaid + "'" for koaid in koaids]
        query = f"select {','.join(fields)} from koa_{instr.lower()} where koaid in ({','.join(koaid_strings)})"
        outname = out_dir / "temp_science_query_results.ipac"
        print(query)
        Koa.query_adql(query, str(outname), overwrite=True, format="ipac")  # save table in tabname
        # Sleep to be nice and not hit koa with too man queries too quickly
        sleep(2)
        query_output_table= Table.read(str(outname), format="ipac")
        if result_table is None:
            result_table = query_output_table
        else:
            result_table = vstack([result_table,query_output_table])
        outname.unlink(missing_ok=True)
        batch_start +=batch_size
        next_batch = koaids[batch_start:batch_start+batch_size]

    return result_table


def koa_query_dg(instr : str, out_dir : Path, dg:DateGroup, exclude_types : list[str]) -> Path:

    fields = ["koaid"] + mi.common_observation_columns + mi.common_program_columns + mi.instr_columns[instr]
    # Exclude unwanted types, and "object" files which are not calibration files
    exclude_types = [f"'{t}'" for t in exclude_types]
    start_date, end_date = dg.window
    query = f"select {','.join(fields)} from koa_{instr.lower()} where date_obs BETWEEN TO_TIMESTAMP('{start_date.to_value('iso', subfmt='date')} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') and TO_TIMESTAMP('{end_date.to_value('iso', subfmt='date')} 23:59:59','YYYY-MM-DD HH24:MI:SS') and koaimtyp not in ({','.join(exclude_types)})"
    outname = out_dir / (dg.get_dir_name() + ".ipac")
    if outname.exists():
        print(f"Skipping existing {outname.name}")
        return outname
    print(query)
    Koa.query_adql(query, str(outname), overwrite=True, format="ipac")  # save table in tabname

    # Sleep to be nice and not hit koa with too man queries too quickly
    sleep(2)
    return outname


def trim_std_files(query_output_table :Table) -> Table:
    # Trim any "object" files to be known standard stars

    std_star_regexes = [r'feige *110([^\d]|$)',
                        r'feige *34([^\d]|$)',
                        r'hz *44([^\d]|$)',
                        r'g191-*b2b',
    ]
    object_files = query_output_table["koaimtyp"] == 'object'
    calib_files = np.logical_not(object_files)
    std_files = False ** len(query_output_table['targname'])
    for regex in std_star_regexes:
        matches = [re.search(regex,str(targname), re.IGNORECASE) is not None for targname in query_output_table['targname']]
        std_files = np.logical_or(std_files, matches)

    return query_output_table[np.logical_or(calib_files, np.logical_and(object_files, std_files))]

def trim_calib_files(ex_spec, files_to_download, query_output_table : Table, dg_metadata):

    for query_file in query_output_table:
        for dg_file in dg_metadata:
            if ex_spec.koa_config_compare(query_file, dg_file):
                if query_file["koaid"] not in files_to_download["koaid"]:
                    files_to_download.add_row([query_file["koaid"], query_file["instrume"], query_file["filehand"], query_file["koaimtyp"], query_file["targname"], query_file["echangl"],query_file["xdangl"],query_file["binning"]])
    


if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))