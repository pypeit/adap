import sys
import argparse
from pathlib import Path
from time import sleep

import numpy as np
from astropy.table import Table
from astropy.time import Time

from extended_spec_mixins import ADAPSpectrographMixin
from date_group import DateGroup
from pykoa.koa import Koa
import metadata_info as mi

def parse_args():
    parser = argparse.ArgumentParser(description='Find and download raw data for science files queried from koa.')
    parser.add_argument('spectrograph_name', type=str, help="The name of the spectrograph.")
    parser.add_argument('out_dir', type=Path, help="Output directory where the query results are downloaded.")
    parser.add_argument('input_query_results', type=Path, help="A KOA query output file containing the science files (and enough info to download them).")
    parser.add_argument('--koaids', type=str, nargs='+', default=None, help="File containing a list of koaids to filter the input query results by. Only thses KOA ids will be considered. If not specified, all files in the query output will be considered.")
    parser.add_argument('--date_window', type=float, default=3.0, help="How long a time range to use when grouping files. Measured in days. Defaults to 3 days.")
    parser.add_argument('--download', default=False, action="store_true", help="Whether or not to download the files after finding them. Defaults to False.")
    parser.add_argument('--raw_data_dir', type=Path, default=None, help="Where to download the raw data. If not specified, the out_dir argument is used.")
    return parser.parse_args()

def main(args):
    extended_spec = ADAPSpectrographMixin.load_extended_spectrograph(spec_name=args.spectrograph_name, matching_files = None)

    if not args.input_query_results.exists():
        print(f"Input query results '{args.input_query_results}' file does not exist.", file=sys.stderr)
        return 1
    format = args.input_query_results.suffix[1:]
    input_query_results = Table.read(args.input_query_results, format=format)

    if not args.out_dir.exists():
        args.out_dir.mkdir(parents=True,exist_ok=True)
        
    if args.koaids is not None:
        filter = [result['koaid'] in args.koaids for result in input_query_results]
        input_query_results = input_query_results[filter]

    if not args.raw_data_dir:
        args.raw_data_dir = args.out_dir            
    else:
        if not args.raw_data_dir.exists():
            args.raw_data_dir.mkdir(parents=True,exist_ok=True)
            
    date_groups = list[DateGroup]()
    for i in range(len(input_query_results)):
        file_date = Time(input_query_results[i]["date_obs"],format="iso")
        found = False
        for dg in date_groups:
            if dg.is_date_in_window(file_date):
                dg.add_metadata_row(input_query_results[i],file_date)
                found=True
        if found is False:
            date_groups.append(DateGroup(args,file_date,input_query_results[i:i+1]))

    print(f"Grouped {len(input_query_results)} files into {len(date_groups)} date groups")

    files_to_download = Table(names=["koaid","instrume","filehand"],dtype=[mi.KOA_ID_DTYPE, "<U10", "<U256"])    
    for dg in date_groups:
        # Put the science/standard files into the files to download
        for dg_file in dg.metadata:
            files_to_download.add_row([dg_file["koaid"], dg_file["instrume"], dg_file["filehand"]])

        query_output = koa_query(extended_spec.instrument_name, args.out_dir, dg)
        trim_calib_files(extended_spec, files_to_download, query_output, dg.metadata)

    files_to_download.write(str(args.out_dir / "files_to_download.csv"),format="csv",overwrite=True)

def koa_query(instr : str, out_dir : Path, dg:DateGroup) -> Path:

    fields = ["koaid"] + mi.common_observation_columns + mi.common_program_columns + mi.instr_columns[instr]
    # Exclude unwanted types, and "object" files which are not calibration files
    exclude_types = [f"'{t}'" for t in mi.exclude_koa_types[instr]] + ["'object'"]
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

def trim_calib_files(ex_spec, files_to_download, query_output : Path, dg_metadata):
    query_output_table = Table.read(str(query_output), format="ipac")

    for query_file in query_output_table:
        for dg_file in dg_metadata:
            if ex_spec.koa_config_compare(query_file, dg_file):
                if query_file["koaid"] not in files_to_download["koaid"]:
                    files_to_download.add_row([query_file["koaid"], query_file["instrume"], query_file["filehand"]])
    


if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))