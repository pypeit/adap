#!/usr/bin/env python3
"""
Given a set of source directories in S3 or local storage that contain raw
DEIMOS FITs file, organize them into a heirarchy and run setup on them.
The resulting heirarchy will look like:

<slit mask name>/
    <Grating_Dispangle_Filter>/
        YYYY-MM-DD_YYYY-MM-DD/
            complete/
                raw/
                    <raw data files>
            incomplete/
                raw/

Where "complete" folders have enough data to be reduced with PypeIt, and
"incomplete" folders do not.                


Example Usage:

To organize a local RAW_DATA1 and RAW_DATA2 directory to raw_organized:
    adap_reorg_setup.py raw_organized RAW_DATA1 RAW_DATA2

Requres: rclone to access a cloud resource.

"""
import traceback
import os
import shutil
import datetime 
from pathlib import Path

from astropy.table import Table
from astropy.time import Time, TimeDelta
import numpy as np

from pypeit.scripts import scriptbase
from pypeit.spectrographs.util import load_spectrograph
from pypeit.metadata import PypeItMetaData
from pypeit.par.pypeitpar import PypeItPar
from pypeit.pypeitsetup import PypeItSetup
from pypeit.core.framematch import FrameTypeBitMask

from metadata_info import config_path_grouping, is_metadata_complete,exclude_pypeit_types
from rclone import RClonePath
    


def write_to_report(args, msg, exc_info=False):
    """Write a message to the report file, creating it if it doesn't exist. """
    if os.path.dirname(args.report) != '' and not os.path.exists(os.path.dirname(args.report)):
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
    if exc_info:
        msg = msg + "\n" + traceback.format_exc()

    with open(args.report, "a") as erf:
        print(msg,file=erf)

class DateGroup:
    def __init__(self, start_date, end_date, local_files):
        self.start_date = start_date
        self.end_date = end_date
        self.local_files = local_files

    def add_date(self, date):
        if date < self.start_date:
            self.start_date = date
        elif date > self.end_date:
            self.end_date = date

    def merge(self, other_dg):
        if other_dg.start_date < self.start_date:
            self.start_date = other_dg.start_date

        if other_dg.end_date > self.end_date:
            self.end_date = other_dg.end_date

        self.local_files += other_dg.local_files

    def get_dir_name(self):
        start_date_text = self.start_date.to_value('iso', subfmt='date')
        end_date_text   = self.end_date.to_value('iso', subfmt='date')
        if start_date_text != end_date_text:
            return  start_date_text + "_" + end_date_text
        else:
            return start_date_text

def create_date_groups(args, all_date_groups, cfg_group_key, cfg_group):
    """Return an array that can groups the entries in a configuration group by a date range."""

    window_delta = TimeDelta(args.date_window, format='jd')

    # Because files are processed in batches, make sure to compare against date groups
    # from all previous batches.  combined_date_groups has the group integer values form
    # previous batches, and combined_dates has the date that correspond to that group.
    if cfg_group_key not in all_date_groups:
        all_date_groups[cfg_group_key] = []

    date_groups = all_date_groups[cfg_group_key]

    # Go through the files in this cfg group
    for mjd, local_file in cfg_group['mjd','local_file']:
        try:
            date = Time(mjd, format='mjd')
        except:
            # We have to keep an entry for this so that the column
            # remains the same length as the table
            date = Time('1970-01-01', format='mjd')

        if len(date_groups) == 0:
            # Make a new date group for this file
            date_groups.append(DateGroup(date, date, [local_file]))
        else:
            # Find an existing date group for this file
            group = None
            for dg_indx in range(len(date_groups)):
                if date_groups[dg_indx] is None:
                    # Skip groups that were set to None by merging
                    continue

                # Find the date window for this group and see if the current date
                # fits into it
                start = date_groups[dg_indx].start_date - window_delta
                end = date_groups[dg_indx].end_date + window_delta
                if date >= start and date <= end:
                    if group is None:
                        # The first group that matched this file
                        date_groups[dg_indx].add_date(date)
                        date_groups[dg_indx].local_files.append(local_file)
                        group = dg_indx
                    else:
                        # The new date belongs to two groups, so those groups should be merged
                        
                        # Merge new group into prior group
                        date_groups[group].merge(date_groups[dg_indx])
                                                                       
                        # Remove the old group, but because we're iterating through the list
                        # just set it to None
                        date_groups[dg_indx] = None                      

            if group is None:
                # No match was found, start a new group
                date_groups.append(DateGroup(date, date, [local_file]))


    # Filter out Nones from merging before returning
    all_date_groups[cfg_group_key] = [x for x in date_groups if x is not None]


def download_files(args, files):
    args.local_out.mkdir(parents=True,exist_ok=True)
    local_files = []
    for file in files:
        try:
            file.download(args.local_out)
            local_files.append(args.local_out / file.name)
        except Exception as e:
            write_to_report(args,f"Failed to download {file}", exc_info=True)
    return local_files

def group_files(args, spectrograph, local_files):
    all_groups = dict()
    for config_group in group_by_config(args, spectrograph, local_files):
        cfg_group_key = config_group['config_group_key'][0]
        create_date_groups(args, all_groups, cfg_group_key, config_group)

    return all_groups

def group_by_config(args, spectrograph, local_files):
    grouping_table = Table(names=["config_group_key","local_file", "mjd"],
                           dtype=["<U",              "<U",        "float64"])

    for local_file in local_files:
        try:
            config_metadata, mjd = read_grouping_metadata(args,spectrograph,local_file)
        except Exception as e:
            write_to_report(args, f"Failed to get metadata from {local_file}.",exc_info=True)
            raise
        config_group_key = "/".join(["_".join(group) for group in config_metadata])
        grouping_table.add_row([config_group_key, str(local_file), mjd])

    return grouping_table.group_by("config_group_key").groups


def read_grouping_metadata(args, spectrograph, local_file):
    """Read metadata from a file needed to group it."""
    instrument = spectrograph.header_name
    print(f"Getting grouping metadata for {local_file}")
    headarr = spectrograph.get_headarr(str(local_file))
    mjd = spectrograph.get_meta_value(headarr, "mjd", required=True)
    metadata = []
    for group in config_path_grouping[instrument]:
        group_metadata = []
        for key, dtype in group:
            # TODO is this the best way to handle dispangle and similar things
            if key == "dispangle" and dtype == "float64":
                value = round(spectrograph.get_meta_value(headarr, key, required=True))
            else:
                value = spectrograph.get_meta_value(headarr, key, required=True)
            group_metadata.append(str(value))
        metadata.append(group_metadata)
    
    return metadata, mjd


def get_files(args):
    """Find all of the source fits files, whether in S3 or local."""
    for source_dir in args.source_dirs:
        if args.source != "local":
            source_path = RClonePath(args.source, source_dir)
        else:
            source_path = Path(source_dir)
        for file in source_path.rglob("*.fits"):
            yield file
        for file in source_path.rglob("*.fits.gz"):
            yield file

def transfer_file(args, file, dest):

    try:
        source_path = Path(file)
        if args.dest == "local":
            dest.mkdir(parents=True,exist_ok=True)
            dest_file = dest / source_path.name
            if args.source == "local":
                # Copying local to local, we can either symlink, copy or move
                if args.symlink:
                    if dest_file.exists() and dest_file.is_symlink():
                        # Remove old links
                        dest_file.unlink()
                    dest_file.symlink_to(source_path, target_is_directory=False)
                elif args.move_files:
                    shutil.move(source_path,dest_file)
                else:
                    shutil.copy2(source_path, dest_file)        
            else:
                # Remote file to local destination, remove temporary file
                shutil.move(source_path, dest_file)
        else:
            # Upload to Remote destination
            dest.upload(source_path)
            # Remove source if they are temporary files, or if "move_files" was given
            if args.source !="local" or args.move_files:
                source_path.unlink()
    except Exception as e:
        write_to_report(args,f"Failed to transfer file {args.source} {file} to {args.dest} {dest}.", exc_info=True)


class ReorgSetup(scriptbase.ScriptBase):

    @classmethod
    def get_parser(cls, width=None):

        parser = super().get_parser(description='Organize raw data files into directories '
                                                'based on configuration keys and date and run '
                                                'pypeit_setup on them.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('spectrograph_name', type=str, help="The name of the spectrograph that created the raw data.")
        parser.add_argument('out_dir', type=str, help="Output directory where the organized directry tree of files will be created.")
        parser.add_argument('source_dirs', type=str, nargs='+', help="One or more source directories containing raw data files.")
        parser.add_argument('--source', type=str, default="local", choices=["local","s3", "gdrive"], help='The source cloud where the are stored, or "local" for a files on the local system.')
        parser.add_argument('--dest', type=str, default="local", choices=["local","s3", "gdrive"], help='The cloud the files will be are copied to, or "local" for the local system.')
        parser.add_argument('--date_window', type=float, default=3.0, help="How long a time range to use when grouping files. Measured in days. Defaults to 3 days.")
        parser.add_argument('--move_files', default=False, action="store_true", help="Whether or not to move the files from their original location instead of copying. Defaults to false.")
        parser.add_argument('--symlink', default=False, action="store_true", help="Use symlinks instead of copying files. Both the source and destination must be on the local machine.")
        parser.add_argument('--local_out', type=Path, default=Path("adap_setup_tmp"), help="A temporary directory used when working with files onthe cloud. Defaults to 'adap_setup_tmp'.")
        parser.add_argument('--report', type=str, default="reorg_report_file.txt", help="Name of the report file to create detailing any issues that occurred when running this script. Defaults to 'reorg_report_file.txt'.")

        return parser

    @staticmethod
    def main(args):
        start_time = datetime.datetime.now()
        spectrograph = load_spectrograph(args.spectrograph_name)
        instrument = spectrograph.header_name
        cfg_lines = ['[rdx]', 'ignore_bad_headers = True', f'spectrograph = {args.spectrograph_name}']
        default_cfg = spectrograph.default_pypeit_par().to_config()
        par = PypeItPar.from_cfg_lines(cfg_lines=default_cfg, merge_with=cfg_lines)

        # Get the Path/RClonePath objects for the source files
        files = get_files(args)         

        if args.source != "local":
            # Download remote files to a temporary directory
            local_files = download_files(args, files)
        else:
            local_files = files

        # Get the destination root, either local or remote
        if args.dest != "local":
            dest_root = RClonePath(args.dest, args.out_dir)
        else:
            dest_root = Path(args.out_dir)
            dest_root.mkdir(exist_ok=True, parents=True)

        # Group the files in DateGroups, indexed by a string representing the
        # combined metadata values needed for to group by configuration
        # The string is formatted as a relative directory path
        groups = group_files(args, spectrograph, local_files)
        for config_dir in groups.keys():
            config_path = dest_root / config_dir
            for date_group in groups[config_dir]:
                # Determine if the date group is "complete" to decide on the destination path name
                metadata = PypeItMetaData(spectrograph, par, files=date_group.local_files)
                metadata.get_frame_types(flag_unknown=True)
                is_complete, file_counts = is_metadata_complete(metadata,instrument)
                complete_str = "complete" if  is_complete else "incomplete"
                if not is_complete:
                    write_to_report(args, f"Incomplete dataset {config_path / date_group.get_dir_name()} files: {file_counts}")

                dest_path = config_path / date_group.get_dir_name() / complete_str / "raw"
                # Exclude any frames appropriate for this instrument
                if len(exclude_pypeit_types[instrument]) > 0:
                    excluded_types = FrameTypeBitMask().flagged(metadata['framebit'],exclude_pypeit_types[instrument])
                else:
                    excluded_types = np.zeros_like(metadata['framebit'],dtype=bool)
                # Also exclude unknown types
                unknown_types = [True if filename.startswith("#") else False for filename in metadata['filename']]
                excluded_rows = np.logical_or(excluded_types, unknown_types)
                rows_to_transfer = np.logical_not(excluded_rows)
                for row in metadata[excluded_types]:
                    write_to_report(args,f"Excluding {row['frametype']} file {row['directory']}/{row['filename']} for instrument {instrument}")
                for row in metadata[unknown_types]:
                    write_to_report(args,f"Excluding unknown type file {row['directory']}/{row['filename'].replace('# ','')} for instrument {instrument}")
                for row in metadata[rows_to_transfer]:
                    transfer_file(args, f"{row['directory']}/{row['filename']}", dest_path)

        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    ReorgSetup.entry_point()
