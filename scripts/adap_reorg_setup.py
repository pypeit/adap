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

from metadata_info import config_path_grouping, is_metadata_complete,exclude_pypeit_types, exclude_metadata_funcs
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
    def __init__(self, start_date, end_date, metadata, setup_name):
        self.start_date = start_date
        self.end_date = end_date
        self.metadata = metadata
        self.setup_name = setup_name

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



def create_date_groups(args, metadata):
    """Return an array that can groups the entries in a configuration group by a date range."""

    date_groups = {}

    # Start the groups based on science files, and then find calibrations that match those
    sci_index = metadata.find_frames('science')
    unique_science_setups = np.unique(metadata[sci_index]['setup'])


    for setup_name in unique_science_setups:
        setup_index = metadata.find_configuration(setup_name)
        sci_files_in_setup = np.logical_and(sci_index, setup_index)
        date_groups[setup_name] = create_setup_date_groups(args, metadata, sci_files_in_setup,setup_name)

    # Now Add in the calibration files
    calib_index = np.logical_not(sci_index)
    for setup_name in unique_science_setups:
        setup_index = metadata.find_configuration(setup_name)
        calib_files_in_setup = np.logical_and(calib_index, setup_index)
        date_groups[setup_name] = create_setup_date_groups(args, metadata, calib_files_in_setup, setup_name, date_groups[setup_name])


    return date_groups

def create_setup_date_groups(args, metadata, setup_files, setup_name, date_groups=None):
    window_delta = TimeDelta(args.date_window, format='jd')

    for metadata_row in metadata[setup_files]:
        mjd = metadata_row['mjd']
        try:
            date = Time(mjd, format='mjd')
        except:
            write_to_report(args, f"Excluding {metadata_row['filename']} because of invalid MJD '{metadata_row['mjd']}'")
            continue

        if date_groups is None or len(date_groups) == 0:
            # Make a new date group for this file
            group_metadata = PypeItMetaData(metadata.spectrograph, metadata.par, data=metadata_row)
            date_groups = [DateGroup(date, date, group_metadata, setup_name)]
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
                        date_groups[dg_indx].metadata.table.add_row(metadata_row)
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
                group_metadata = PypeItMetaData(metadata.spectrograph, metadata.par, data=metadata_row)
                date_groups.append(DateGroup(date, date, group_metadata, setup_name))


    # Filter out Nones from merging before returning
    return [x for x in date_groups if x is not None]


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


def get_all_metadata(args, spectrograph, matching_files, local_files):
    metadata = PypeItMetaData(spectrograph, spectrograph.default_pypeit_par(), files=local_files, strict=False)
    instrument = spectrograph.header_name

    # Get the frame types for each file
    metadata.get_frame_types(flag_unknown=True)

    # Find the "unknown" files, and remove them
    unknown_files = np.where([filename.lstrip().startswith("#") for filename in metadata['filename']])

    for unknown_file in metadata[unknown_files]['filename']:
        write_to_report(args, "Excluding {unknown_file} because it has an unknown frame type")
    
    metadata.remove_rows(unknown_files)

    # Exclude unwanted frametypes
    
    if len(exclude_pypeit_types[instrument]) > 0:
        excluded_types = FrameTypeBitMask().flagged(metadata['framebit'],exclude_pypeit_types[instrument])
        excluded_by_type_rows = np.where(excluded_types)
        for i in excluded_by_type_rows:
            write_to_report(args, f"Excluding {metadata[i]['filename']} because it frame type {metadata[i]['frametype']}")

        metadata.remove_rows(excluded_by_type_rows)

    # If matching files was given, exclude science files that aren't matching
    if matching_files is not None:
        science_files = metadata.find_frames('science')
        non_matching_science_files = np.where([filename in matching_files['filename'] for filename in metadata[science_files]['filename']])[0]
        if len(non_matching_science_files) > 0:
            for filename in metadata[science_files][non_matching_science_files]['filename']:
                write_to_report(args, f"Excluding {filename} because it is not a matching science file.")
            metadata.remove_rows(non_matching_science_files)

    # Run instrument specific exclude logic
    if instrument in exclude_metadata_funcs:
        excluded, reasons = exclude_metadata_funcs[instrument](metadata)
        if len(excluded > 0):
            for i, metadata_indx in enumerate(excluded):
                write_to_report(args, f"Excluding {metadata[metadata_indx]['filename']} of type {metadata[metadata_indx]['frametype']} because {reasons[i]}")
            metadata.remove_rows(excluded)

    # Now group into configurations
    metadata.set_configurations()

    # Report on calibration files that don't match any science files
    science_files = metadata.find_frames('science')
    unique_science_setup_names = np.unique(metadata[science_files]['setup'])
    calib_files = np.logical_not(science_files)
    non_matching_calib_files = np.ones_like(calib_files, dtype=bool)
    for setup_name in unique_science_setup_names:
        setup_files = metadata.find_configuration(setup_name)
        setup_calib_files = np.logical_and(setup_files,calib_files)
        non_matching_calib_files = np.logical_and(non_matching_calib_files, np.logical_not(setup_calib_files))

    for filename in metadata[non_matching_calib_files]['filename']:
        write_to_report(args, f"Excluding {filename} because it does not match the configuration of any desired science files.")
    return metadata

def get_config_dir_paths(args, metadata):

    spectrograph = metadata.spectrograph
    instrument = spectrograph.header_name

    unique_configs = metadata.unique_configurations()
    all_paths = set()
    config_dir_names = {}
    for config, config_dict in unique_configs.items():

        # The config dir name always starts with the instrument
        config_path = Path(instrument)
        for grouping in config_path_grouping[instrument]:
            grouping_strings = []
            for part in grouping:
                key=part[0]
                type = part[1]
                if key not in config_dict:
                    value = "None"
                else:
                    value = config_dict[key]
                    if key=='binning':
                        # The comma causes some issues in the directory name, so replace with an x
                        value = value.replace(",", "x")
                    elif type == 'float64':
                        # Use the spectrograph rtol value to figure out how to round to get a nice string value for the
                        # directory name
                        rtol = spectrograph.meta[key].get('rtol', None)
                        if rtol is not None:
                            value = round(value,int(np.abs(np.log10(rtol))))
                        else:
                            # Otherwise just round to a whole number
                            value=round(value)
                        # Convert to a string for use in the directory name
                        value = f"{value:g}"
                    else:
                        value = str(value)
                grouping_strings.append(value)
            config_path = config_path / "_".join(grouping_strings)
        
        if config_path in all_paths:
            # The names should be unique, but there are edge cases where they aren't. (like the rounding of floating point #s 
            # not matching how PypeItMetadata groups stuff). So now we make sure it's unique
            i = 1
            new_name = config_path
            while new_name in all_paths:
                new_name = config_path.with_name(config_path.name + f"_{i}")
                i+=1
            config_path = new_name

        all_paths.add(config_path)                
        config_dir_names[config] = config_path
    return config_dir_names
                
                    



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
            value = spectrograph.get_meta_value(headarr, key, required=True)
            if key in ["dispangle"] and dtype == "float64":
                # This is how I handled DEIMOS, I'm not sure if it's the best way
                value = round(value)               

            elif key == "binning" and isinstance(value,str):
                # The comma causes some issues in the directory name, so replace with an x
                value = value.replace(",", "x")
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
                                                'based on configuration keys.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('spectrograph_name', type=str, help="The name of the spectrograph that created the raw data.")
        parser.add_argument('out_dir', type=str, help="Output directory where the organized directry tree of files will be created.")
        parser.add_argument('source_dirs', type=str, nargs='+', help="One or more source directories containing raw data files.")
        parser.add_argument('--matching_file_list', type=Path, default=None, help='A list of science files matching desired targets. Only datasets containing these files will be transferred.')
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
        if os.path.exists(args.report):
            os.unlink(args.report)
        start_time = datetime.datetime.now()
        write_to_report(args, f"adap_reorg_setup.py started at {start_time.isoformat()}.")
        write_to_report(args,  "--------------------------------------------------------")

        if args.matching_file_list is not None:
            matching_files = Table(dtype=[('filename', 'U')])
            with open(args.matching_file_list, "r") as f:
                for line in f:
                    matching_files.add_row([line.strip()])
        else:
            matching_files = None

        spectrograph = load_spectrograph(args.spectrograph_name)
        instrument = spectrograph.header_name
        cfg_lines = ['[rdx]', 'ignore_bad_headers = True', f'spectrograph = {args.spectrograph_name}']
        default_cfg = spectrograph.default_pypeit_par().to_config()
        par = PypeItPar.from_cfg_lines(cfg_lines=default_cfg, merge_with=cfg_lines)

        # Get the Path/RClonePath objects for the source files
        files = list(get_files(args))

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

        # Group the files in datasets for reduction.
        # 
        # First read the metadata for the files.
        # This process will remove unwanted files
        all_metadata = get_all_metadata(args, spectrograph, matching_files, local_files)

        # This will have grouped them into configs, convert these into the directory names we'll use
        config_dir_names = get_config_dir_paths(args, all_metadata)

        all_groups = create_date_groups(args, all_metadata)

        for setup_name in all_groups.keys():
            for date_group in all_groups[setup_name]:

                is_complete, file_counts = is_metadata_complete(date_group.metadata,instrument)
                complete_str = "complete" if  is_complete else "incomplete"
                dataset_path = config_dir_names[date_group.setup_name] / date_group.get_dir_name()
                if not is_complete:
                    write_to_report(args, f"Incomplete dataset {dataset_path} files: {file_counts}")

                dest_path = dest_root / dataset_path / complete_str / "raw"

                for row in date_group.metadata:
                    transfer_file(args, f"{row['directory']}/{row['filename']}", dest_path)

        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    ReorgSetup.entry_point()
