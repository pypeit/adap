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

from astropy.table import Table, Column, vstack
from astropy.time import Time, TimeDelta
import numpy as np

from pypeit.scripts import scriptbase
from pypeit.metadata import PypeItMetaData
from pypeit.par.pypeitpar import PypeItPar
from pypeit.pypeitsetup import PypeItSetup
from pypeit.core.framematch import FrameTypeBitMask

from metadata_info import ADAPSpectrographMixin
from rclone import RClonePath
    


def write_to_report(args, msg, exc_info=False):
    """Write a message to the report file, creating it if it doesn't exist. """
    if os.path.dirname(args.report) != '' and not os.path.exists(os.path.dirname(args.report)):
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
    if exc_info:
        msg = msg + "\n" + traceback.format_exc()

    with open(args.report, "a") as erf:
        print(msg,file=erf)

class DateGroup():
    def __init__(self, args, mjd, metadata):
        self.metadata = metadata
        self.window_delta = TimeDelta(args.date_window, format='jd')
        self.start_date = mjd
        self.end_date = mjd

    def add_metadata_row(self, row, mjd):
        self._add_date(mjd)
        self.metadata.table.add_row(row)
    
    def _add_date(self, mjd):
        if mjd < self.start_date:
            self.start_date = mjd
        elif mjd > self.end_date:
            self.end_date = mjd

    def is_date_in_window(self, mjd):
        # Find the date window for this group and see if the given date
        # fits into it
        start = self.start_date - self.window_delta
        end = self.end_date + self.window_delta
        return mjd >= start and mjd <= end

    def merge(self, other_dg):
        if other_dg.start_date < self.start_date:
            self.start_date = other_dg.start_date

        if other_dg.end_date > self.end_date:
            self.end_date = other_dg.end_date

        self.metadata.table = vstack([self.metadata.table, other_dg.metadata.table])

    def get_dir_name(self):
        start_date_text = self.start_date.to_value('iso', subfmt='date')
        end_date_text   = self.end_date.to_value('iso', subfmt='date')
        if start_date_text != end_date_text:
            return  start_date_text + "_" + end_date_text
        else:
            return start_date_text       


def create_groups(args, metadata):

    extra_keys =  metadata.spectrograph.extra_group_keys()
    sci_files = metadata.find_frames('science')
    grouped_table = metadata.table[sci_files].group_by(['setup'] + extra_keys)
    groups = [PypeItMetaData(metadata.spectrograph, par=metadata.par, data=group.copy(True)) for group in grouped_table.groups]

    print(f"Grouped Science files into {len(groups)} groups. Now adding calibs...")
    calib_files = np.logical_not(sci_files)
    for group in groups:
        group_path = get_config_dir_path(group)        
        calib_files_for_group = np.logical_and(calib_files, metadata.find_configuration(group[0]['setup']))
        group.table = vstack([group.table, metadata[calib_files_for_group]])
        group.table.add_column([str(group_path)] * len(group.table), name="group_path")
            

    return groups


def create_date_groups(args, metadata_groups):
    """Return an array that can groups the entries in a configuration group by a date range."""

    all_date_groups = []

    for metadata in metadata_groups:
        print(f"Creating date groups for setup {metadata[0]['group_path']} science files")
        # Start the groups based on science files, and then find calibrations that match those
        sci_index = metadata.find_frames('science')

        date_groups = []
        date_groups = create_setup_date_groups(args, metadata, sci_index, date_groups)
        print(f"Created {len(date_groups)} date groups for setup {metadata[0]['group_path']}")

        print(f"Adding calib files to date groups for setup {metadata[0]['group_path']} science files")
        # Now Add in the calibration files
        calib_index = np.logical_not(sci_index)
        all_date_groups += create_setup_date_groups(args, metadata, calib_index, date_groups)

    return all_date_groups

def create_setup_date_groups(args, metadata, files_to_consider, date_groups=[]):

    for row in metadata[files_to_consider]:
        mjd = row['mjd']
        try:
            row_date = Time(mjd, format='mjd')
        except:
            write_to_report(args, f"Excluding {row['filename']} because of invalid MJD '{row['mjd']}'")
            continue

        # Find an existing date group for this file
        group = None
        for dg_indx in range(len(date_groups)):
            if date_groups[dg_indx] is None:
                # Skip groups that were set to None by merging
                continue

            # Find the date window for this group and see if the current date
            # fits into it
            if date_groups[dg_indx].is_date_in_window(row_date):
                if group is None:
                    # The first group that matched this file
                    date_groups[dg_indx].add_metadata_row(row, row_date)
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
            new_dg_metadata = PypeItMetaData(metadata.spectrograph, metadata.par, data=metadata.table[:0].copy(True))
            new_dg_metadata.table.add_row(row)
            date_groups.append(DateGroup(args, row_date, new_dg_metadata))


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



def get_all_metadata(args, extended_spec, matching_files, local_files):
    metadata = PypeItMetaData(extended_spec, extended_spec.default_pypeit_par(), files=local_files, strict=False)
    instrument = extended_spec.header_name

    # Get the frame types for each file
    metadata.get_frame_types(flag_unknown=True)

    # Find the "unknown" files, and remove them
    unknown_files = np.where([filename.lstrip().startswith("#") for filename in metadata['filename']])

    for unknown_file in metadata[unknown_files]['filename']:
        write_to_report(args, f"Excluding {unknown_file} because it has an unknown frame type")
    
    metadata.remove_rows(unknown_files)

    # Exclude unwanted frametypes
    
    if len(extended_spec.exclude_pypeit_types()) > 0:
        excluded_types = FrameTypeBitMask().flagged(metadata['framebit'],extended_spec.exclude_pypeit_types())
        excluded_by_type_rows = np.where(excluded_types)
        for i in excluded_by_type_rows:
            write_to_report(args, f"Excluding {metadata[i]['filename']} because it frame type {metadata[i]['frametype']}")

        metadata.remove_rows(excluded_by_type_rows)

    # If matching files was given, exclude science files that aren't matching
    if matching_files is not None:
        science_files = metadata.find_frames('science')
        non_matching_files = [filename not in matching_files['koaid'] for filename in metadata['filename']]
        non_matching_science_files = np.where(np.logical_and(science_files, non_matching_files))[0]
        if len(non_matching_science_files) > 0:
            for filename in metadata[non_matching_science_files]['filename']:
                write_to_report(args, f"Excluding {filename} because it is not a matching science file.")
            metadata.remove_rows(non_matching_science_files)

    # Run instrument specific exclude logic
    excluded, reasons = extended_spec.exclude_metadata(metadata)
    if len(excluded) > 0:
        for i, metadata_indx in enumerate(excluded):
            write_to_report(args, f"Excluding {metadata[metadata_indx]['filename']} of type {metadata[metadata_indx]['frametype']} because {reasons[i]}")
        metadata.remove_rows(excluded)

    # Now group into configurations
    metadata.set_configurations()

    # Remove calibration files that don't match any science files
    science_files = metadata.find_frames('science')
    unique_science_setup_names = np.unique(metadata[science_files]['setup'])
    calib_files = np.logical_not(science_files)
    matching_calib_files = np.zeros_like(calib_files, dtype=bool)
    for setup_name in unique_science_setup_names:
        setup_files = metadata.find_configuration(setup_name)
        setup_calib_files = np.logical_and(setup_files,calib_files)
        matching_calib_files = np.logical_or(matching_calib_files, setup_calib_files)

    # The non matching files are ones that aren't matching calib files and aren't science_files
    non_matching_calib_files = np.where(np.logical_not(np.logical_or(matching_calib_files, science_files)))[0]
    if len(non_matching_calib_files) > 0:
        # Report on what's being removed
        for filename in metadata[non_matching_calib_files]['filename']:
            write_to_report(args, f"Excluding {filename} because it does not match the configuration of any desired science files.")
        metadata.remove_rows(non_matching_calib_files)

    # Force generation of configurations now that rows have been removed
    metadata.table.remove_column('setup')
    metadata.unique_configurations(force=True)
    metadata.set_configurations()

    extended_spec.add_extra_metadata(metadata)
    return metadata

def get_config_dir_path(metadata):

    extended_spec = metadata.spectrograph
    instrument = extended_spec.header_name

    # Get the configuration values from a sample row, preferably a science row
    science_files = metadata.find_frames('science')
    if np.any(science_files):
        metadata_row=metadata[science_files][0]
    else:
        metadata_row = metadata[0]

    # The config dir name always starts with the instrument
    config_path = Path(instrument)
    for grouping in extended_spec.config_path_grouping():
        grouping_strings = []
        for part in grouping:
            key=part[0]
            type = part[1]
            if key not in metadata_row.colnames:
                value = "Unknown"
            else:
                value = metadata_row[key]
                if key=='binning':
                    # The comma causes some issues in the directory name, so replace with an x
                    value = value.replace(",", "x")
                elif type == 'float64':
                    # Use the spectrograph rtol value to figure out how to round to get a nice string value for the
                    # directory name
                    rtol = extended_spec.meta[key].get('rtol', None)
                    if rtol is not None:
                        value = round(value,int(np.abs(np.log10(rtol))))
                    else:
                        # Otherwise just round to a whole number
                        value=round(value)
                    # Convert to a string for use in the directory name
                    value = f"{value:g}"
                elif value is None or len(str(value)) == 0:
                    value = "Unknown"
                else:
                    value = str(value)
                    
            grouping_strings.append(value)
        config_path = config_path / "_".join(grouping_strings)
        
    return config_path


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
        parser.add_argument('--matching_file_list', type=Path, default=None, help='A CSV file with the science files matching the desired objects.')
        parser.add_argument('--source', type=str, default="local", choices=["local","s3", "gdrive"], help='The source cloud where the are stored, or "local" for a files on the local system.')
        parser.add_argument('--dest', type=str, default="local", choices=["local","s3", "gdrive"], help='The cloud the files will be are copied to, or "local" for the local system.')
        parser.add_argument('--date_window', type=float, default=3.0, help="How long a time range to use when grouping files. Measured in days. Defaults to 3 days.")
        parser.add_argument('--move_files', default=False, action="store_true", help="Whether or not to move the files from their original location instead of copying. Defaults to false.")
        parser.add_argument('--symlink', default=False, action="store_true", help="Use symlinks instead of copying files. Both the source and destination must be on the local machine.")
        parser.add_argument('--local_out', type=Path, default=Path("adap_setup_tmp"), help="A temporary directory used when working with files onthe cloud. Defaults to 'adap_setup_tmp'.")
        parser.add_argument('--report', type=str, default="reorg_report_file.txt", help="Name of the report file to create detailing any issues that occurred when running this script. Defaults to 'reorg_report_file.txt'.")
        parser.add_argument('--obslog', default=False, action="store_true", help="Whether to write an obslog out alongside each group of files.")
        return parser

    @staticmethod
    def main(args):
        if os.path.exists(args.report):
            os.unlink(args.report)
        start_time = datetime.datetime.now()
        write_to_report(args, f"adap_reorg_setup.py started at {start_time.isoformat()}.")
        write_to_report(args,  "--------------------------------------------------------")

        if args.matching_file_list is not None:
            # Load as a csv file
            matching_files = Table.read(args.matching_file_list, format="csv")
        else:
            matching_files = None

        extended_spec = ADAPSpectrographMixin.load_extended_spectrograph(spec_name=args.spectrograph_name, matching_files = matching_files)

        # Get the Path/RClonePath objects for the source files
        files = list(get_files(args))
        print(f"Found {len(files)} files.")
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
        print(f"Getting metadata")
        all_metadata = get_all_metadata(args, extended_spec, matching_files, local_files)
        print(f"Have metadata for {len(all_metadata)} files.")
        # Group them into configurations, which may involve adap specific keys not in the spectrographs
        # configuration_keys
        print(f"Creating groups...")
        groups = create_groups(args, all_metadata)
        print(f"Created {len(groups)} groups. Now dividing into date groups...")

        all_groups = create_date_groups(args, groups)
        print(f"Created {len(all_groups)} date groups.")

        for date_group in all_groups:
            dataset_path = Path(date_group.metadata['group_path'][0], date_group.get_dir_name())

            
            print(f"Checking completeness of {dataset_path} date group {date_group.get_dir_name()}...")
            is_complete, file_counts = extended_spec.is_metadata_complete(date_group.metadata)
            complete_str = "complete" if  is_complete else "incomplete"
            print(f"{dataset_path} is {complete_str}")
            if not is_complete:
                write_to_report(args, f"Incomplete dataset {dataset_path} files: {file_counts}")

            dest_path = dest_root / dataset_path / complete_str / "raw"
            if args.obslog: # Doesn't work with remote path yet
                print("Writing obslog")
                dest_path.mkdir(parents=True,exist_ok=True)
                date_group.metadata.write(dest_path.parent / "obslog.txt", sort_col='frametype', overwrite=True)
            print(f"Transferring {dest_path}")
            for row in date_group.metadata:
                transfer_file(args, f"{row['directory']}/{row['filename']}", dest_path)

        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    ReorgSetup.entry_point()
