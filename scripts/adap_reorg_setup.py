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
                keck_deimos_A/
                    <output of PypeIt Setup>
            incomplete/
                raw/
                keck_deimos_A/
                    <output of PypeIt Setup>

Where "complete" folders have enough data to be reduced with PypeIt, and
"incomplete" folders do not.                


Example Usage:

To organize a local RAW_DATA1 and RAW_DATA2 directory to raw_organized:
    adap_reorg_setup.py raw_organized RAW_DATA1 RAW_DATA2

To organize an S3 RAW_DATA files 50 files at a time:
    adap_reorg_setup.py s3://pypeit/adap/raw_organized s3://pypeit/adap/RAW_DATA --batch_size 50

Requres: "boto3" and "smart_open" pip packages to use S3

"""
import glob
import os
import shutil
import datetime 
import traceback
from operator import is_

from astropy.table import Table, vstack, Column, MaskedColumn
from astropy.time import Time, TimeDelta
import numpy as np

from pypeit.scripts import scriptbase
from pypeit.spectrographs.util import load_spectrograph
from pypeit.metadata import PypeItMetaData
from pypeit.par.pypeitpar import PypeItPar
from pypeit.pypeitsetup import PypeItSetup
import cloudstorage

def is_metadata_complete(metadata):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       The minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """
    if len(metadata.table) == 0:
        # This can happen if all of the files in this directory were removed from the metadata
        # due to unknown types.
        return False

    num_science_frames = np.sum(metadata.find_frames('science'))
    num_flat_frames = np.sum(np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat')))
    num_arc_frames = np.sum(metadata.find_frames('arc'))
    return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1)
    
def write_to_report(args, msg):
    """Write a message to the report file, creating it if it doesn't exist. """
    if os.path.dirname(args.report) != '' and not os.path.exists(os.path.dirname(args.report)):
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "a") as erf:
        print(msg,file=erf)

def transfer_data(args, file_list, target_dir):
    """Transfers data from the source to destination. Supports cloud to cloud,
    or local to local. Will also move if args.move_files is True.
    """
    files = []
    for file in file_list:
        if cloudstorage.is_cloud_uri(target_dir):
            if not cloudstorage.is_cloud_uri(file):
                raise ValueError("Currently grouping from local storage to cloud is not supported")
            else:
                # This treating of possible URIs as paths will not work on windows                
                dest_file = os.path.join(target_dir, os.path.basename(file))
                print(f"Copying {file} to {dest_file}")
                cloudstorage.copy(file, dest_file)
        else:
            os.makedirs(target_dir, exist_ok=True) 
            if args.move_files is True:
                print(f"Moving {file} to {target_dir}")
                shutil.move(file, target_dir)
            else:
                print(f"Copying {file} to {target_dir}")
                shutil.copy2(file, target_dir)
            files.append(os.path.join(target_dir, os.path.basename(file)))

    return files

def run_setup(args, target_dir, metadata):
    """Run PypeIt setup on a directory given the PypeItMetadata for it."""
    try:
        # Build a PypeItSetup object. Because this script can work in a batch mode where previous batches of data
        # may no longer exist, we manually set the fitstbl to a pre-built PypeItMetaData object.
        file_list = [os.path.join(x, y) for (x,y) in zip(metadata['directory'], metadata['filename'])]
        ps = PypeItSetup(file_list=file_list, spectrograph_name = args.spectrograph_name)
        ps.fitstbl = metadata

        if cloudstorage.is_cloud_uri(args.out_dir):
            os.makedirs(target_dir, exist_ok=True)
            ps.run(sort_dir=target_dir, setup_only=True)

            print(f"Writing pypeit file to {target_dir}")
            files = metadata.write_pypeit(target_dir)

            for file in files:
                rel_path = os.path.relpath(file, args.local_out)
                cloud_dest = os.path.join(args.out_dir, rel_path)
                print(f"Uploading {file} file to {cloud_dest}")
                cloudstorage.upload(file, cloud_dest)
            
        else:
            os.makedirs(target_dir, exist_ok=True)
            ps.run(sort_dir=target_dir, setup_only=True)
            print(f"Writing pypeit file to {target_dir}")
            metadata.write_pypeit(target_dir)

    except Exception as e:
        write_to_report(args, f"Failed to run setup on {target_dir}")
        write_to_report(args, traceback.format_exc())

def find_none_rows(table):
    # Return indexes of rows in table that have None values.

    rows = None
    for col in table.colnames:
        if rows is not None:
            rows = np.concatenate((np.where(np.vectorize(is_)(table[col], None))[0], rows))
        else:
            rows = np.where(np.vectorize(is_)(table[col], None))[0]

    return np.unique(rows)

def merge_tables(table1, table2):
    # I kept having errors merging tables where the column type was 'object' in one table
    # and 'float' in another caused by "None" values  This code is meant to deal with that by
    # detecting this situation converting the columns to "object"

    assert len(table1.columns) == len(table2.columns)
    for c1 in table1.itercols():
        if (c1.dtype != table2[c1.name].dtype):
            if c1.dtype == np.dtype('object'):
                # Convert Table 2's column to object
                if isinstance(table2[c1.name], MaskedColumn):
                    table2[c1.name].mask = np.zeros_like(table2[c1.name].mask, dtype=bool)

                newcol = Column(table2[c1.name], c1.name, np.dtype('object'))
                table2[c1.name] = newcol
            elif table2[c1.name].dtype == np.dtype('object'):
                # Convert Table 1's column to object
                if isinstance(table1[c1.name], MaskedColumn):
                    table1[c1.name].mask = np.zeros_like(table1[c1.name].mask, dtype=bool)

                newcol = Column(table1[c1.name], c1.name, np.dtype('object'))
                table1[c1.name] = newcol
            # Else we hope vstack can handle it                

    return vstack([table1, table2])

class DateGroup:
    def __init__(self, start_date, end_date, parent_dir):
        self.start_date = start_date
        self.end_date = end_date
        self.parent_dir = parent_dir

        if start_date != end_date:
            self.path = os.path.join(parent_dir, start_date.to_value('iso', subfmt='date') + "_" + end_date.to_value('iso', subfmt='date'))
        else:
            self.path = os.path.join(parent_dir, start_date.to_value('iso', subfmt='date'))

    def merge(self, other_dg):

        if self.parent_dir != other_dg.parent_dir:
            raise ValueError("Can't merge date groups from different parent directories.")

        start_date = self.start_date
        end_date = self.end_date

        if other_dg.start_date < self.start_date:
            start_date = other_dg.start_date
        if other_dg.end_date > self.end_date:
            end_date = other_dg.end_date

        return DateGroup(start_date, end_date, self.parent_dir)

def get_dates(cfg_group):
    dates = []
    for mjd in cfg_group['mjd']:
        try:
            date = Time(mjd, format='mjd')#.to_value('iso', subfmt='date')
        except:
            # We have to keep an entry for this so that the column
            # remains the same length as the table
            date = Time('1970-01-01', format='mjd')
        dates.append(date)


_all_date_groups = dict()

class DateGroup:
    def __init__(self, id, start_date, end_date):
        self.id = id
        self.start_date = start_date
        self.end_date = end_date

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


def get_date_groups(args, cfg_group_dir, cfg_group):
    """Return an array that can groups the entries in a configuration group by a date range."""

    window_delta = TimeDelta(args.date_window, format='jd')
    date_group_ids = np.zeros(len(cfg_group),dtype=int)

    # Because files are processed in batches, make sure to compare against date groups
    # from all previous batches.  combined_date_groups has the group integer values form
    # previous batches, and combined_dates has the date that correspond to that group.
    if cfg_group_dir not in _all_date_groups:
        #_all_date_groups[cfg_group_dir] = (np.empty(0, dtype=int), np.empty(0, dtype=int))
        _all_date_groups[cfg_group_dir] = []

    #(combined_date_groups, combined_dates) = _all_date_groups[cfg_group_dir]
    date_groups = _all_date_groups[cfg_group_dir]

    # next_group is the next integer to use for a date group if a file doesn't match any previous
    # group. This starts at 1 because 0 is reserved for images without an assigned group
    if len(date_groups) > 0:
        next_group = np.max([x.id for x in date_groups]) + 1
    else:
        next_group = 1

    # Get the dates for the images in this batch
    dates = []
    for mjd in np.trunc(cfg_group['mjd']):
        try:
            date = Time(mjd, format='mjd')#.to_value('iso', subfmt='date')
        except:
            # We have to keep an entry for this so that the column
            # remains the same length as the table
            date = Time('1970-01-01', format='mjd')
        dates.append(date)


    for i in range(len(dates)):
        if len(date_groups) == 0:
            date_groups.append(DateGroup(next_group, dates[i], dates[i]))
            date_group_ids[i] = next_group
            next_group += 1
        else:
            group = None
            for j in range(len(date_groups)):
                if date_groups[j] is None:
                    # Skip groups that were set to None by merging
                    continue

                # Find the date window for this group and see if the current date
                # fits into it
                start = date_groups[j].start_date - window_delta
                end = date_groups[j].end_date + window_delta
                if dates[i] >= start and dates[i] <= end:
                    if group is None:
                        date_groups[j].add_date(dates[i])
                        date_group_ids[i] = date_groups[j].id
                        group = date_groups[j]                  
                    else:
                        # The new date belongs to two groups, so those groups should be merged
                        
                        # Merge new group into prior group
                        group.merge(date_groups[j])
                        
                        # Set group id for current date
                        date_group_ids[i] = group.id
                        
                        # Update group ids for dates associated with the old group
                        old_idx = date_group_ids == date_groups[j].id
                        date_group_ids[old_idx] = group.id
                        
                        # Remove the old group, but because we're iterating through the list
                        # just set it to None
                        date_groups[j] = None                      

            if group is None:
                # No match was found, start a new group
                date_groups.append(DateGroup(next_group, dates[i], dates[i]))
                date_group_ids[i] = next_group
                next_group += 1

    # Filter out Nones from merging before returning
    _all_date_groups[cfg_group_dir] = [x for x in date_groups if x is not None]

    return date_group_ids

def rename_date_range_dirs(base_dir):
    """Rename date range directories from the date range id to the 
       actual start/end dates"""
    for key in _all_date_groups.keys():
        date_groups = _all_date_groups[key]
        for dg in date_groups:
            start_date_str = dg.start_date.to_value('iso', subfmt='date')
            end_date_str   = dg.end_date.to_value('iso', subfmt='date')
            if start_date_str == end_date_str:
                new_name = dg.start_date.to_value('iso', subfmt='date')
            else:
                new_name = dg.start_date.to_value('iso', subfmt='date') + "_" + dg.end_date.to_value('iso', subfmt='date')

            orig_dir = os.path.join(base_dir, key, str(dg.id))
            new_dir = os.path.join(base_dir, key, new_name)
            os.rename(orig_dir, new_dir)

def transfer_batch(args, spectrograph, par, files):
    """Transfer a batch of files to their correct, grouped destinations.
    For cloud files, the file is downloaded and grouped to local data, and
    will only be copied to the correct destination in the cloud later"""
    
    ## if s3 transfer s3 files to a temp location, then continue from there
    if cloudstorage.is_cloud_uri(args.source_dirs[0]):
        os.makedirs(args.local_out, exist_ok=True)
        local_files = [cloudstorage.download(file, args.local_out) for file in files]
    else:
        local_files = files


    grouping_metadata_table = read_grouping_metadata(args, spectrograph, files, local_files)
    transferred_files = []
    # Group the files to determine where to transfer them
    print("Grouping by 'decker'")
    for decker_group in grouping_metadata_table.group_by('decker').groups:
        print("Grouping by 'dispname', 'dispangle', 'filter1'")
        for cfg_group in decker_group.group_by(['dispname', 'dispangle', 'filter1']).groups:

            config_group_dir = f"{cfg_group['decker'][0]}{os.path.sep}{cfg_group['dispname'][0]}_{cfg_group['dispangle'][0]:.0f}_{cfg_group['filter1'][0]}".replace(" ", "_")
            print("Grouping by dates")
            date_group_ids = get_date_groups(args, config_group_dir, cfg_group)
            date_groups = cfg_group.group_by(date_group_ids).groups
            for i in range(len(date_groups)):

                date_group_dir = os.path.join(config_group_dir, str(date_groups.keys[i]))
                #complete_target = os.path.join(dest, "complete")
                if cloudstorage.is_cloud_uri(args.source_dirs[0]):
                    dest = os.path.join(args.local_out,date_group_dir)
                else:
                    dest = os.path.join(args.out_dir,  date_group_dir)
               
                incomplete_target = os.path.join(dest, "incomplete")
                raw_dir = os.path.join(incomplete_target, "raw")

                file_list = transfer_data(args, list(date_groups[i]['localfile']), raw_dir)
                transferred_files += file_list

                print(f"Creating PypeItMetaData for {date_group_dir}")
                metadata = PypeItMetaData(spectrograph, par, files=file_list)
                # Keep original source information so that the files can be copied within S3 later
                if cloudstorage.is_cloud_uri(args.source_dirs[0]):
                    source_dirs = [os.path.dirname(x['sourcefile']) for x in date_groups[i]]
                    metadata.table['directory'] = Column(data=source_dirs, name='directory')

                metadata_file = os.path.join(incomplete_target, f"metadata_{config_group_dir.replace(os.path.sep, '_')}.ecsv")
                if os.path.exists(metadata_file):
                    # Merge with the results of a prior run
                    print(f"Merging with {metadata_file}")
                    existing_metadata = Table.read(metadata_file, format="ascii.ecsv")
                    metadata.table = merge_tables(existing_metadata, metadata.table)
                metadata.table.sort(['mjd', 'filename'])
                metadata.table.write(metadata_file, format="ascii.ecsv",overwrite=True)

    return transferred_files

def is_bias_frame(spectrograph, headarr):
    """ Determine if a DEIMOS frame is a bias frame."""
    if (spectrograph.get_meta_value(headarr, "idname") == 'Bias' and
        spectrograph.get_meta_value(headarr, "lampstat01") == 'Off' and
        spectrograph.get_meta_value(headarr, "hatch") == 'closed'):
        return True
    else:
        return False

def read_grouping_metadata(args, spectrograph, files, local_files):
    """Read metadata from a file needed to group it."""
    keys = ["decker", "dispname", "dispangle", "filter1", "mjd", "sourcefile", "localfile"]
    dtypes = ['<U', '<U', 'float64', '<U', 'float64', '<U', '<U']
    data_rows = []
    for (source_file, local_file) in zip(files, local_files):

        try:
            print(f"Getting grouping metadata for {local_file}")
            headarr = spectrograph.get_headarr(local_file)
            if is_bias_frame(spectrograph, headarr):
                write_to_report(args, f"Skipping bias frame {local_file}.")
                continue

            data_row = []
            for key in keys:
                if key == "sourcefile":
                    data_row.append(source_file)
                elif key == "localfile":
                    data_row.append(local_file)
                elif key == "dispangle":
                    data_row.append(round(spectrograph.get_meta_value(headarr, key, required=True)))
                else:
                    data_row.append(spectrograph.get_meta_value(headarr, key, required=True))
            data_rows.append(data_row)
        except Exception as e:
            write_to_report(args, f"Failed to get metadata from {local_file}. {e}")

    if len(data_rows) == 0:
        return Table(data=None, names=keys, dtype=dtypes)
    else:
        return Table(rows=data_rows, names=keys, dtype=dtypes)


def get_files(args):
    """Find all of the source fits files, whether in S3 or local."""
    results = []
    if cloudstorage.is_cloud_uri(args.source_dirs[0]):
        for source_dir in args.source_dirs:
            results += cloudstorage.list_objects(source_dir, ['*.fits', '*.fits.gz'])
    else:
        for source_dir in args.source_dirs:
            files = glob.glob(os.path.join(source_dir, '**/*.fits'), recursive=True)
            files += glob.glob(os.path.join(source_dir, '**/*.fits.gz'), recursive=True)
            for file in files:
                results.append((file, os.stat(file).st_size))

    return results

def remove_batch(files):
    for file in files:
        os.unlink(file)

class ReorgSetup(scriptbase.ScriptBase):

    @classmethod
    def get_parser(cls, width=None):

        parser = super().get_parser(description='Organize raw data files into directories '
                                                'based on configuration keys and date and run '
                                                'pypeit_setup on them.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('out_dir', type=str, help="Output directory where the organized directry tree of files will be created.")
        parser.add_argument('source_dirs', type=str, nargs='+', help="One or more source directories containing raw data files.")
        parser.add_argument('--date_window', type=float, default=3.0, help="How long a time range to use when grouping files. Measured in days. Defaults to 3 days.")
        parser.add_argument('--move_files', default=False, action="store_true", help="Whether or not to move the files from their original location instead of copying. Defaults to false.")
        parser.add_argument('--endpoint_url', type=str, default='https://s3-west.nrp-nautilus.io', help="Endpoint URL to use when working with files in S3. Defaults to the PRP nautilus west coast URL.")
        parser.add_argument('--spectrograph_name', type=str, default='keck_deimos', help="The name of the spectrograph that created the raw data. Defaults to keck_deimos.")
        parser.add_argument('--local_out', type=str, default="adap_setup_tmp", help="A temporary directory used when working with files on S3. Defaults to 'adap_setup_tmp'.")
        parser.add_argument('--report', type=str, default="reorg_report_file.txt", help="Name of the report file to create detailing any issues that occurred when running this script. Defaults to 'reorg_report_file.txt'.")
        parser.add_argument("--batch_size", type=int, default=0, help="Divides the files into batches of this size when downloading from S3. Defaults to 0, inidicating all files will be processed in one batch.")

        return parser

    @staticmethod
    def main(args):
        start_time = datetime.datetime.now()
        spectrograph = load_spectrograph(args.spectrograph_name)
        cfg_lines = ['[rdx]', 'ignore_bad_headers = True', f'spectrograph = {args.spectrograph_name}']
        default_cfg = spectrograph.default_pypeit_par().to_config()
        par = PypeItPar.from_cfg_lines(cfg_lines=default_cfg, merge_with=cfg_lines)

        if cloudstorage.is_cloud_uri(args.source_dirs[0]):
            cloudstorage.initialize_cloud_storage(args.source_dirs[0], args)
        

        # The batch_size argument is in GiB, covnert to bytes
        batch_size = args.batch_size * (2**30)
        current_batch_size = 0
        current_batch_files = []

        #Transfer files batch by batch, organizing the files as we go
        # Files from the cloud are organized into a local directory first
        for file, size in get_files(args):
            if args.batch_size != 0 and current_batch_size + size >= batch_size:
                print("Transferring batch")
                transferred_files = transfer_batch(args, spectrograph, par, current_batch_files)               
                # We only use the temporary storage if the target is in the cloud
                if cloudstorage.is_cloud_uri(args.source_dirs[0]):
                    print("Removing batch")
                    remove_batch(transferred_files)
                current_batch_files = []
                current_batch_size = 0

            current_batch_files.append(file)
            current_batch_size += size
        print("Transferring final batch")
        transferred_files = transfer_batch(args, spectrograph, par, current_batch_files)
        if args.batch_size != 0 and cloudstorage.is_cloud_uri(args.source_dirs[0]):
            # We only use the temporary storage if the target is in the cloud
            remove_batch(transferred_files)

        if cloudstorage.is_cloud_uri(args.out_dir):            
            metadata_search_path = args.local_out
        else:
            metadata_search_path = args.out_dir

        rename_date_range_dirs(metadata_search_path)

        incomplete_science_dirs = []
        files_with_unknown_type = []
        files_with_missing_metadata = []
        for metadata_file in glob.glob(os.path.join(metadata_search_path, "**/*.ecsv"), recursive=True):
            target_dir = os.path.dirname(metadata_file)
            metadata = PypeItMetaData(spectrograph, par, data = Table.read(metadata_file, format="ascii.ecsv"))

            rows_with_none = find_none_rows(metadata.table)
            for row in metadata.table[rows_with_none]:
                files_with_missing_metadata.append(os.path.join(target_dir, "raw", row['filename']))

            metadata.table.remove_rows(rows_with_none)

            # Initialize types in PypeItMetadata so we can find science frames 
            metadata.get_frame_types(flag_unknown=True)

            # Find and filter out images with unknown types
            unknown_idx = np.logical_or(metadata.table["frametype"] == 'None', np.asarray(np.vectorize(is_)(metadata.table["frametype"], None)))
            for row in metadata.table[unknown_idx]:
                files_with_unknown_type.append(os.path.join(target_dir, "raw", row['filename']))

            metadata.table.remove_rows(unknown_idx)

            if is_metadata_complete(metadata):
                if os.path.basename(target_dir) == 'incomplete':
                    new_target_dir = os.path.join(os.path.dirname(target_dir), "complete")
                    os.rename(target_dir, new_target_dir)
                    target_dir = new_target_dir
                    raw_dir = os.path.join(target_dir, "raw")
            else:
                raw_dir = os.path.join(target_dir, "raw")
                if len(metadata.table[metadata.find_frames("science")]) > 0:
                    incomplete_science_dirs.append(os.path.dirname(target_dir))

            if not cloudstorage.is_cloud_uri(args.out_dir):
                # Update directory with renamed value
                metadata.table['directory'] = Column(data=np.full(len(metadata.table), raw_dir), name='directory')
            else:

                # Transfer files within the cloud, updating the metadata directory to point to the new cloud URI
                # This has to be done after determining if the directory is complete, because there's no
                # rename or move in S3
                cloud_dir = os.path.join(args.out_dir, os.path.relpath(target_dir, args.local_out), 'raw')
                file_list = [os.path.join(row['directory'], row['filename']) for row in metadata.table]
                transfer_data(args, file_list, cloud_dir)
                metadata.table['directory'] = Column(data=np.full(len(metadata.table), cloud_dir), name='directory')

            # Run Setup
            run_setup(args, target_dir, metadata)

        if len(incomplete_science_dirs) > 0:
            write_to_report(args, "Incomplete Directories with Science Frames:\n")
            for dir in incomplete_science_dirs:
                write_to_report(args, dir)

        if len(files_with_unknown_type) > 0:
            write_to_report(args, "\nFiles with unknown type:\n")
            for file in files_with_unknown_type:
                write_to_report(args, file)

        if len(files_with_missing_metadata) > 0:
            write_to_report(args, "\nFiles with missing metadata:\n")
            for file in files_with_missing_metadata:
                write_to_report(args, file)

        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    ReorgSetup.entry_point()
