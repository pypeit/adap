#!/usr/bin/env python3

import glob
import os
import shutil

import datetime 

from astropy.table import Table, vstack, Column
from astropy.time import Time
from astropy.io import fits 
import numpy as np
from pypeit import spectrographs

from pypeit.scripts import scriptbase
from pypeit.spectrographs.util import load_spectrograph
from pypeit.metadata import PypeItMetaData
from pypeit.par.pypeitpar import PypeItPar
from pypeit.pypeitsetup import PypeItSetup
import cloudstorage

def is_metadata_complete(metadata):
    metadata.get_frame_types(flag_unknown=True)
    num_science_frames = np.sum(metadata.find_frames('science'))
    num_flat_frames = np.sum(np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat')))
    num_arc_frames = np.sum(metadata.find_frames('arc'))
    return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1)
    
def write_to_report(args, msg):
    if os.path.dirname(args.report) != '' and not os.path.exists(os.path.dirname(args.report)):
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "a") as erf:
        print(msg,file=erf)

def transfer_data(args, file_list, target_dir):

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

    try:
        # Build a PypeItSetup object. Because this script can work in a batch mode where previous batches of data
        # may no longer exist, we manually set the fitstbl to a pre-built PypeItMetaData object.
        file_list = [os.path.join(x, y) for (x,y) in zip(metadata['directory'], metadata['filename'])]
        ps = PypeItSetup(file_list=file_list, spectrograph_name = args.spectrograph_name)
        ps.fitstbl = metadata

        if cloudstorage.is_cloud_uri(args.out_dir):
            os.makedirs(target_dir, exist_ok=True)
            ps.run(sort_dir=target_dir)

            print(f"Writing pypeit file to {target_dir}")
            files = metadata.write_pypeit(target_dir)

            for file in files:
                rel_path = os.path.relpath(file, args.local_out)
                cloud_dest = os.path.join(args.out_dir, rel_path)
                print(f"Uploading {file} file to {cloud_dest}")
                cloudstorage.upload(file, cloud_dest)
            
        else:
            os.makedirs(target_dir, exist_ok=True)
            ps.run(sort_dir=target_dir)
            print(f"Writing pypeit file to {target_dir}")
            metadata.write_pypeit(target_dir)

    except Exception as e:
        write_to_report(args, f"Failed to run setup on {target_dir}, exception {e}")

def merge_tables(table1, table2):
    # I kept having errors merging tables where the column type was 'object' in one table
    # and 'float' in another caused by "None" values  This code is meant to deal with that by
    # detecting this situation converting the columns to "object"

    assert len(table1.columns) == len(table2.columns)
    for c1 in table1.itercols():
        if (c1.dtype != table2[c1.name].dtype):
            if c1.dtype == np.dtype('object'):
                # Convert Table 2's column to object
                newcol = Column(table2[c1.name], c1.name, np.dtype('object'))
                table2[c1.name] = newcol
            elif table2[c1.name].dtype == np.dtype('object'):
                # Convert Table 1's column to object
                newcol = Column(table1[c1.name], c1.name, np.dtype('object'))
                table1[c1.name] = newcol
            # Else we hope vstack can handle it                

    return vstack([table1, table2])

def transfer_batch(args, spectrograph, par, files):

    ## if s3 transfer s3 files to a temp location, then continue from there
    if cloudstorage.is_cloud_uri(args.source_dirs[0]):
        os.makedirs(args.local_out, exist_ok=True)
        local_files = [cloudstorage.download(file, args.local_out) for file in files]
    else:
        local_files = files


    # do this?
    grouping_metadata_table = read_grouping_metadata(args, spectrograph, files, local_files)
    transferred_files = []
    # Group the files to determine where to transfer them
    print("Grouping by 'decker'")
    for decker_group in grouping_metadata_table.group_by('decker').groups:
        print("Grouping by 'dispname', 'dispangle', 'filter1'")
        for cfg_group in decker_group.group_by(['dispname', 'dispangle', 'filter1']).groups:
            """
            dates = []
            for mjd in cfg_group['mjd']:
                try:
                    date = Time(mjd, format='mjd').to_value('iso', subfmt='date')
                except:
                    import pdb; pdb.set_trace()
                    # to_value is failing because  subfmt isn't recognized as valid
                    # keyword argument
                    # We have to keep an entry for this so that the column
                    # remains the same length as the table
                    date = '1970-01-01'
                dates.append(date)
            """
            dates = get_date_groups(args, cfg_group)
            config_group_dir = f"{cfg_group['dispname'][0]}_{cfg_group['dispangle'][0]:.0f}_{cfg_group['filter1'][0]}".replace(" ", "_")
            print("Grouping by dates")
            date_groups = cfg_group.group_by(np.array(dates)).groups
            for i in range(len(date_groups)):

                # Determine the name of the date group based on the min/max dates
                min_date = Time(np.min(date_groups[i]['mjd']), format='mjd').to_value('iso', subfmt='date')
                max_date = Time(np.max(date_groups[i]['mjd']), format='mjd').to_value('iso', subfmt='date')
                if min_date != max_date:
                    dg_name = f"{min_date}_{max_date}"
                else:
                    dg_name = min_date
    
                grouping_path = os.path.join(decker_group['decker'][0].replace(" ", "_"),
                                             config_group_dir,
                                             dg_name)
                
                
                #complete_target = os.path.join(dest, "complete")
                if cloudstorage.is_cloud_uri(args.source_dirs[0]):
                    dest = os.path.join(args.local_out, grouping_path)
                else:
                    dest = os.path.join(args.out_dir, grouping_path)
               
                incomplete_target = os.path.join(dest, "incomplete")
                raw_dir = os.path.join(incomplete_target, "raw")
                file_list = transfer_data(args, list(date_groups[i]['localfile']), raw_dir)
                transferred_files += file_list

                print(f"Creating PypeItMetaData for {grouping_path}")
                metadata = PypeItMetaData(spectrograph, par, files=file_list)
                # Keep original source information so that the files can be copied within S3 later
                if cloudstorage.is_cloud_uri(args.source_dirs[0]):
                    source_dirs = [os.path.dirname(x['sourcefile']) for x in date_groups[i]]
                    metadata.table['directory'] = Column(data=source_dirs, name='directory')

                metadata_file = os.path.join(incomplete_target, f"metadata_{grouping_path.replace(os.path.sep, '_')}.ecsv")
                if os.path.exists(metadata_file):
                    # Merge with the results of a prior run
                    print(f"Merging with {metadata_file}")
                    existing_metadata = Table.read(metadata_file, format="ascii.ecsv")
                    metadata.table = merge_tables(existing_metadata, metadata.table)
                metadata.table.sort(['mjd', 'filename'])
                metadata.table.write(metadata_file, format="ascii.ecsv",overwrite=True)
                
    return transferred_files

def is_bias_frame(spectrograph, headarr):
    if (spectrograph.get_meta_value(headarr, "idname") == 'Bias' and
        spectrograph.get_meta_value(headarr, "lampstat01") == 'Off' and
        spectrograph.get_meta_value(headarr, "hatch") == 'closed'):
        return True
    else:
        return False

def read_grouping_metadata(args, spectrograph, files, local_files):

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

def get_date_groups(args, cfg_group):
    """Return an array of that can groups the entries in a configuration group"""
    date_groups = np.zeros(len(cfg_group))
    next_frame = 0
    next_group = 1

    dates = []
    for mjd in cfg_group['mjd']:
        try:
            date = Time(mjd, format='mjd')#.to_value('iso', subfmt='date')
        except:
            # We have to keep an entry for this so that the column
            # remains the same length as the table
            date = Time('1970-01-01', format='mjd')
        dates.append(date)


    for i in range(len(date_groups)):
        if i == next_frame:
            continue # Don't compare with self
        if np.fabs((dates[i] - dates[next_frame]).value) <= args.date_window:
            if date_groups[next_frame] == 0:
                if date_groups[i] != 0:
                    date_groups[next_frame] = date_groups[i]
                else:
                    date_groups[next_frame] = next_group
                    date_groups[i] = next_group
                    next_group += 1
            else:
                if date_groups[i] == 0:
                    date_groups[i] = date_groups[next_frame]
                elif date_groups[i] != date_groups[next_frame]:
                    prev_entries_in_group = date_groups == date_groups[next_frame]
                    date_groups[prev_entries_in_group] = date_groups[i]
    for i in range(len(date_groups)):
        if date_groups[i] == 0:
            date_groups[i] = next_group
            next_group += 1
    return date_groups
    # IS this really it?

def get_files(args):

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

class BatchSetup(scriptbase.ScriptBase):

    @classmethod
    def get_parser(cls, width=None):

        parser = super().get_parser(description='Organize a batch of files into directories '\
                                                'based on configuration keys and date and run ' \
                                                'pypeit_setup on them.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('out_dir', type=str)
        parser.add_argument('source_dirs', type=str, nargs='+')
        parser.add_argument('--date_window', type=float, default=3.0)
        parser.add_argument('--move_files', default=False, action="store_true")
        parser.add_argument('--endpoint_url', type=str, default='https://s3-west.nrp-nautilus.io')
        parser.add_argument('--spectrograph_name', type=str, default='keck_deimos')
        parser.add_argument('--local_out', type=str, default="adap_setup_tmp")
        parser.add_argument('--report', type=str, default="reorg_report_file.txt")
        parser.add_argument("--batch_size", type=int, default=0)

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
        for file, size in get_files(args):
            if args.batch_size != 0 and current_batch_size + size >= batch_size:
                print("Transferring batch")
                transferred_files = transfer_batch(args, spectrograph, par, current_batch_files)               
                print("Removing batch")
                remove_batch(transferred_files)
                current_batch_files = []
                current_batch_size = 0

            current_batch_files.append(file)
            current_batch_size += size
        print("Transferring final batch")
        transferred_files = transfer_batch(args, spectrograph, par, current_batch_files)
        if args.batch_size != 0:
            remove_batch(transferred_files)

        if cloudstorage.is_cloud_uri(args.out_dir):            
            metadata_search_path = args.local_out
        else:
            metadata_search_path = args.out_dir

        incomplete_science_dirs = []
        files_with_unknown_type = []
        for metadata_file in glob.glob(os.path.join(metadata_search_path, "**/*.ecsv"), recursive=True):
            target_dir = os.path.dirname(metadata_file)
            metadata = PypeItMetaData(spectrograph, par, data = Table.read(metadata_file, format="ascii.ecsv"))
            # Initialize types in PypeItMetadata so we can find science frames and 
            # unknown type
            metadata.get_frame_types(flag_unknown=True)

            if is_metadata_complete(metadata):
                if os.path.basename(target_dir) == 'incomplete':
                    new_target_dir = os.path.join(os.path.dirname(target_dir), "complete")
                    os.rename(target_dir, new_target_dir)
                    target_dir = new_target_dir
                    raw_dir = os.path.join(target_dir, "raw")
                    if not cloudstorage.is_cloud_uri(args.out_dir):
                        metadata.table['directory'] = Column(data=np.full(len(metadata.table), raw_dir), name='directory')
            else:
                if len(metadata.table[metadata.find_frames("science")]) > 0:
                    incomplete_science_dirs.append(os.path.dirname(target_dir))

            for row in metadata.table:
                if row['frametype'] is None or row['frametype'] == 'None':
                    files_with_unknown_type.append(os.path.join(row['directory'], row['filename']))

            # Transfer files within the cloud, updating the metadata directory to point to the new cloud URI
            if cloudstorage.is_cloud_uri(args.out_dir):
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


        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    BatchSetup.entry_point()
