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
        if os.path.dirname(args.error_report) != '' and not os.path.exists(os.path.dirname(args.error_report)):
            os.makedirs(os.path.dirname(args.error_report), exist_ok=True)
        with open(args.error_report, "a") as erf:
            print(f"Failed to run setup on {target_dir}, exception {e}",file=erf)

def transfer_batch(args, spectrograph, par, files):

    ## if s3 transfer s3 files to a temp location, then continue from there
    if cloudstorage.is_cloud_uri(args.source_dirs[0]):
        os.makedirs(args.local_out, exist_ok=True)
        local_files = [cloudstorage.download(file, args.local_out) for file in files]
    else:
        local_files = files


    # do this?
    grouping_metadata_table = read_grouping_metadata(spectrograph, files, local_files, args.error_report)
    transferred_files = []
    # Group the files to determine where to transfer them
    print("Grouping by 'decker'")
    for decker_group in grouping_metadata_table.group_by('decker').groups:
        print("Grouping by 'dispname', 'dispangle', 'filter1'")
        for cfg_group in decker_group.group_by(['dispname', 'dispangle', 'filter1']).groups:
            dates = []
            for mjd in cfg_group['mjd']:
                try:
                    date = Time(mjd, format='mjd').to_value('iso', subfmt='date')
                except:
                    # We have to keep an entry for this so that the column
                    # remains the same length as the table
                    date = '1970-01-01'
                dates.append(date)

            config_group_dir = f"{cfg_group['dispname'][0]}_{cfg_group['dispangle'][0]:.0f}_{cfg_group['filter1'][0]}".replace(" ", "_")
            print("Grouping by dates")
            date_groups = cfg_group.group_by(np.array(dates)).groups
            for i in range(len(date_groups)):

                grouping_path = os.path.join(decker_group['decker'][0].replace(" ", "_"),
                                             config_group_dir,
                                             date_groups.keys[i])
                
                
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
                    #metadata.table.write(os.path.join(target_dir, "new_metadata.ecsv"), format="ascii.ecsv",overwrite=True)
                    print(f"Merging with {metadata_file}")
                    existing_metadata = Table.read(metadata_file, format="ascii.ecsv")
                    metadata.table = vstack([existing_metadata, metadata.table])
                metadata.table.sort(['mjd', 'filename'])
                metadata.table.write(metadata_file, format="ascii.ecsv",overwrite=True)
                
                ##
                ##
                #metadata.table['directory'] = Column(data = [raw_dir for entry in metadata.table['filename']], name='directory')
                #print(f"Running setup on {dest_path}")
                #run_setup(args, dest_path, metadata)

                #transferred_files += file_list

                #metadata_file = os.path.join(args.out_dir, target_dir, "metadata.ecsv")
                #if os.path.exists(metadata_file):
                #    # Merge with the results of a prior run
                    #metadata.table.write(os.path.join(target_dir, "new_metadata.ecsv"), format="ascii.ecsv",overwrite=True)
                #    print(f"Merging with {metadata_file}")
                #    existing_metadata = Table.read(metadata_file, format="ascii.ecsv")
                #    metadata.table = vstack([existing_metadata, metadata.table])

                #metadata.table.write(metadata_file, format="ascii.ecsv",overwrite=True)

    return transferred_files

def read_grouping_metadata(spectrograph, files, local_files, error_report_file):

    keys = ["decker", "dispname", "dispangle", "filter1", "mjd", "sourcefile", "localfile"]
    dtypes = ['<U', '<U', 'float64', '<U', 'float64', '<U', '<U']
    data_rows = []
    for (source_file, local_file) in zip(files, local_files):

        try:
            #if cloudstorage.is_cloud_uri(source_file):
            #    print(f"Getting grouping metadata for {source_file}")
            #    # New cloud code
            #    with cloudstorage.open(source_file, "rb") as cloud_file:
            #        hdul = fits.open(cloud_file)

            #    # Read the fits headers
            #    headarr = spectrograph.get_headarr(hdul)
            #else:
            print(f"Getting grouping metadata for {local_file}")
            headarr = spectrograph.get_headarr(local_file)

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
            if os.path.dirname(error_report_file) != '' and not os.path.exists(os.path.dirname(error_report_file)):
                os.makedirs(os.path.dirname(error_report_file), exist_ok=True)

            with open(error_report_file, "a") as erf:
                print(f"Failed to get metadata from {local_file}. {e}", file=erf)
    if len(data_rows) == 0:
        return Table(data=None, names=keys, dtype=dtypes)
    else:
        return Table(rows=data_rows, names=keys, dtype=dtypes)

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
        parser.add_argument('--move_files', default=False, action="store_true")
        parser.add_argument('--endpoint_url', type=str, default='https://s3-west.nrp-nautilus.io')
        parser.add_argument('--spectrograph_name', type=str, default='keck_deimos')
        parser.add_argument('--local_out', type=str, default="adap_setup_tmp")
        parser.add_argument('--error_report', type=str, default="error_report_file.txt")
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

        for metadata_file in glob.glob(os.path.join(metadata_search_path, "**/*.ecsv"), recursive=True):
            target_dir = os.path.dirname(metadata_file)
            metadata = PypeItMetaData(spectrograph, par, data = Table.read(metadata_file, format="ascii.ecsv"))
            if is_metadata_complete(metadata) and os.path.basename(target_dir) == 'incomplete':
                new_target_dir = os.path.join(os.path.dirname(target_dir), "complete")
                os.rename(target_dir, new_target_dir)
                target_dir = new_target_dir
                raw_dir = os.path.join(target_dir, "raw")
                if not cloudstorage.is_cloud_uri(args.out_dir):
                    metadata.table['directory'] = Column(data=np.full(len(metadata.table), raw_dir), name='directory')
            
            # Transfer files within the cloud, updating the metadata directory to point to the new cloud URI
            if cloudstorage.is_cloud_uri(args.out_dir):
                cloud_dir = os.path.join(args.out_dir, os.path.relpath(target_dir, args.local_out), 'raw')
                file_list = [os.path.join(row['directory'], row['filename']) for row in metadata.table]
                transfer_data(args, file_list, cloud_dir)
                metadata.table['directory'] = Column(data=np.full(len(metadata.table), cloud_dir), name='directory')

            # Run Setup
            run_setup(args, target_dir, metadata)


        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        print(f"Total runtime: {total_time}")
            
        return 0

if __name__ == '__main__':
    BatchSetup.entry_point()
