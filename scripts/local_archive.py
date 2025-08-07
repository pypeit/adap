#!/usr/bin/env python3

import os
from pathlib import Path
import shutil
import datetime
import argparse
import sys

from pypeit.archive import ArchiveDir

   
from archive import create_metadata_archives, populate_archive, write_messages

def get_parser():

    parser = argparse.ArgumentParser(Path(sys.argv[0]).name, description='Create an archive of fits files and metadata for submission to KOA.')

    parser.add_argument('archive_dir', type=str, help="Directory containing the files being sent to KOA.")
    parser.add_argument('--copy', type=str, default=None, help="Copy the archive to the given location")
    parser.add_argument('--report', type=str, default="report.txt", help="Location of a report file indicating any missing files. Defaults to report.txt.")
    parser.add_argument("--subdirs", type=str, nargs="*", default=[], help="List of subdirectories of archive_dir to limit the search for files to.")
    return parser


def main(args):
    start_time = datetime.datetime.now()

    # Create the archive objects. This will create directories if needed, or open up
    # metadata for files in a pre-existing archive
    if args.copy is not None:
        dest_archive_root = Path(args.copy)
        dest_archive_root.mkdir(parents=True, exist_ok=True)
        copy_to_archive=True
    else:
        dest_archive_root = Path(args.archive_dir)
        copy_to_archive=False

    metadata_archives = create_metadata_archives(dest_archive_root)
    source_archive_root = Path(args.archive_dir)
    archive = ArchiveDir(dest_archive_root, metadata_archives, copy_to_archive=copy_to_archive)

    # Recursively scan all of the source directories from the command line arguments
    if len(args.subdirs) > 0:
        dirs_to_scan = [Path(source_archive_root) / subdir for subdir in args.subdirs]
    else:
        dirs_to_scan = [Path(source_archive_root)]

    messages = populate_archive(archive,source_archive_root,dirs_to_scan)
    archive.save()

    print(f"Copying README to archive root.")
    script_path = Path(__file__).parent.absolute().joinpath("archive_README")
    shutil.copy2(script_path, dest_archive_root.joinpath("README"))

    end_time = datetime.datetime.now()
    timing_strings = [f"Started {start_time.isoformat()}",
                      f"Finished {end_time.isoformat() }",
                      f"Duration {end_time - start_time }"]

    write_messages(args.report, messages)

    for s in timing_strings:
        print(s)
    return 0

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))


