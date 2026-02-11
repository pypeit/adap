import argparse

import numpy

import Target
import Query
import DownloadUtils

def parse_target_file(filename):
    """
    Parse the target file
    """

    targets = []

    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('#') or line.strip() == '':
                continue
            parts = line.split()
            name = parts[0]
            ra = parts[1] if len(parts) > 1 else None
            dec = parts[2] if len(parts) > 2 else None
            targets.append(Target.Target(name, ra, dec))
    return targets


def parse_args():

    """
    Parse the command line arguments
    """

    parser = argparse.ArgumentParser(description='Download data from the KOA database for objects in a file')
    parser.add_argument('filename', type=str, \
                        help='Name of the file containing target names amd coordinates, one per line')
    parser.add_argument('-o','--outdir', type=str, default='.', \
                        help='Output directory, defaults to .')
    parser.add_argument('-t','--test', action='store_true', help='Run in test mode')
    parser.add_argument('-n','--nodownload', action='store_true', help='Skip file download')


    args = parser.parse_args()

    return args

def main():
    '''
    Main function to query the KOA database for a target object
    and download the data for each night it was observed.
    '''
    args = parse_args()

    targets = parse_target_file(args.filename)

    for target in targets:
        # Create a query object
        query = Query.Query(target, topdir=args.outdir)
        query.query_position()
        if query.obj_results is None:
            print(f'No data found for target: {target.name} {target.ra} {target.dec}')
            continue
        names = [str(d) for d in list(numpy.unique(query.obj_results['targname']))]
        for name in names:
            print(f'Found target name: {name}')

        DownloadUtils.fill_dates(query, target, test=args.test, nodownload=args.nodownload)

if __name__ == "__main__":
    main()
