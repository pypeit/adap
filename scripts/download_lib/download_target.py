import argparse
import sys

import numpy

import Target
import DownloadUtils
import Query

def parse_args():

    """
    Parse the command line arguments
    """

    parser = argparse.ArgumentParser(description='Download data from the KOA database for objects in a file')
    parser.add_argument('target', type=str, \
                        help='Name of the file containing target names amd coordinates, one per line')
    parser.add_argument('ra', type=str, \
                        help='Right ascension of the target')
    parser.add_argument('dec', type=str, \
                        help='Declination of the target')
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

    # Create a query object
    target = Target.Target(args.target, ra=args.ra, dec=args.dec)
    query = Query.Query(target, topdir=args.outdir)
    query.query_position()
    if query.obj_results is None:
        print(f'No data found for target: {target.name} {target.ra} {target.dec}')
        sys.exit()
    names = [str(d) for d in list(numpy.unique(query.obj_results['targname']))]
    for name in names:
        print(f'Found target name: {name}')

    DownloadUtils.fill_dates(query, target, test=args.test, nodownload=args.nodownload)

if __name__ == "__main__":
    main()
