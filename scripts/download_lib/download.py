import argparse
import sys

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

def report(results):
    '''
    Print a summary of every dataset that did not come down cleanly.

    Returns the number of datasets that need attention.
    '''
    bad = [r for r in results if r[1] != 'COMPLETE']

    print('')
    print(f'{len(results) - len(bad)} of {len(results)} datasets complete')

    if len(bad) == 0:
        return 0

    print('')
    print('These datasets did not download cleanly:')
    for dataset, status, detail in bad:
        print(f'  {status:10} {dataset}  ({detail})')

    return len(bad)


def main():
    '''
    Main function to query the KOA database for a target object
    and download the data for each night it was observed.

    Exits non-zero if any target or dataset failed, so that a caller can tell a run
    that returned everything from one that quietly returned part of it.
    '''
    args = parse_args()

    targets = parse_target_file(args.filename)

    results = []
    failed_targets = []

    for target in targets:
        # Create a query object
        query = Query.Query(target, topdir=args.outdir)
        query.query_position()

        # A query that errored and a query that found nothing both leave obj_results
        # empty, but only the first is a failure. query_error tells them apart.
        if query.query_error is not None:
            print(f'FAILED {target.name}: {query.query_error}')
            failed_targets.append(target.name)
            continue

        # query_position leaves obj_results as the empty list it was initialized to if the
        # KOA query failed, and returns a zero row table if the query found nothing, so
        # check for both rather than just None.
        if query.obj_results is None or len(query.obj_results) == 0:
            print(f'No data found for target: {target.name} {target.ra} {target.dec}')
            continue
        names = [str(d) for d in list(numpy.unique(query.obj_results['targname']))]
        for name in names:
            print(f'Found target name: {name}')

        results.extend(DownloadUtils.fill_dates(query, target, test=args.test,
                                                nodownload=args.nodownload))

    n_bad = report(results)

    if len(failed_targets) > 0:
        print('')
        print('These targets could not be queried at all:')
        for name in failed_targets:
            print(f'  {name}')

    return 1 if (n_bad > 0 or len(failed_targets) > 0) else 0

if __name__ == "__main__":
    sys.exit(main())
