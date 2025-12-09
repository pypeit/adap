import os
import argparse

import numpy

import Night
import Query
import Target

def enough_files(query, night):
    """
    Check if there are enough calibration files to download
    """
    enough = False

    for instr in ("LRIS", "LRISBLUE"):

        if len(query.date_results) > 0:
            mtch = query.date_results['instrume'] == instr
            mtch &= query.date_results['dichname'] == night.dichname
            if instr == "LRIS":
                mtch &= query.date_results['graname'] == night.graname
                mtch &= query.date_results['binning'] == night.red_binning
            else:
                mtch &= query.date_results['grisname'] == night.grisname
                mtch &= query.date_results['binning'] == night.blue_binning
            arc = mtch & (query.date_results['koaimtyp'] == 'arclamp')
            flat = mtch & (query.date_results['koaimtyp'] == 'flatlamp')
            n_arc = len(query.date_results['koaid'][arc])
            n_flat = len(query.date_results['koaid'][flat])
            if n_arc > 0 and n_flat > 3:
                enough = True

    return enough

def parse_args():
    """
    Parse the command line arguments
    """
    parser = argparse.ArgumentParser(description='Query the KOA database for a target object')
    parser.add_argument('targname', type=str, help='Name of the target object')
    parser.add_argument('ra', type=str, default=None,
                        help='RA of the target object in decimal degrees')
    parser.add_argument('dec', type=str, default=None,
                        help='Declination of the target object in decimal degrees')
    parser.add_argument('-D', '--download', action='store_true',
                        help='Download the files after querying')
    cl_args = parser.parse_args()

    return cl_args

def main():
    """
    Main function to query the KOA database for a target object
    """
    args = parse_args()

    # Create a target object
    target = Target.Target(args.targname, ra=args.ra, dec=args.dec)

    # Create a query object
    query = Query.Query(target)

    query.query_position()

    if query.obj_results is None:
        print(f'No data found for target: {args.targname} {args.ra} {args.dec}')
        return

    target.dates = [ str(d) for d in list(numpy.unique(query.obj_results['date_obs']))]

    for cdate in target.dates:
        # Create a night object
        night = Night.Night(cdate)

        mtch = query.obj_results['date_obs'] == cdate

        night.fill_night(query, mtch)

        # Query the KOA database for the target object on a specific date
        query.query_date(night)

        enough = enough_files(query, night)

        # Download the files from the KOA database
        if enough and args.download:
            query.download(night.date_file)
            print(f'Downloading files for {args.targname} for {night.date}')

    return

if __name__ == "__main__":
    main()
