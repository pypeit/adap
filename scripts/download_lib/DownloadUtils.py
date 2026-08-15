import os
import glob
import re

import numpy
import astropy.table
from astropy import units
import astropy.coordinates

import pypeit.core.standard
import Night

def match_sci_target(results, target):
    """
    Match the target name to the science target name
    """

    if pypeit.core.standard.get_archive_standard(results['ra'], results['dec'],
                                              tol=0.5, check=True):
        return True
    else:
        targ_coord = astropy.coordinates.SkyCoord(ra=target.ra, dec=target.dec,
                                                    unit=(units.deg, units.deg))
        sci_coord = astropy.coordinates.SkyCoord(ra=results['ra'], dec=results['dec'],
                                                    unit=(units.deg, units.deg))
        sep = targ_coord.separation(sci_coord)
        if sep < 20.*units.arcsec:
            return True

    return False

def file_cleanup(query, cdate) -> None:
    """
    Clean up the files
    """

    raw_b = os.path.join(query.outdir,cdate,'LRISBLUE','raw_b')
    raw_r = os.path.join(query.outdir,cdate,'LRIS','raw_r')

    for d in (raw_b, raw_r):
        if os.path.exists(d) is False:
            os.makedirs(d)

    for instr in ("LB","LR"):
        raw = raw_b if instr == "LB" else raw_r
        for fn in glob.glob(os.path.join(raw,"lev0",f"{instr}*.fits")):
            os.rename(fn, os.path.join(raw,os.path.basename(fn)))

    try:
        os.rmdir(os.path.join(query.outdir,cdate,"raw","lev0"))
        os.rmdir(os.path.join(query.outdir,cdate,"raw"))
    except OSError:
        pass

    return

def build_final_flat_list(query, instr, night):
    """
    Build the final arc list for the given date and instrument
    """

    final_flat_table_name = instr + "_" + night.date + "_flats.tbl"
    final_flat_table_name = os.path.join(query.outdir, final_flat_table_name)

    mtch = query.date_results['instrume'] == instr
    mtch &= query.date_results['dichname'] == night.dichname
    if instr == "LRIS":
        mtch &= query.date_results['graname'] == night.graname
        mtch &= query.date_results['binning'] == night.red_binning
    else:
        mtch &= query.date_results['grisname'] == night.grisname
        mtch &= query.date_results['binning'] == night.blue_binning

    flats = mtch & (query.date_results['koaimtyp'] == 'flatlamp')

    domes = flats & (query.date_results['trapdoor'] == 'open')
    internals = flats & (query.date_results['trapdoor'] == 'closed')

    if len(query.date_results['koaid'][domes]) > 0:
        dome_table = query.date_results[domes]
        dome_table.write(final_flat_table_name, format='ascii.ipac', overwrite=True)
        return final_flat_table_name

    if len(query.date_results['koaid'][internals]) > 0:
        internal_table = query.date_results[internals]
        internal_table.write(final_flat_table_name, format='ascii.ipac', overwrite=True)
        return final_flat_table_name

    return None

def build_final_arc_table(query, instr, night):
    """
    Build the final arc table
    returns the name
    """

    final_arc_table_name = instr + "_" +  night.date + "_arcs.tbl"
    final_arc_table_name = os.path.join(query.outdir, final_arc_table_name)

    mtch = query.date_results['instrume'] == instr
    mtch &= query.date_results['dichname'] == night.dichname
    mtch &= (query.date_results['koaimtyp'] == 'arclamp')

    if instr == "LRIS":
        mtch &= query.date_results['graname'] == night.graname
        mtch &= query.date_results['binning'] == night.red_binning
    else:
        mtch &= query.date_results['grisname'] == night.grisname
        mtch &= query.date_results['binning'] == night.blue_binning

    if len(query.date_results['koaid'][mtch]) == 0:
        return None

    delta = numpy.abs(query.date_results['dec'][mtch] - float(query.target.dec))
    delta_min_ind = (delta <= (delta.min()+1))
    arcs = numpy.zeros_like(mtch, dtype=bool)
    arcs[mtch] = delta_min_ind
    n_arc = len(query.date_results['koaid'][arcs])

    if n_arc == 0:
        return None

    final_table = query.date_results[arcs]
    final_table.write(final_arc_table_name, format='ascii.ipac', overwrite=True)

    return final_arc_table_name

def build_final_table(night, query):
    """
    Build the final table
    returns the name
    """

    final_tables = []

    for instr in ("LRIS", "LRISBLUE"):

        final_table_name = instr + "_" + night.date + "_" + query.target.name + ".tbl"
        final_table_name = os.path.join(query.outdir, final_table_name)

        mtch = query.date_results['instrume'] == instr
        mtch &= query.date_results['dichname'] == night.dichname
        if instr == "LRIS":
            lamps = (query.date_results['neon'] == 'on') | \
                (query.date_results['argon'] == 'on') | (query.date_results['mercury'] == 'on')
            mtch &= query.date_results['graname'] == night.graname
            mtch &= query.date_results['binning'] == night.red_binning
        else:
            lamps = (query.date_results['cadmium'] == 'on') | \
                (query.date_results['zinc'] == 'on')
            mtch &= query.date_results['grisname'] == night.grisname
            mtch &= query.date_results['binning'] == night.blue_binning


        arcs = mtch & (query.date_results['koaimtyp'] == 'arclamp') & lamps
        science = mtch & (query.date_results['koaimtyp'] == 'object')
        n_arc = len(query.date_results['koaid'][arcs])

        use = numpy.zeros(len(query.date_results), dtype=bool)
        for frame in query.date_results[science]:
            sep_match = match_sci_target(frame, query.target)
            if sep_match:
                use[query.date_results['koaid'] == frame['koaid']] = True

        final_arc_table_name = build_final_arc_table(query, instr, night)
        if final_arc_table_name is None:
            continue

        final_flat_table_name = build_final_flat_list(query, instr, night)
        if final_flat_table_name is None:
            continue

        final_table = query.date_results[(science & use)]
        if len(final_table) == 0 or n_arc == 0:
            continue

        final_table.write(final_table_name, format='ascii.ipac', overwrite=True)
        final_tables.append(final_table_name)
        final_tables.append(final_flat_table_name)
        final_tables.append(final_arc_table_name)

    return final_tables


def fill_dates(query, target, test=False, nodownload=False):
    """
    Fill the target object with the dates it was observed
    """
    target.dates = [ str(d) for d in list(numpy.unique(query.obj_results['date_obs']))]

    for cdate in target.dates:
        # Create a night object
        night = Night.Night(cdate)

        mtch = query.obj_results['date_obs'] == cdate

        night.fill_night(query, mtch)

        if night.graname is None and night.grisname is None:
            continue

        query.query_date(night)

        final_table_names = build_final_table(night, query)
        if len(final_table_names) == 0:
            print(f'No files found for {target.name} for {night.date}')
            continue

        if test:
            print(f'Test mode: Not downloading files for {target.name} for {night.date}')
            print('Would have downloaded:')
            for final_table_name in final_table_names:
                for row in astropy.table.Table.read(final_table_name, format='ascii.ipac'):
                    print(row[0])
        else:
            print(f'Downloading files for {target.name} for {night.date}')

            fn_date = re.sub("-","",cdate)

            for final_table_name in final_table_names:
                if "LRISBLUE" in final_table_name:
                    instr, raw = 'LRISBLUE', 'raw_b'
                else:
                    instr, raw = 'LRIS', 'raw_r'
                query.download_dir = os.path.join(query.outdir,fn_date,instr,raw)
                if nodownload is False:
                    query.download(final_table_name)
            file_cleanup(query, fn_date)
