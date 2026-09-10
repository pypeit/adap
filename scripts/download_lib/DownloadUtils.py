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
        # KOA delivers frames either uncompressed or gzipped, and "*.fits" does not match
        # "*.fits.gz", so both patterns are needed. Anything left behind in lev0 is
        # invisible to trimming_setup.py, which globs the raw directory itself without
        # recursing into it.
        for pattern in (f"{instr}*.fits", f"{instr}*.fits.gz"):
            for fn in glob.glob(os.path.join(raw,"lev0",pattern)):
                os.rename(fn, os.path.join(raw,os.path.basename(fn)))

    # Remove the scaffolding this function created but did not fill: the lev0 directories
    # the frames were just moved out of, and then raw_b/raw_r themselves for an arm that
    # had no data this night, since both are created unconditionally above. os.rmdir only
    # removes an empty directory, so an arm that did get frames is left untouched. lev0
    # has to go first or its parent would not be empty yet.
    for d in (os.path.join(raw_b,"lev0"), os.path.join(raw_r,"lev0"), raw_b, raw_r):
        try:
            os.rmdir(d)
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


def verify_download(download_dir, table_fn):
    """
    Check that every frame named in a final table is actually present on disk.

    This is the only reliable check that a download worked. Koa.download catches
    per-file errors internally, prints them and continues to the next row, so it
    returns normally even when nothing at all was retrieved -- and Query.download
    can only report the rarer case where it raised outright.

    Call this *after* file_cleanup, which is what moves the frames out of the lev0
    subdirectory KOA delivers them into and up to download_dir itself.

    Returns (n_expected, missing, readable). A frame that stayed behind in lev0 counts
    as missing, which is correct: trimming_setup.py globs the raw directory without
    recursing, so it would not see that file either.
    """
    try:
        table = astropy.table.Table.read(table_fn, format='ascii.ipac')
    except Exception as e:
        print(f'Warning: could not read {table_fn} to verify the download: {e}')
        return 0, [], False

    koaids = [str(k) for k in list(table['koaid'])]
    missing = [k for k in koaids
               if not os.path.exists(os.path.join(download_dir, k))]

    return len(koaids), missing, True


def check_night(target, fn_date, attempts):
    """
    Collapse one night's per-table download attempts into one result per dataset.

    attempts is a list of (instr, download_dir, table_fn, ran) tuples, one per final
    table. Each dataset covers three of them -- science, arcs and flats -- and is
    only usable if all three arrived, so they are verified together.

    Returns a list of (dataset, status, detail) tuples, where dataset is the
    "<target>/<date>/<instrument>" name and status is COMPLETE, INCOMPLETE or FAILED.
    """
    results = []

    for instr in ("LRIS", "LRISBLUE"):
        rows = [a for a in attempts if a[0] == instr]
        if len(rows) == 0:
            continue

        dataset = f'{target.name}/{fn_date}/{instr}'
        expected = 0
        missing = []
        raised = False
        unreadable = []

        for _, download_dir, table_fn, ran in rows:
            if ran is False:
                raised = True
            n_expected, n_missing, readable = verify_download(download_dir, table_fn)
            if readable is False:
                # Without the table there is no list of frames to check against, so the
                # dataset cannot be called complete whatever is on disk.
                unreadable.append(os.path.basename(table_fn))
                continue
            expected += n_expected
            missing.extend(n_missing)

        if len(unreadable) > 0:
            detail = 'could not read ' + ', '.join(unreadable)
            print(f'FAILED {dataset}: {detail}')
            results.append((dataset, 'FAILED', detail))
            continue

        # What is on disk is the authority. A download call that raised while every
        # frame is nonetheless present is not a problem -- that is what a retry over
        # already fetched data looks like -- so it is noted but does not fail anything.
        if len(missing) == 0:
            detail = f'{expected} frames'
            if raised:
                detail += ' (a download call raised, but nothing is missing)'
            print(f'Verified {dataset}: {detail}')
            results.append((dataset, 'COMPLETE', detail))
            continue

        detail = f'{len(missing)} of {expected} frames missing'
        if raised:
            detail += ', and a download call failed'
        # Nothing arriving, or the call raising, points at KOA or the transport and is
        # worth retrying. Most frames arriving and a few not is more likely to be a
        # problem with those specific frames, and wants looking at rather than a retry.
        status = 'FAILED' if (raised or len(missing) == expected) else 'INCOMPLETE'
        print(f'{status} {dataset}: {detail}')
        for koaid in missing:
            print(f'    missing: {koaid}')
        results.append((dataset, status, detail))

    return results


def fill_dates(query, target, test=False, nodownload=False):
    """
    Fill the target object with the dates it was observed and download each night.

    Returns a list of (dataset, status, detail) tuples, one per dataset the night
    produced, where status is COMPLETE, INCOMPLETE or FAILED. Nights that KOA simply
    has no data for produce no entry -- that is not a failure. Returns an empty list
    in test or nodownload mode, where there is nothing on disk to verify.
    """
    results = []

    target.dates = [ str(d) for d in list(numpy.unique(query.obj_results['date_obs']))]

    for cdate in target.dates:
        fn_date = re.sub("-","",cdate)

        # Create a night object
        night = Night.Night(cdate)

        mtch = query.obj_results['date_obs'] == cdate

        night.fill_night(query, mtch)

        if night.graname is None and night.grisname is None:
            continue

        query.query_date(night)

        # query_date leaves date_results as None if the KOA query failed, so skip this
        # night rather than letting build_final_table raise on it. date_error tells the
        # two apart: a night KOA errored on has to be retried, a night with genuinely no
        # frames does not.
        if query.date_results is None or len(query.date_results) == 0:
            if query.date_error is not None:
                print(f'FAILED {target.name}/{fn_date}: {query.date_error}')
                results.append((f'{target.name}/{fn_date}', 'FAILED', query.date_error))
            else:
                print(f'No KOA results for {target.name} for {night.date}')
            continue

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

            # Remember which table went to which directory, so the frames can be checked
            # once file_cleanup has moved them up out of lev0.
            attempts = []

            for final_table_name in final_table_names:
                if "LRISBLUE" in final_table_name:
                    instr, raw = 'LRISBLUE', 'raw_b'
                else:
                    instr, raw = 'LRIS', 'raw_r'
                query.download_dir = os.path.join(query.outdir,fn_date,instr,raw)
                ran = True
                if nodownload is False:
                    ran = query.download(final_table_name)
                attempts.append((instr, query.download_dir, final_table_name, ran))

            file_cleanup(query, fn_date)

            if nodownload is False:
                results.extend(check_night(target, fn_date, attempts))

    return results
