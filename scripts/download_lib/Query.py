import os
import pykoa.koa as koa
import astropy.table

class Query():
    '''
    Query: Query the KOA database for a target object
    '''
    def __init__(self, target, topdir='.') -> None:

        self.instr = 'lris'
        self.koa_table = 'koa_lris'
        self.outdir = f'{topdir}/{target.name}'
        self.download_dir = os.path.join(self.outdir,'raw')
        self.target = target
        self.obj_results = []
        self.date_results = None
        # A KOA query that raises and one that legitimately finds nothing both leave
        # obj_results/date_results empty, and the two need very different responses: the
        # first is a failure to retry, the second is just a target that was never
        # observed. These record that a query actually errored.
        self.query_error = None
        self.date_error = None
        self.query_keys = 'koaid, object, instrume, koaimtyp, frameno, ra, dec,  \
            to_char(date_obs,\'YYYY-MM-DD\') as date_obs, elaptime, binning, \
            airmass, dichname, graname, grisname, slitname, trapdoor, \
            progid, proginst,  progpi, progtitl, semester, ofname, filehand, \
            argon, cadmium, mercury, neon, zinc '
        if os.path.exists(self.outdir) is False:
            os.makedirs(self.outdir)

    def __str__(self) -> str:
        return f'<Query {self.target} {self.outdir} {self.download_dir} {len(self.obj_results)}>'

    def __repr__(self) -> str:
        return self.__str__()

    def query_object(self) -> None:
        '''
        query_object: Query the KOA database for the target object
        '''

        otbl = os.path.join(self.outdir,self.instr + "_object.tbl")
        if os.path.exists(otbl):
            self.obj_results = astropy.table.Table.read(otbl, format='ascii.ipac')
            return

        # Create a KOA client
        if os.path.exists(otbl) is False:
            try:
                koa.Koa.query_object(self.instr, self.target.name, otbl,
                                        radius=0.01)
            except Exception as e:
                self.query_error = f'KOA query_object failed for {self.target.name}: {e}'
                print(f'Warning: {self.query_error}')
                return

        # save the results
        if os.path.exists(otbl):
            self.obj_results = astropy.table.Table.read(otbl, format='ascii.ipac')
        return

    def query_position(self) -> None:
        '''
        query_position: Query the KOA database for the target object
        Defaults to a 5" radius search
        '''

        ptbl = os.path.join(self.outdir,self.instr + "_position.tbl")
        if os.path.exists(ptbl):
            # this means the query has already been performed
            self.obj_results = astropy.table.Table.read(ptbl, format='ascii.ipac')
            return

        circle_pos = f'circle {self.target.ra} {self.target.dec} {5/3600.0:0.5f}'

        # Create a KOA client
        if os.path.exists(ptbl) is False:
            try:
                koa.Koa.query_position(self.instr, circle_pos, ptbl)
            except Exception as e:
                self.query_error = f'KOA query_position failed for {self.target.name}: {e}'
                print(f'Warning: {self.query_error}')
                return

        # save the results. A query that raised nothing but wrote no table has still
        # failed -- it is not the same as a search that found no matching frames.
        if os.path.exists(ptbl):
            self.obj_results = astropy.table.Table.read(ptbl, format='ascii.ipac')
        else:
            self.query_error = (f'KOA query_position wrote no results table for '
                                f'{self.target.name}')
            print(f'Warning: {self.query_error}')
        return

    def gen_query(self, night, instr) -> str:
        '''
        gen_query: Generate the query string for the KOA database
        '''

        binning = night.red_binning if instr == 'LRIS' else night.blue_binning

        query =  "select " + self.query_keys + " from " + self.koa_table
        query = query + f" where to_char(date_obs,'YYYY-MM-DD') = '{night.date}'"
        query = query + f" and binning like '{binning}'"
        query = query + f" and dichname like '{night.dichname}'"
        query = query + f" and slitname like '{night.slitname}'"
        if instr == 'LRIS':
            query = query + f" and graname like '{night.graname}'"
            query = query + f" and round(grangle, 2) = {night.grangle:0.2f}"
        else:
            query = query + f" and grisname like '{night.grisname}'"
        query = query + f" and instrume like '{instr}'"

        return query

    def query_date(self, night) -> None:
        '''
        query_date: Query the KOA database for the target object on a specific date
        '''

        # Results are accumulated across the two instruments below, so each night has to
        # start from empty. Otherwise date_results carries over from previous nights, and
        # the final tables filter on instrument configuration but not on date, so an
        # earlier night with the same configuration would be downloaded again.
        self.date_results = None
        self.date_error = None
        errors = []

        for instr in ("LRIS", "LRISBLUE"):

            if instr == "LRIS" and night.graname is None:
                continue
            if instr == "LRISBLUE" and night.grisname is None:
                continue

            query = self.gen_query(night, instr)

            night.date_file = os.path.join(self.outdir,instr + f"_adql_{night.date}.tbl")

            # Create a KOA client. A failure for one instrument must not stop the other
            # from being queried, so move on to it rather than abandoning the night.
            if os.path.exists(night.date_file) is False:
                try:
                    koa.Koa.query_adql(query, night.date_file, format='ipac')
                except Exception as e:
                    errors.append(f'query_adql failed for {instr} on {night.date}: {e}')
                    print(f'Warning: KOA query_adql failed for {instr} on {night.date}')
                    continue

            # save the results. A query that returned nothing leaves no table to read,
            # which likewise must not stop the other instrument.
            try:
                t = astropy.table.Table.read(night.date_file, format='ascii.ipac')
            except Exception as e:
                errors.append(f'no readable KOA results for {instr} on {night.date}: {e}')
                print(f'Warning: no readable KOA results for {instr} on {night.date}')
                continue

            if self.date_results is None:
                self.date_results = t
            else:
                self.date_results = astropy.table.vstack([self.date_results, t])

        # Only report an error if nothing at all came back. One arm failing while the
        # other returns data is the tolerated case the loop above is written for.
        if self.date_results is None and len(errors) > 0:
            self.date_error = '; '.join(errors)

        return

    def frame_path(self, koaid):
        '''
        frame_path: Where a frame lives once it has been downloaded, or None if it
        has not been.

        There are two places to look. KOA delivers into a "lev0" subdirectory of
        download_dir, which is where Koa.download writes and where its own per-file
        resume check looks. DownloadUtils.file_cleanup then moves the frames up into
        download_dir itself, which is where trimming_setup.py expects them. A frame
        may therefore be in either place depending on whether cleanup has run for
        that night yet, and both count as downloaded.
        '''
        for path in (os.path.join(self.download_dir, koaid),
                     os.path.join(self.download_dir, 'lev0', koaid)):
            if os.path.exists(path):
                return path
        return None

    def missing_frames(self, table_fn) -> list:
        '''
        missing_frames: The koaids in a final table that are not yet on disk.
        '''
        t = astropy.table.Table.read(table_fn, format='ascii.ipac')
        return [str(f) for f in list(t['koaid']) if self.frame_path(str(f)) is None]

    def check_if_downloaded(self, table_fn) -> bool:
        '''
        check_if_downloaded: Have *all* the files in this table already been downloaded?

        This used to return True as soon as it found any one file from the table, which
        meant a download interrupted after its first frame was never resumed -- the next
        attempt saw that one file and skipped the whole table. Completeness is the only
        useful question here.
        '''
        return len(self.missing_frames(table_fn)) == 0

    def write_retry_table(self, table_fn, missing) -> str:
        '''
        write_retry_table: Write a copy of a final table holding only the rows still
        needed, and return its path.

        Koa.download skips frames it finds at "lev0/<koaid>" under the output directory,
        so it resumes correctly within a single night's run. It cannot resume across
        runs, because file_cleanup has by then moved those frames up out of lev0 and it
        no longer sees them -- it would fetch the whole table again. Handing it only the
        missing rows makes the resume explicit and independent of where the frames that
        did arrive ended up.
        '''
        table = astropy.table.Table.read(table_fn, format='ascii.ipac')
        wanted = set(missing)
        subset = table[[str(k) in wanted for k in list(table['koaid'])]]

        base, ext = os.path.splitext(table_fn)
        retry_fn = base + '_retry' + ext
        subset.write(retry_fn, format='ascii.ipac', overwrite=True)

        return retry_fn

    def download(self, table_fn) -> bool:
        '''
        download: Download the files from the KOA database

        Returns False if Koa.download raised, True otherwise.

        A True return is *not* evidence that the frames arrived. Koa.download catches
        per-file download errors itself, prints them and carries on to the next row, so
        it returns normally even when every file in the table failed. Only
        DownloadUtils.verify_download establishes what is actually on disk.
        '''
        # Download the files
        if os.path.exists(self.download_dir) is False:
            os.makedirs(self.download_dir)

        missing = self.missing_frames(table_fn)

        if len(missing) == 0:
            return True

        # Only ask KOA for what is not already here. On a first attempt that is the whole
        # table; on a retry it is the remainder.
        request_fn = table_fn
        total = len(astropy.table.Table.read(table_fn, format='ascii.ipac'))
        if len(missing) < total:
            print(f'Resuming {os.path.basename(table_fn)}: '
                  f'{len(missing)} of {total} frames still needed')
            request_fn = self.write_retry_table(table_fn, missing)

        try:
            koa.Koa.download(request_fn, 'ipac', self.download_dir)
        except Exception as e:
            print(f'Warning: KOA download failed for {table_fn}: {e}')
            return False

        return True
