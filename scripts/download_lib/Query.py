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
            except Exception:   
                print(f'Warning: KOA query_object failed for {self.target.name}')
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
            except Exception:
                print(f'Warning: KOA query_position failed for {self.target.name}')
                return

        # save the results
        if os.path.exists(ptbl):
            self.obj_results = astropy.table.Table.read(ptbl, format='ascii.ipac')
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

        for instr in ("LRIS", "LRISBLUE"):

            if instr == "LRIS" and night.graname is None:
                continue
            if instr == "LRISBLUE" and night.grisname is None:
                continue

            query = self.gen_query(night, instr)

            night.date_file = os.path.join(self.outdir,instr + f"_adql_{night.date}.tbl")

            # Create a KOA client
            if os.path.exists(night.date_file) is False:
                try:
                    koa.Koa.query_adql(query, night.date_file, format='ipac')
                except Exception:
                    print(f'Warning: KOA query_adql failed for {night.date}')
                    return

            # save the results
            t = astropy.table.Table.read(night.date_file, format='ascii.ipac')
            if self.date_results is None:
                self.date_results = t
            else:
                self.date_results = astropy.table.vstack([self.date_results, t])

        return

    def check_if_downloaded(self, table_fn) -> bool:
        '''
        check_if_downloaded: Check if the files have already been downloaded
        '''
        t = astropy.table.Table.read(table_fn, format='ascii.ipac')
        filenames = [str(f) for f in  list(t['koaid'])]
        exists = False
        for fn in filenames:
            if os.path.exists(os.path.join(self.download_dir,fn)):
                exists = True
                break
        return exists

    def download(self, table_fn) -> None:
        '''
        download: Download the files from the KOA database
        '''
        # Download the files
        if os.path.exists(self.download_dir) is False:
            os.makedirs(self.download_dir)

        exists = self.check_if_downloaded(table_fn)

        if exists is False:
            try:
                koa.Koa.download(table_fn, 'ipac', self.download_dir)
            except Exception:
                print(f'Warning: KOA download failed for {table_fn}')
        return
