import datetime
import numpy

class Night():
    """
    Class to store the information of a night
    associated with a target.
    """
    def __init__(self, date) -> None:

        self.date = date
        delta = datetime.timedelta(days=3)
        date_start = datetime.datetime.strptime(date, '%Y-%m-%d') - delta
        date_end = datetime.datetime.strptime(date, '%Y-%m-%d') + delta
        self.date_start = date_start.strftime('%Y-%m-%d')
        self.date_end = date_end.strftime('%Y-%m-%d')
        self.date_file = None
        self.grisname = None
        self.graname = None
        self.grangle = None
        self.slitname = None
        self.red_binning = None
        self.blue_binning = None
        self.dichname = None

    def __str__(self) -> str:
        return f'<Night {self.date} Grism={self.grisname} Grating={self.graname} Dichroic={self.dichname} Slit={self.slitname}>'

    def __repr__(self) -> str:
        return self.__str__()

    def fill_night(self, query, mtch):
        """
        Fill the night object with information from the query.
        """
        self.date = query.obj_results['date_obs'][mtch][0]

        red = query.obj_results['instrume'] == 'LRIS'
        blue = query.obj_results['instrume'] == 'LRISBLUE'

        good_gra = query.obj_results['graname'] == '400/8500'
        if numpy.any(mtch & good_gra & red):
            self.graname = query.obj_results['graname'][mtch & good_gra & red][0]

        good_gris = (query.obj_results['grisname'] == '600/4000') | \
            (query.obj_results['grisname'] == '400/3400')
        if numpy.any(mtch & good_gris & blue):
            self.grisname = query.obj_results['grisname'][mtch & good_gris & blue][0]

        if self.graname is not None:
            self.red_binning = query.obj_results['binning'][mtch & red][0]
            self.grangle = query.obj_results['grangle'][mtch & red][0]
        if self.grisname is not None:
            self.blue_binning = query.obj_results['binning'][mtch & blue][0]

        if self.red_binning is None:
            self.graname = None
        if self.blue_binning is None:
            self.grisname = None

        if self.graname is not None or self.grisname is not None:
            self.dichname = query.obj_results['dichname'][mtch][0]

        if self.slitname is None:
            self.slitname = query.obj_results['slitname'][mtch][0]

        return
