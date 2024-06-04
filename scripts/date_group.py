from astropy.table import vstack
from astropy.time import TimeDelta

class DateGroup():
    def __init__(self, args, dt, metadata):
        self.metadata = metadata
        self.window_delta = TimeDelta(args.date_window, format='jd')
        self.start_date = dt
        self.end_date = dt

    def add_metadata_row(self, row, dt):
        self._add_date(dt)
        self.metadata.add_row(row)
    
    def _add_date(self, dt):
        if dt < self.start_date:
            self.start_date = dt
        elif dt > self.end_date:
            self.end_date = dt

    def is_date_in_window(self, dt):
        # Find the date window for this group and see if the given date
        # fits into it
        start = self.start_date - self.window_delta
        end = self.end_date + self.window_delta
        return dt >= start and dt <= end

    @property
    def window(self):
        return (self.start_date - self.window_delta, self.end_date + self.window_delta)

    def merge(self, other_dg):
        if other_dg.start_date < self.start_date:
            self.start_date = other_dg.start_date

        if other_dg.end_date > self.end_date:
            self.end_date = other_dg.end_date

        self.metadata = vstack([self.metadata, other_dg.metadata])

    def get_dir_name(self):
        start_date_text = self.start_date.to_value('iso', subfmt='date')
        end_date_text   = self.end_date.to_value('iso', subfmt='date')
        if start_date_text != end_date_text:
            return  start_date_text + "_" + end_date_text
        else:
            return start_date_text       
