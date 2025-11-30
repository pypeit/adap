class Target():
    """
    Class to store information about a target object.
    Includes a list of dates where the target was observed.
    """
    def __init__(self, name, ra=None, dec=None) -> None:
        self.name = name
        self.ra = ra
        self.dec = dec
        self.dates = []

    def __str__(self) -> str:
        ostr = f'<Target {self.name}'
        if self.ra:
            ostr += f' {self.ra} {self.dec}'
        if len(self.dates) > 0:
            ostr += f' {self.dates}'
        ostr += '>'
        return ostr

    def __repr__(self) -> str:
        return self.__str__()
