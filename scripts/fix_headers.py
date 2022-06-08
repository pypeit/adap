#!/usr/bin/env python3
from datetime import date

from astropy.io import fits

hdul = fits.open("orig/DE.20030401.36154.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC, and TARGNAME values which were missing from the original."
hdul[0].header['RA'] = 189.1420833333333
hdul[0].header['DEC'] = 62.276805555555555
hdul[0].header['TARGNAME'] = 'goods1new'

hdul.writeto("updated/DE.20030401.36154.fits")
hdul.close()


hdul = fits.open("orig/DE.20070117.32982.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC values which were missing from the original."
hdul[0].header['RA'] = 150.39416666666665  
hdul[0].header['DEC'] = 1.7632222222222222

hdul.writeto("updated/DE.20070117.32982.fits")
hdul.close()

hdul = fits.open("orig/DE.20070414.33633.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC values which were missing from the original."
hdul[0].header['RA'] = 189.28404166666664
hdul[0].header['DEC'] = 62.25711111111111

hdul.writeto("updated/DE.20070414.33633.fits")
hdul.close()

hdul = fits.open("orig/DE.20080502.54142.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC, and TARGNAME values which were missing from the original."
hdul[0].header['RA'] = 67.35
hdul[0].header['DEC'] = 0.039
hdul[0].header['TARGNAME'] = 'HORIZON STOW'

hdul.writeto("updated/DE.20080502.54142.fits")
hdul.close()

hdul = fits.open("orig/DE.20100208.13304.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC values which were missing from the original."
hdul[0].header['RA'] = 57.99999999999999
hdul[0].header['DEC'] = 45.0

hdul.writeto("updated/DE.20100208.13304.fits")
hdul.close()

hdul = fits.open("orig/DE.20110401.48075.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC values which were missing from the original."
hdul[0].header['RA'] = 189.09133333333332
hdul[0].header['DEC'] = 62.20425

hdul.writeto("updated/DE.20110401.48075.fits")
hdul.close()

hdul = fits.open("orig/DE.20120323.39775.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC values which were missing from the original."
hdul[0].header['RA'] = 189.24349999999998
hdul[0].header['DEC'] = 62.244194444444446

hdul.writeto("updated/DE.20120323.39775.fits")
hdul.close()

"""
The math to find the airmass for DE.20090427.36288.fits
was done by averaging the elevation of two adjacent images

In [13]: avg_el = (81.27863136 + 74.05489800) / 2

In [14]: avg_el
Out[14]: 77.66676468

In [15]: avg_z = 90 - avg_el

In [16]: am = 1/math.cos(math.radians(avg_z))

In [17]: am
Out[17]: 1.0236233449387302
"""
hdul = fits.open("orig/DE.20090427.36288.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC, TARGNAME, AIRMASS values which were missing from the original."
hdul[0].header['RA'] = 200.95141666666663
hdul[0].header['DEC'] = 27.512722222222223
hdul[0].header['TARGNAME'] = 'sdf_25r'
hdul[0].header['AIRMASS'] = 1.0236233449387302

hdul.writeto("updated/DE.20090427.36288.fits")
hdul.close()

# Picked Airmass for this one from the previous short exposure since the next exposure
# was from a different mask
# 
hdul = fits.open("orig/DE.20040418.31045.fits")

hdul[0].header['COMMENT'] = f"This file was edited on {date.today().isoformat()} to set the RA, DEC, TARGNAME, AIRMASS values which were missing from the original."
hdul[0].header['RA'] = 189.13633333333328
hdul[0].header['DEC'] = 62.27983333333333
hdul[0].header['TARGNAME'] = 'hdf04b'
hdul[0].header['AIRMASS'] = 1.36312612

hdul.writeto("updated/DE.20040418.31045.fits")
hdul.close()



