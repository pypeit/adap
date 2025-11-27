import argparse

import astropy.io.fits as fits
import matplotlib.pyplot as plt
from scipy.interpolate import BSpline, make_interp_spline
import numpy

def parse_args():
    parser = argparse.ArgumentParser(
        description="Remove telluric absorption from spectrum."
    )
    parser.add_argument(
        "tell_file", type=str, help="File containing telluric absorption data."
    )
    parser.add_argument(
        "data_file", type=str, help="File containing observed spectrum data."
    )
    parser.add_argument(
        "--dichroic_min",
        type=float,
        default=5550,
        help="Minimum wavelength to consider (default: 5550 A).",
    )
    parser.add_argument(
        "--dichroic_max",
        type=float,
        default=10150,
        help="Maximum wavelength to consider (default: 10150 A).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Plot intermediate results.",
    )
    return parser.parse_args()

def get_tell(fn, dichroic_min=5550, dichroic_max=10150):

    hdus=fits.open(fn)
    tdata = hdus[1].data
    twave = tdata['wave'][0]
    tval = tdata['telluric'][0]

    tval = tval[(twave > dichroic_min)&(twave < dichroic_max)]
    twave = twave[(twave > dichroic_min)&(twave < dichroic_max)]
 
    return twave, tval

def read_data(fn, dichroic_min=5550, dichroic_max=10150):
    hdus = fits.open(fn)
    data = hdus[1].data

    tdata = {}

    for nm in data.columns.names:
        if 'wave' in nm:
            continue
        tdata[nm] = data[nm][(data['wave']> dichroic_min)&(data['wave'] < dichroic_max)]
    nm='wave_grid_mid'
    tdata[nm] = data[nm][(data['wave']> dichroic_min)&(data['wave'] < dichroic_max)]
    nm='wave'
    tdata[nm] = data[nm][(data['wave']> dichroic_min)&(data['wave'] < dichroic_max)]

    return tdata

def write_data(fn, tdata):
    cols = []
    for key in tdata:
        col = fits.Column(name=key, format='D', array=tdata[key])
        cols.append(col)
    hdu = fits.BinTableHDU.from_columns(fits.ColDefs(cols))
    hdu.writeto(fn, overwrite=True)

def scale_tell(twave, tval, obs_airmass, tell_airmass):
    wgt = (obs_airmass/tell_airmass)**0.55
    spl = make_interp_spline(twave, tval*wgt)
    return spl

def measure_offset(data_wave, data_flux, tell_wave, tell_flux, maxlag=10, plot=False):

    inds = numpy.arange(len(tell_wave))
    tell_use = (tell_wave > 6800) & (tell_wave < 7800)
    data_use = (data_wave > 6700) & (data_wave < 7900)
    start_ind = numpy.min(inds[tell_use])
    print(f'Start ind: {start_ind}')

    cc = numpy.correlate(data_flux[data_use], tell_flux[tell_use], mode='valid')
    print(numpy.argmax(cc))
    cc_size = (cc.size - 1) // 2
    lagscale = numpy.arange(-cc_size-1, cc_size+1, dtype=float)
    if plot:
        plt.plot(lagscale, cc)
        plt.show()
    ii = numpy.argmax(cc) + numpy.arange(-6, 7)
    offset = numpy.mean(lagscale[ii])
    p = numpy.polyfit(lagscale[ii] - offset, cc[ii], 2)
    offset = numpy.mean(lagscale[ii])
    print(p, offset)
    off = offset - 0.5 * p[1] / p[0]
    return off

def main():

    args = parse_args()
    tell_file = args.tell_file
    data_file = args.data_file
    dichroic_min = args.dichroic_min
    dichroic_max = args.dichroic_max

    twave, tval = get_tell(tell_file, dichroic_min=dichroic_min, dichroic_max=dichroic_max)
    tdata = read_data(data_file, dichroic_min=dichroic_min, dichroic_max=dichroic_max)

    if args.plot:
        plt.plot(twave, tval*numpy.mean(tdata['flux']), label='Telluric')
        plt.plot(tdata['wave'], tdata['flux'], alpha=0.3)
        plt.xlabel('Wavelength (A)')
        plt.ylabel('Flux (arbitrary units)')
        plt.title('Telluric Corrected Spectrum')
        plt.show()

    fin_off = measure_offset(tdata['wave'], tdata['flux'], twave, tval, maxlag=10, plot=args.plot)
    print(f'Offset: {fin_off} pixels')
    spl = scale_tell(twave, tval, obs_airmass=1.185, tell_airmass=1.33)

    tdata['tell_flux'] = tdata['flux']/spl(tdata['wave'])

    if args.plot:
        plt.plot(tdata['wave'], tdata['tell_flux'])
        plt.plot(tdata['wave'], tdata['flux'], alpha=0.3)
        plt.xlabel('Wavelength (A)')
        plt.ylabel('Flux (arbitrary units)')
        plt.title('Telluric Corrected Spectrum')
        plt.show()

if __name__ == "__main__":
    main()
