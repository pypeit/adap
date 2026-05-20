import argparse

import astropy.io.fits as fits
import matplotlib.pyplot as plt
from scipy.interpolate import BSpline, make_interp_spline
import numpy
import pypeit.inputfiles


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
        default=10000,
        help="Maximum wavelength to consider (default: 10000 A).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Plot intermediate results.",
    )
    parser.add_argument(
        "--pypeit_file",
        type=str,
        default="../keck_lris_red_A/keck_lris_red_A.pypeit",
    )
    return parser.parse_args()

def get_tell(fn, data_wave, dichroic_min=5550, dichroic_max=10000):

    hdus=fits.open(fn)
    tdata = hdus[1].data
    twave = tdata['wave'][0]
    tval = tdata['telluric'][0]

    tval = tval[(twave > dichroic_min)&(twave < dichroic_max)]
    twave = twave[(twave > dichroic_min)&(twave < dichroic_max)]
 
    spl= make_interp_spline(twave, tval)

    return spl(data_wave)

def read_data(fn, dichroic_min=5550, dichroic_max=10000):
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

def scale_tell(obs_airmass, tell_airmass):
    wgt = (obs_airmass/tell_airmass)**0.55

    return wgt

def find_off(lag_scale, cc):
    ii = numpy.argmax(cc) + numpy.arange(-6, 7)
    p = numpy.polyfit(lag_scale[ii], cc[ii], 2)
    off = numpy.mean(lag_scale[ii]) -0.5 * p[1] / p[0]

    return off

def measure_offset(data_wave, data_flux, tell_flux, plot=False):

    inds = numpy.arange(len(tell_flux))
    data_use = (data_wave > 6855) & (data_wave < 7050)
    data_use = data_use | ((data_wave > 7145) & (data_wave < 7400))
    data_use = data_use | ((data_wave > 7570) & (data_wave < 7700))
    data_use = data_use | ((data_wave > 8089) & (data_wave < 8375))
    data_use = data_use | ((data_wave > 8905) & (data_wave < 9895))

    data_norm = numpy.mean(data_flux[data_use])

    start_ind = numpy.min(inds[data_use])
    if plot:
        plt.plot(data_wave[data_use], tell_flux[data_use], label='Telluric')
        plt.plot(data_wave[data_use], data_flux[data_use]/data_norm, label='Data')
        plt.xlabel(r'Wavelength ($\AA$)')
        plt.ylabel('Flux')
        plt.title('Telluric and Data Spectra in Telluric Region')
        plt.show()

    cc = numpy.correlate(data_flux[data_use]/data_norm, tell_flux[data_use], mode='full')
    cc_size = (cc.size - 1) // 2
    if cc.size%2 == 1:
        lagscale = numpy.arange(-cc_size, cc_size+1, dtype=float)
    else:
        lagscale = numpy.arange(-cc_size-1, cc_size+1, dtype=float)
    if plot:
        plt.plot(lagscale, cc)
        plt.xlabel('Lag (pixels)')
        plt.ylabel('Cross-correlation')
        plt.title('Cross-correlation between Data and Telluric Spectra')
        plt.show()


    off = find_off(lagscale, cc)

    mid = len(tell_flux) // 2
    data_w = data_wave[mid]
    data_w_spl = make_interp_spline(inds, data_wave)
    tell_w = data_w_spl(mid + off)

    if plot:
        print(f'Offset: {off} pixels, which corresponds to {data_w-tell_w:.2f} A')

    return off

def read_pypeit(pypeit_file, tell_file):
    pfile  = pypeit.inputfiles.PypeItFile.from_file(pypeit_file)
    pypeit_table = pfile.data

    sciframes = pypeit_table['frametype'] =='science'
    stdframes = pypeit_table['frametype'] =='standard'

    tell_airmass = 1
    for i,n in enumerate(pypeit_table['filename'][stdframes]):
        if n in tell_file:
            tell_airmass = pypeit_table['airmass'][stdframes][i]
            break
    obs_airmass = numpy.mean(pypeit_table['airmass'][sciframes])

    return obs_airmass, tell_airmass

def write_data(fn, tdata):
    fn = fn.replace('.fits', '_tell.fits')
    cols = []
    for key in tdata:
        col = fits.Column(name=key, format='D', array=tdata[key])
        cols.append(col)
    hdu = fits.BinTableHDU.from_columns(fits.ColDefs(cols))
    hdu.writeto(fn, overwrite=True)

def main():

    args = parse_args()
    tell_file = args.tell_file
    data_file = args.data_file
    dichroic_min = args.dichroic_min
    dichroic_max = args.dichroic_max

    tdata = read_data(data_file, dichroic_min=dichroic_min, dichroic_max=dichroic_max)
    tval = get_tell(tell_file, tdata['wave'], dichroic_min=dichroic_min, dichroic_max=dichroic_max)

    obs_airmass, tell_airmass = read_pypeit(args.pypeit_file, tell_file)

    if args.plot:
        plt.plot(tdata['wave'], tval*numpy.mean(tdata['flux']), label='Telluric')
        plt.plot(tdata['wave'], tdata['flux'], label='Data',alpha=0.3)
        plt.xlabel('Wavelength (A)')
        plt.ylabel('Flux (arbitrary units)')
        plt.title('Telluric and Data Spectra')
        plt.legend()
        plt.show()

    fin_off = measure_offset(tdata['wave'], tdata['flux'], tval, plot=args.plot)
    tinds = numpy.arange(0,len(tval))
    wgt = scale_tell(obs_airmass=obs_airmass, tell_airmass=tell_airmass)
    spl = make_interp_spline(tinds, tval*wgt)
    tinds = tinds - fin_off
    fin_tval = spl(tinds)

    if args.plot:
        plt.plot(tdata['wave'], fin_tval*numpy.mean(tdata['flux']), label='Scaled and Shifted Telluric')
        plt.plot(tdata['wave'], tdata['flux'], label='Data',alpha=0.3)
        plt.xlabel('Wavelength (A)')
        plt.ylabel('Flux (arbitrary units)')
        plt.title('Shifted Telluric vs Data Spectra')
        plt.legend()
        plt.show()    

    tdata['flux'] = tdata['flux'] / fin_tval

    if args.plot:
        plt.plot(tdata['wave'], tdata['flux'], label='Telluric-corrected Data')
        plt.xlabel('Wavelength (A)')
        plt.ylabel('Flux (arbitrary units)')
        plt.title('Telluric-corrected Data Spectrum')
        plt.legend()
        plt.show()

    write_data(args.data_file, tdata)

if __name__ == "__main__":
    main()
