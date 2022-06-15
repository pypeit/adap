import os
import glob
import numpy as np

from pypeit import pypeitsetup

from IPython import embed
from torch import _make_per_channel_quantized_tensor

# Test case

lcl_path = '/scratch/REDUX/Keck/DEIMOS/adap_organizing'

file_list = glob.glob(os.path.join(lcl_path, 'raw', 'DE*.fits'))
ps = pypeitsetup.PypeItSetup(file_list=file_list, 
                             spectrograph_name = 'keck_deimos')



# Reduce dir
target_dir = os.path.join(lcl_path, 'reduce')
os.makedirs(target_dir, exist_ok=True)

# Run setup
ps.run(sort_dir=target_dir, setup_only=True)

mjd_order = np.argsort(ps.fitstbl['mjd'])

# Keep
keep = np.ones(len(ps.fitstbl), dtype=bool)

# Criteria
best_dec = np.abs(ps.fitstbl['dec']-45.) < 1.
best_arc_exp = (ps.fitstbl['exptime'] > 1.) & (ps.fitstbl['exptime'] <= 5.) 
best_flat_exp = (ps.fitstbl['exptime'] > 5.) & (ps.fitstbl['exptime'] <= 20.) 
test_criterion = (ps.fitstbl['exptime'] > 50000.)

arc_type = 'arc,tilt'
flat_type = 'pixelflat,illumflat,trace'
min_arcs = 2
min_flats = 3

# Cut down Arcs
arcs = ps.fitstbl['frametype'] == arc_type

arc_criteria = [best_arc_exp, best_dec, test_criterion]

while True:
    criteria = np.stack([arcs]+arc_criteria)
    gd_arcs = np.all(criteria, axis=0)
    # Have enough?
    if np.sum(gd_arcs) > min_arcs or len(arc_criteria) == 0:
        break
    # Remove a criterion
    print("Removing an arc criterion")
    arc_criteria.pop()

# Keep only the last ones in time
keep[arcs] = False

keep_arcs = np.where(gd_arcs)[0]
sort_mjd_arcs = np.argsort(ps.fitstbl['mjd'].data[keep_arcs])
keep[keep_arcs[sort_mjd_arcs[-min_arcs:]]] = True

# Flats
flats = ps.fitstbl['frametype'] == flat_type

flat_criteria = [best_flat_exp, best_dec]

while True:
    criteria = np.stack([flats]+flat_criteria)
    gd_flats = np.all(criteria, axis=0)
    # Have enough?
    if np.sum(gd_flats) > min_flats or len(flat_criteria) == 0:
        break
    # Remove a criterion
    print("Removing an flat criterion")
    flat_criteria.pop()

# Keep the last ones
keep[flats] = False

keep_flats = np.where(gd_flats)[0]
sort_mjd_flats = np.argsort(ps.fitstbl['mjd'].data[keep_flats])
keep[keep_flats[sort_mjd_flats[-min_flats:]]] = True

# Finish
ps.fitstbl.table = ps.fitstbl.table[keep]
ps.fitstbl.write_pypeit(target_dir)