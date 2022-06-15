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

# Keep
keep = np.ones(len(ps.fitstbl), dtype=bool)

# Criteria
best_dec = np.abs(ps.fitstbl['dec']-45.) < 1.
best_arc_exp = (ps.fitstbl['exptime'] > 1.) & (ps.fitstbl['exptime'] <= 5.) 
best_flat_exp = (ps.fitstbl['exptime'] > 5.) & (ps.fitstbl['exptime'] <= 20.) 

# Cut down Arcs
arcs = ps.fitstbl['frametype'] == 'arc,tilt'
min_arcs = 2

arc_criteria = [best_arc_exp, best_dec]

while True:
    criteria = np.stack([arcs]+arc_criteria)
    gd_arcs = np.all(criteria, axis=0)
    # Have enough?
    if np.sum(gd_arcs) > min_arcs or len(arc_criteria) == 0:
        break
    # Remove a criterion
    arc_criteria.pop()

# Keep the last ones
keep_arcs = np.where(gd_arcs)[0][-min_arcs:]
keep[arcs] = False
keep[keep_arcs] = True

# Flats
flats = ps.fitstbl['frametype'] == 'pixelflat,illumflat,trace'
min_flats = 3

flat_criteria = [best_flat_exp, best_dec]

while True:
    criteria = np.stack([flats]+flat_criteria)
    gd_flats = np.all(criteria, axis=0)
    # Have enough?
    if np.sum(gd_flats) > min_flats or len(flat_criteria) == 0:
        break
    # Remove a criterion
    flat_criteria.pop()

# Keep the last ones
keep_flats = np.where(gd_flats)[0][-min_flats:]
keep[flats] = False
keep[keep_flats] = True

# Finish
ps.fitstbl.table = ps.fitstbl.table[keep]
ps.fitstbl.write_pypeit(target_dir)