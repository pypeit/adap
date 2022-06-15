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
filenames = ps.fitstbl['filename'].data.tolist()

mjd_order = np.argsort(ps.fitstbl['mjd'])

# Criteria

# For arcs, strive to take at least 1 per lamp set

best_dec = np.abs(ps.fitstbl['dec']-45.) < 1.
best_arc_exp = (ps.fitstbl['exptime'] >= 1.) & (ps.fitstbl['exptime'] <= 15.) 
best_flat_exp = (ps.fitstbl['exptime'] > 5.) & (ps.fitstbl['exptime'] <= 30.) 

test_criterion = (ps.fitstbl['exptime'] > 50000.)


arc_type = 'arc,tilt'
flat_type = 'pixelflat,illumflat,trace'
min_arc_set = 1
min_flats = 3

# Cut down Arcs
arcs = ps.fitstbl['frametype'] == arc_type

arc_criteria = [best_arc_exp, best_dec]

# Lamps
unique_lamps = np.unique(ps.fitstbl['lampstat01'][arcs].data)
indiv_lamps = [item.split(' ') for item in unique_lamps]
nlamps = [len(item) for item in indiv_lamps]

lamp_order = np.argsort(nlamps)[::-1]

# Loop on the sets
all_keep_arcs = []
all_lamps = []
for ilamp in lamp_order:
    # Any new ones?
    new = False
    for lamp in indiv_lamps[ilamp]:
        if lamp not in all_lamps:
            new = True
    if not new:
        print(f"No new lamps in {indiv_lamps[ilamp]}")
        continue
    
    arc_set = arcs & (ps.fitstbl['lampstat01'].data == unique_lamps[ilamp])
    
    while True:
        criteria = np.stack([arc_set]+arc_criteria)
        gd_arcs = np.all(criteria, axis=0)
        # Have enough?
        if np.sum(gd_arcs) >= min_arc_set or len(arc_criteria) == 0:
            break
        # Remove a criterion
        arc_criteria.pop()

    # Take the latest in time
    keep_arcs = np.where(gd_arcs)[0]
    sort_mjd_arcs = np.argsort(ps.fitstbl['mjd'].data[keep_arcs])
    keep_arcs = keep_arcs[sort_mjd_arcs[-min_arc_set:]]

    all_keep_arcs += keep_arcs.tolist()

    # Record the lamps
    all_lamps += indiv_lamps[ilamp]
    all_lamps = np.unique(all_lamps).tolist()
    

# Keep only the last ones in time
all_arcs = np.where(arcs)[0]
sort_mjd_arcs = np.argsort(ps.fitstbl['mjd'].data[keep_arcs])
for idx in all_arcs:
    if idx not in all_keep_arcs:
        filenames[idx] = '#'+filenames[idx]

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
all_flats = np.where(flats)[0]
keep_flats = np.where(gd_flats)[0]
sort_mjd_flats = np.argsort(ps.fitstbl['mjd'].data[keep_flats])
keep_flats = keep_flats[sort_mjd_flats[-min_flats:]]
for idx in all_flats:
    if idx not in keep_flats:
        filenames[idx] = '#'+filenames[idx]


# Finish
ps.fitstbl.table['filename'] = filenames
ps.fitstbl.write_pypeit(target_dir)