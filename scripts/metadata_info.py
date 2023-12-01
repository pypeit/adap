import astropy.coordinates
from datetime import date
import numpy as np

common_observation_columns = ["targname", "koaimtyp", "ra",      "dec",     "date_obs"]
common_observation_dtypes  = ["<U15",     "<U6",      "float64", "float64", "<U10"]
common_program_columns = ["semid","proginst","progpi","progtitl"]
common_program_dtypes =  ["<U14", "<U12",    "<U18",  "<U255"]
instr_columns = {"DEIMOS":  ["airmass", "waveblue", "wavered", "slmsknam","filter", "gratenam", "filehand"],
                 "ESI":     ["airmass", "waveblue","wavered", "filter","apmsknam","slmsknam","obsmode", "prismnam", "binning", "filehand",],
                 "HIRES":   ["airmass", "waveblue","wavered", "detector", "deckname","fil1name","fil2name","guidfwhm","iodin","echangl","xdangl","binning","filehand",],
                 "LRIS":    ["airmass", "waveblue","wavered", "blufilt","dichname","graname","grisname","guidfwhm","instrume","redfilt","slitname","dichroic","numamps","binning","grangle","wavelen","taplines","filehand",],
                 "MOSFIRE": ["airmass", "waveblue","wavered", "maskname", "filter", "gratmode", "filehand"],
                 "NIRES":   ["airmass", "waveblue","wavered", "instr","filehand", ],
                 "NIRSPEC": ["airmass", "waveblue","wavered", "camera","dispers","filter","slitname","scifilt1","scifilt2","echlpos","disppos","filehand"]
                }
match_columns = ["instrument","koaid","obj_id","obj_name", "separation"]
match_dtypes = [ "<U10",      "<U25", int,     "<U41",     float]

instrument_names = list(instr_columns.keys())
config_key_columns = {"DEIMOS":  ["slmsknam", "gratenam", "filter", "waveblue", "wavered"],
                      "ESI":     ["slmsknam","prismnam","binning"],
                      "HIRES":   ["fil1name","echangl","xdangl","binning"],
                      "LRIS":    ["graname","dichroic","slitname","numamps","binning","grangle","wavelen","taplines", "grisname"],
                      "MOSFIRE": ["maskname", "filter", "gratmode"],
                      "NIRES":   ["instr"],
                      "NIRSPEC": ["camera","dispers","filter","slitname","scifilt1","scifilt2","echlpos","disppos"],
                      }

config_path_grouping = {"DEIMOS": [[("decker","<U")], [("dispname","<U"), ("dispangle", "float64"), ("filter", "<U")]],
                        "ESI":    [[("decker", "<U")], [("dispname", "<U"), ("binning", "<U")]],
                        "HIRES":  [[("filter1", "<U")], [('echangle', "float64"), ('xdangle', "float64"), ("binning", "<U")]],
                       }
exclude_pypeit_types = {"DEIMOS": ["bias"],
                        "ESI":     ["standard"],
                        "HIRES":   [],
                        "LRIS":    [],
                        "MOSFIRE": [],
                        "NIRES":   [],
                        "NIRSPEC": [],
                        }
exclude_koa_types = {"DEIMOS":  ["fscal", "bias", "dark", "focus"],
                     "ESI":     ["fscal", "focus"],
                     "HIRES":   ["fscal", "focus"],
                     "LRIS":    ["fscal", "focus","polcal"],
                     "MOSFIRE": ["fscal", "focus"],
                     "NIRES":   ["fscal", "focus", "bias", "dark"],
                     "NIRSPEC": ["fscal", "focus"],
                    }

def exclude_hires_metadata(metadata):
    # Exclude anything with 0.0 echangle or xdangle, as
    # PypeItMetadata can't compare those
    exclude = np.logical_or(metadata['echangle'] == 0.0, metadata['xdangle'] == 0.0)
    indices = np.where(exclude)[0]
    reasons = ["echangle and/or xdangle == 0.0"] * len(indices)
    return indices, reasons

exclude_metadata_funcs = {"HIRES": exclude_hires_metadata}


def get_lris_spec_name(koaid, obs_date):
    obs_date = date.fromisoformat(obs_date[0:10])
    if koaid.startswith("LB"):
        if obs_date <= date(2009,4,30):
            return "keck_lris_blue_orig"
        else:
            return "keck_lris_blue"
    else:
        if obs_date >= date(2021,4,22):
            return "keck_lris_red_mark4"
        elif obs_date <= date(2009,5,2):
            return "keck_lris_red_orig"
        else:
            return "keck_lris_red"

def is_deimos_metadata_complete(metadata):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       For DEIMOS minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """
    if len(metadata.table) == 0:
        # This can happen if all of the files in this directory were removed from the metadata
        # due to unknown types.
        return False

    num_science_frames = np.sum(metadata.find_frames('science'))
    num_flat_frames = np.sum(np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat')))
    num_arc_frames = np.sum(metadata.find_frames('arc'))
    return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1), f"sci {num_science_frames} flat {num_flat_frames} arc {num_arc_frames}"

def is_esi_metadata_complete(metadata):
    """Determine if a PypeItMetadata object has enough data to be reduced.
    For ESI:
        3+ dome flats (not internal)
        1 good CuAr (300s+) and 1 good non-CuAr arc (typically ~20s)
        5+ bias
    """
    num_science_frames = np.sum(metadata.find_frames('science'))
    flat_frames = np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat'))
    dome_flat_frames = np.logical_and(flat_frames, metadata['idname'] == 'DmFlat')
    num_dome_flat_frames = np.sum(dome_flat_frames)
    
    arc_frames = metadata.find_frames('arc')
    cu_frames = metadata[arc_frames]['lampstat02'] == "on"
    num_good_cu_frames = np.sum(np.logical_and(cu_frames,metadata[arc_frames]['exptime'] >= 300.0))
    xe_or_hgne_lamps = np.logical_or(metadata[arc_frames]['lampstat01'] == "on",metadata[arc_frames]['lampstat03']=="on")
    num_non_cu_frames = np.sum(np.logical_and(metadata[arc_frames][xe_or_hgne_lamps]['exptime']>=10,metadata[arc_frames][xe_or_hgne_lamps]['exptime']<=30))
    
    num_bias_frames = np.sum(metadata.find_frames('bias') )
    return ((num_science_frames >=1 and num_dome_flat_frames >= 3 and num_good_cu_frames >= 1 and num_non_cu_frames >=1 and num_bias_frames >=5),
            f"sci {num_science_frames} domeflats {num_dome_flat_frames} Good CuAr {num_good_cu_frames} Good non-CuAR {num_non_cu_frames} bias {num_bias_frames}")

def is_hires_metadata_complete(metadata):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       For HIRES minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """
    if len(metadata.table) == 0:
        # This can happen if all of the files in this directory were removed from the metadata
        # due to unknown types.
        return False

    num_science_frames = np.sum(metadata.find_frames('science'))
    num_flat_frames = np.sum(metadata.find_frames('pixelflat'))
    num_arc_frames = np.sum(metadata.find_frames('arc'))
    return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1), f"sci {num_science_frames} flat {num_flat_frames} arc {num_arc_frames}"


def is_metadata_complete(metadata, instrument):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       The minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """
    if len(metadata.table) == 0:
        # This can happen if all of the files in this directory were removed from the metadata
        # due to unknown types.
        return False

    if instrument == "DEIMOS":
        return is_deimos_metadata_complete(metadata)
    elif instrument == "ESI":
        return is_esi_metadata_complete(metadata)
    elif instrument == "HIRES":
        return is_hires_metadata_complete(metadata)
    else:
        raise ValueError(f"No metadata complete function defined for instrument {instrument}")


