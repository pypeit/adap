
common_observation_columns = ["targname", "koaimtyp", "ra",      "dec",     "date_obs", "ut"]
common_observation_dtypes  = ["<U15",     "<U6",      "float64", "float64", "<U10",     "<U15"]
common_program_columns = ["semid","proginst","progpi","progtitl"]
common_program_dtypes =  ["<U14", "<U12",    "<U18",  "<U255"]
instr_columns = {"DEIMOS":  ["instrume","airmass", "waveblue", "wavered", "slmsknam","filter", "gratenam", "filehand"],
                 "ESI":     ["instrume","airmass", "waveblue","wavered", "filter","apmsknam","slmsknam","obsmode", "prismnam", "binning", "filehand",],
                 "HIRES":   ["instrume","airmass", "waveblue","wavered", "detector", "deckname","xdispers","fil1name","fil2name","guidfwhm","iodin","echangl","xdangl","binning","filehand",],
                 "LRIS":    ["instrume","airmass", "waveblue","wavered", "blufilt","dichname","graname","grisname","guidfwhm","redfilt","slitname","dichroic","numamps","binning","grangle","wavelen","taplines","filehand",],
                 "MOSFIRE": ["instrume","airmass", "waveblue","wavered", "maskname", "filter", "gratmode", "filehand"],
                 "NIRES":   ["instrume","airmass", "waveblue","wavered", "instr","filehand", ],
                 "NIRSPEC": ["instrume","airmass", "waveblue","wavered", "camera","dispers","filter","slitname","scifilt1","scifilt2","echlpos","disppos","filehand"]
                }
KOA_ID_DTYPE = '<U28' # includes possible file extension e.g. 'II.YYYYMMDD.NNNNN.NN.fits.gz'
match_columns = ["instrument","koaid","obj_id","obj_name", "rs_best", "separation"]
match_dtypes = [ "<U10",      KOA_ID_DTYPE, int,     "<U41",     float,     float]

instrument_names = list(instr_columns.keys())
config_key_columns = {"DEIMOS":  ["slmsknam", "gratenam", "filter", "waveblue", "wavered"],
                      "ESI":     ["slmsknam","prismnam","binning"],
                      "HIRES":   ["deckname", "xdispers", "fil1name","binning", "echangl","xdangl",],
                      "LRIS":    ["graname","dichroic","slitname","numamps","binning","grangle","wavelen","taplines", "grisname"],
                      "MOSFIRE": ["maskname", "filter", "gratmode"],
                      "NIRES":   ["instr"],
                      "NIRSPEC": ["camera","dispers","filter","slitname","scifilt1","scifilt2","echlpos","disppos"],
                      }

config_path_grouping = {"DEIMOS": [[("decker","<U")], [("dispname","<U"), ("dispangle", "float64"), ("filter", "<U")]],
                        "ESI":    [[("decker", "<U")], [("dispname", "<U"), ("binning", "<U")]],
                        "HIRES":  [[("qsolist_obj_name", "<U")], [("dispname","<U"),("decker","<U"), ("filter1", "<U"), ('echangle', "float64"), ('xdangle', "float64"), ("binning", "<U")]],
                       }
exclude_pypeit_types = {"DEIMOS": ["bias"],
                        "ESI":     ["standard"],
                        "HIRES":   ["bias", "dark"],
                        "LRIS":    [],
                        "MOSFIRE": [],
                        "NIRES":   [],
                        "NIRSPEC": [],
                        }
exclude_koa_types = {"DEIMOS":  ["fscal", "bias", "dark", "focus"],
                     "ESI":     ["fscal", "focus"],
                     "HIRES":   ["fscal", "focus", "dark"],
                     "LRIS":    ["fscal", "focus","polcal"],
                     "MOSFIRE": ["fscal", "focus"],
                     "NIRES":   ["fscal", "focus", "bias", "dark"],
                     "NIRSPEC": ["fscal", "focus"],
                    }


spec_to_instrument = {"keck_deimos": "DEIMOS",
                      "keck_esi": "ESI",
                      "keck_hires": "HIRES",
                      "keck_lris_blue": "LRIS",
                      "keck_lris_blue_orig": "LRIS",
                      "keck_lris_red": "LRIS",
                      "keck_lris_red_orig": "LRIS",
                      "keck_lris_red_mark4": "LRIS",
                      "keck_mosire": "MOSFIRE",
                      "keck_nires": "NIRES",
                      "keck_nirspec": "NIRSPEC",
                     }
