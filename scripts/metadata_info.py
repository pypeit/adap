import astropy.coordinates
from astropy.table import Table
from datetime import date
import numpy as np
from pypeit.spectrographs.keck_deimos import KeckDEIMOSSpectrograph
from pypeit.spectrographs.keck_hires import KECKHIRESSpectrograph
from pypeit.spectrographs.keck_esi import KeckESISpectrograph


common_observation_columns = ["targname", "koaimtyp", "ra",      "dec",     "date_obs"]
common_observation_dtypes  = ["<U15",     "<U6",      "float64", "float64", "<U10"]
common_program_columns = ["semid","proginst","progpi","progtitl"]
common_program_dtypes =  ["<U14", "<U12",    "<U18",  "<U255"]
instr_columns = {"DEIMOS":  ["instrume","airmass", "waveblue", "wavered", "slmsknam","filter", "gratenam", "filehand"],
                 "ESI":     ["instrume","airmass", "waveblue","wavered", "filter","apmsknam","slmsknam","obsmode", "prismnam", "binning", "filehand",],
                 "HIRES":   ["instrume","airmass", "waveblue","wavered", "detector", "deckname","fil1name","fil2name","guidfwhm","iodin","echangl","xdangl","binning","filehand",],
                 "LRIS":    ["instrume","airmass", "waveblue","wavered", "blufilt","dichname","graname","grisname","guidfwhm","redfilt","slitname","dichroic","numamps","binning","grangle","wavelen","taplines","filehand",],
                 "MOSFIRE": ["instrume","airmass", "waveblue","wavered", "maskname", "filter", "gratmode", "filehand"],
                 "NIRES":   ["instrume","airmass", "waveblue","wavered", "instr","filehand", ],
                 "NIRSPEC": ["instrume","airmass", "waveblue","wavered", "camera","dispers","filter","slitname","scifilt1","scifilt2","echlpos","disppos","filehand"]
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
                        "HIRES":  [[("qsolist_obj_name", "<U")], [("dispname","<U"),("decker","<U"), ("filter1", "<U"), ('echangle', "float64"), ('xdangle', "float64"), ("binning", "<U")]],
                        "LRIS":  [[("qsolist_obj_name", "<U")], [("dispname","<U"),("decker","<U"), ("filter1", "<U"), ('echangle', "float64"), ('xdangle', "float64"), ("binning", "<U")]],                       }
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

class ADAPSpectrographMixin:


    @classmethod
    def load_extended_spectrograph(cls,spec_name=None,instr_name=None,matching_files=None):
        if spec_name == "keck_deimos" or instr_name == "DEIMOS":
            return ADAP_DEIMOSExtendedSpectrograph()
        elif spec_name == "keck_hires" or instr_name == "HIRES":
            return ADAP_HIRESExtendedSpectrograph(matching_files)   
        elif spec_name == "keck_esi" or instr_name == "ESI":
            return ADAP_ESIExtendedSpectrograph()
        elif spec_name == "keck_lris_blue":
            return ADAP_LRISBExtendedSpectrograph()
        elif spec_name == "keck_lris_blue_orig":
            return ADAP_LRISBOrigExtendedSpectrograph()
        elif spec_name == "keck_lris_red":
            return ADAP_LRISRExtendedSpectrograph()
        elif spec_name == "keck_lris_red_orig":
            return ADAP_LRISROrigExtendedSpectrograph()
        elif spec_name == "keck_lris_red_mark4":
            return ADAP_LRISRMark4ExtendedSpectrograph()
        else:
            raise NotImplementedError(f"Not extended spectrograph defined for {spec_name}/{instr_name}")

    def koa_columns(self):
        return instr_columns[self.header_name]
    
    def config_key_koa_columns(self):
        return config_key_columns[self.header_name]
    
    def exclude_koa_types(self):
        return exclude_koa_types[self.header_name]
    
    def exclude_pypeit_types(self):
        return exclude_pypeit_types[self.header_name]
    
    def exclude_metadata(self, metadata):
        """Finds metadata that should be excluded from ADAP processing for this spectrograph 
        and returns why it is being excluded
        
        Args:
            metadata (PypeItMetaData): The metadata to search.
            
        Return:
            indices (Sequence): The indices within the metadata to exclude.
            reasons (Sequence[str]): The reasons each index should be excluded.
        """
        # By default exclude nothing        
        return [],[]
    
    def is_metadata_complete(self,metadata):
        """Returns whether a grouped set of files are sufficient to reduce with PypeIt.
         
        Args:
            metadata (PypeItMetaData): The metadata for the files.

        Return:
            bool : True if the goup is complete, False if incomplete.
            str : A summary line for logging.            
        """
        raise NotImplementedError("is_metadata_complete not implemented for " + self.__class__.__name__)
    
    def config_path_grouping(self):
        return config_path_grouping.get(self.header_name,None)

    def extra_group_keys(self):
        # Return extra keys needed for grouping that aren't in the configuration keys
        return []
    
    def add_extra_metadata(self, args, metadata):
        """Add any extra metadata columns needed by adap_reorg_setup.py for this instrument.
         
        Args:
            args (argparse.Namespace): The arguments to adap_reorg_setup.py
            metadata (PypeItMetaData): The metadata for the files.

        Return:
            None
        """
        pass

class ADAP_DEIMOSExtendedSpectrograph(ADAPSpectrographMixin, KeckDEIMOSSpectrograph):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       For DEIMOS minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """

    def is_metadata_complete(self, metadata):
        if len(metadata.table) == 0:
            # This can happen if all of the files in this directory were removed from the metadata
            # due to unknown types.
            return False

        num_science_frames = np.sum(metadata.find_frames('science'))
        num_flat_frames = np.sum(np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat')))
        num_arc_frames = np.sum(metadata.find_frames('arc'))
        return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1), f"sci {num_science_frames} flat {num_flat_frames} arc {num_arc_frames}"

class ADAP_HIRESExtendedSpectrograph(ADAPSpectrographMixin, KECKHIRESSpectrograph):


    def __init__(self, matching_files):
        if matching_files is not None:
            hires_files = matching_files['instrument'] == 'HIRES'
            self.file_to_object_map = {filename: object for filename, object in matching_files[hires_files]['koaid','obj_name']}
        else:
            self.file_to_object_map = None
        super().__init__()

    #def exclude_metadata(self, metadata):
    #    # Exclude anything with 0.0 echangle or xdangle, as
    #    # PypeItMetadata can't compare those
    #    exclude = np.logical_or(metadata['echangle'] == 0.0, metadata['xdangle'] == 0.0)
    #    indices = np.where(exclude)[0]
    #    reasons = ["echangle and/or xdangle == 0.0"] * len(indices)
    #    return indices, reasons

    def is_metadata_complete(self, metadata):
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

    def add_extra_metadata(self, metadata):
        if self.file_to_object_map is not None:
            new_data = [self.file_to_object_map.get(filename, '') for filename in metadata['filename']]
        else:
            new_data = metadata['target'].copy()
        metadata.table.add_column(new_data,name='qsolist_obj_name')


    def extra_group_keys(self):
        # Return extra keys needed for grouping that aren't in the configuration keys
        return ['decker', 'qsolist_obj_name']
    
        
    #def same_configuration(self, configs, check_keys=True):
    #    extra_keys = ['decker', 'target']

    #    if isinstance(configs, dict):
    #        config_list = list(configs.values())
    #    else:
    #        config_list = configs

    #    # We need the frametype added by modify_config, if that wasn't called, fallback to superclass
    #    if any([True if cfg is None or 'frametype' not in cfg else False for cfg in config_list]):
    #        return super().same_configuration(configs, check_keys)

    #    # Only compare decker and target if comparing science file to science file
    #    if all([True if 'science' in cfg['frametype'] else False for cfg in config_list]):
    #        keys_to_compare = self.configuration_keys() + extra_keys
    #    else:
    #        keys_to_compare = self.configuration_keys()

    #    # Remove frametype and any unneeded key from the given configs
    #    dicts_to_compare = [{key: cfg[key] for key in keys_to_compare} for cfg in config_list]
    #    return super().same_configuration(dicts_to_compare, check_keys=False)
    
    #def modify_config(self, row, cfg):
    #    """
    #    Modify the configuration dictionary for a given frame. This method is
    #    used in :func:`~pypeit.metadata.PypeItMetaData.set_configurations` to
    #    modify in place the configuration requirement to assign a specific frame
    #    to the current setup.

    #    **This method is not defined for all spectrographs.**

    #    Args:
    #        row (`astropy.table.Row`_):
    #            The table row with the metadata for one frame.
    #        cfg (:obj:`dict`):
    #            Dictionary with metadata associated to a specific configuration.

    #    Returns:
    #        :obj:`dict`: modified dictionary with metadata associated to a
    #        specific configuration.
    #    """
    #    cfg['frametype'] = row['frametype']
    #    cfg['decker'] = row['decker']
    #    cfg['target'] = row['target']
    #    return cfg

#def config_independent_frames(self):
    #    # Calibrations (other than bias/dark as defined in spectrograph.py) should be grouped only on the original config keys
    #    orig_config_keys = super().configuration_keys()
    #    return {
    #            'pixelflat': orig_config_keys,
    #            'trace': orig_config_keys,
    #            'tilt': orig_config_keys,
    #            'arc': orig_config_keys,
    #           }
        #return super().config_independent_frames()

class ADAP_ESIExtendedSpectrograph(ADAPSpectrographMixin, KeckESISpectrograph):

    def is_metadata_complete(self, metadata):
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

def group_lris_metadata_by_spec_name():
    pass

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


