from datetime import date
from astropy import time
import numpy as np
from pypeit.spectrographs.keck_deimos import KeckDEIMOSSpectrograph
from pypeit.spectrographs.keck_hires import KECKHIRESSpectrograph
from pypeit.spectrographs.keck_esi import KeckESISpectrograph
from pypeit.spectrographs.keck_lris import KeckLRISSpectrograph
from pypeit.spectrographs.keck_lris import KeckLRISBOrigSpectrograph
from pypeit.spectrographs.keck_lris import KeckLRISBSpectrograph
from pypeit.spectrographs.keck_lris import KeckLRISRSpectrograph
from pypeit.spectrographs.keck_lris import KeckLRISRMark4Spectrograph
from pypeit.spectrographs.keck_lris import KeckLRISROrigSpectrograph
from pypeit.io import fits_open

from metadata_info import config_key_columns, config_path_grouping, exclude_pypeit_types, instr_columns, exclude_koa_types

class ADAPSpectrographMixin:

    def __init__(self, instr_name=None, matching_files=None):
        # The isntrument name is usually the header name
        self.instrument_name = self.header_name if instr_name is None else instr_name

        if matching_files is not None:
            instr_files = matching_files['instrument'] == self.instrument_name
            self.file_to_object_map = {filename: object for filename, object in matching_files[instr_files]['koaid','obj_name']}
        else:
            self.file_to_object_map = None
        super().__init__()
        

    @classmethod
    def load_extended_spectrograph(cls,spec_name=None,instr_name=None,matching_files=None):
        if spec_name == "keck_deimos" or instr_name == "DEIMOS":
            return ADAP_DEIMOSExtendedSpectrograph()
        elif spec_name == "keck_hires" or instr_name == "HIRES":
            return ADAP_HIRESExtendedSpectrograph(matching_files)   
        elif spec_name == "keck_esi" or instr_name == "ESI":
            return ADAP_ESIExtendedSpectrograph()
        elif spec_name == "keck_lris" or instr_name == "LRIS":
            return ADAP_LRISExtendedSpectrograph(matching_files)
        elif spec_name == "keck_lris_blue":
            return ADAP_LRISBlueExtendedSpectrograph(matching_files)
        elif spec_name == "keck_lris_blue_orig":
            return ADAP_LRISBlueOrigExtendedSpectrograph(matching_files)
        elif spec_name == "keck_lris_red":
            return ADAP_LRISRedExtendedSpectrograph(matching_files)
        elif spec_name == "keck_lris_red_orig":
            return ADAP_LRISRedOrigExtendedSpectrograph(matching_files)
        elif spec_name == "keck_lris_red_mark4":
            return ADAP_LRISRedMark4ExtendedSpectrograph(matching_files)
        else:
            raise NotImplementedError(f"Not extended spectrograph defined for {spec_name}/{instr_name}")

    def is_multi_class_instrument(self):
        # Used for instruments with multiple PypeIt spectrograph classes, like LRIS
        return False

    def group_multi_class_files(self, files):
        raise NotImplemented

    def koa_columns(self):
        return instr_columns[self.instrument_name]
    
    def config_key_koa_columns(self):
        return config_key_columns[self.instrument_name]
    
    def exclude_koa_types(self):
        return exclude_koa_types[self.instrument_name]
    
    def exclude_pypeit_types(self):
        return exclude_pypeit_types[self.instrument_name]
    
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
    
    def is_metadata_complete(self,metadata, standard_as_science=False):
        """Returns whether a grouped set of files are sufficient to reduce with PypeIt.
         
        Args:
            metadata (PypeItMetaData): The metadata for the files.
            standard_as_science (bool): True if standards should count as science.

        Return:
            bool : True if the goup is complete, False if incomplete.
            str : A summary line for logging.            
        """
        raise NotImplementedError("is_metadata_complete not implemented for " + self.__class__.__name__)
    
    def config_path_grouping(self):
        return config_path_grouping.get(self.instrument_name,None)

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

        def koa_config_compare(self, config1, config2):
            """Compare the PypeIt configuration between two items using the koa attribute names.
            The items can be rows returned from koa, or a dict with the correct keys.
            Uses same comparison logic as spectrograph.same_configuration.

            Args:
                config1: The first row/dict for comparison
                config2: The second row/dict for comparison

            Return:
                True if they are the same, False if not.
            """
            pass

class ADAP_DEIMOSExtendedSpectrograph(ADAPSpectrographMixin, KeckDEIMOSSpectrograph):
    """Determine if a PypeItMetadata object has enough data to be reduced.
       For DEIMOS minimum requirements for this are a science frame, a flat frame, and
       an arc frame.
    """

    def is_metadata_complete(self, metadata,standard_as_science=False):
        if len(metadata.table) == 0:
            # This can happen if all of the files in this directory were removed from the metadata
            # due to unknown types.
            return False

        num_science_frames = np.sum(metadata.find_frames('science'))
        if standard_as_science:
            num_science_frames += np.sum(metadata.find_frames('standard'))

        num_flat_frames = np.sum(np.logical_or(metadata.find_frames('pixelflat'), metadata.find_frames('illumflat')))
        num_arc_frames = np.sum(metadata.find_frames('arc'))
        return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1), f"sci {num_science_frames} flat {num_flat_frames} arc {num_arc_frames}"

class ADAP_HIRESExtendedSpectrograph(ADAPSpectrographMixin, KECKHIRESSpectrograph):


    def __init__(self, matching_files):
        super().__init__(matching_files=matching_files)

    #def exclude_metadata(self, metadata):
    #    # Exclude anything with 0.0 echangle or xdangle, as
    #    # PypeItMetadata can't compare those
    #    exclude = np.logical_or(metadata['echangle'] == 0.0, metadata['xdangle'] == 0.0)
    #    indices = np.where(exclude)[0]
    #    reasons = ["echangle and/or xdangle == 0.0"] * len(indices)
    #    return indices, reasons

    def is_metadata_complete(self, metadata,standard_as_science=False):
        """Determine if a PypeItMetadata object has enough data to be reduced.
        For HIRES minimum requirements for this are a science frame, a flat frame, and
        an arc frame.
        """
        if len(metadata.table) == 0:
            # This can happen if all of the files in this directory were removed from the metadata
            # due to unknown types.
            return False

        num_science_frames = np.sum(metadata.find_frames('science'))
        if standard_as_science:
            num_science_frames += np.sum(metadata.find_frames('standard'))
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
    
    def koa_config_compare(self, config1, config2):
        """Compare the PypeIt configuration between two items using the koa attribute names.
        The items can be rows returned from koa, or a dict with the correct keys.
        Uses same comparison logic as spectrograph.same_configuration.

        Args:
            config1: The first row/dict for comparison
            config2: The second row/dict for comparison

        Return:
            True if they are the same, False if not.
        """
        koa_key_to_pypeit_key = {'deckname': 'decker',
                                 'xdispers': 'dispname',
                                 'filter1': 'fil1name',
                                 'echangl': 'echangle',
                                 'xdangl': 'xdangle',
                                 'binning': 'binning'}
        
        for key in config_key_columns['HIRES']:
            if isinstance(config1[key], (float, np.floating)):
                pypeit_key = koa_key_to_pypeit_key[key]
                if not np.isclose(config1[key], config2[key], 
                                  rtol=self.meta[pypeit_key].get('rtol', 0.0),
                                  atol=self.meta[pypeit_key].get('atol', 0.0),
                                  equal_nan=True):
                    return False
            else:
                if config1[key] != config2[key]:
                    return False
        return True

class ADAP_ESIExtendedSpectrograph(ADAPSpectrographMixin, KeckESISpectrograph):

    def is_metadata_complete(self, metadata,standard_as_science=False):
        """Determine if a PypeItMetadata object has enough data to be reduced.
        For ESI:
            3+ dome flats (not internal)
            1 good CuAr (300s+) and 1 good non-CuAr arc (typically ~20s)
            5+ bias
        """
        num_science_frames = np.sum(metadata.find_frames('science'))
        if standard_as_science:
            num_science_frames += np.sum(metadata.find_frames('standard'))
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

def get_lris_spec_name(obs_date, koaid=None, instrument=None):
    if koaid is None and instrument is None:
        raise ValueError("Need koaid or instrument to identify Keck LRIS spectrograph subclass.")
    if instrument is not None:
        if instrument == "LRISBLUE":
            is_blue=True
        else:
            is_blue=False
    elif koaid is not None:
        if koaid.startswith("LB"):
            is_blue = True
        else:
            is_blue = False
    
    if is_blue:
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


class ADAP_LRISExtendedSpectrograph(ADAPSpectrographMixin):

    blue_grouping = [[("qsolist_obj_name", "<U")], [("spec_name", "<U"), ("dispname","<U"),("decker","<U"), ("dichroic", "<U"), ("amp", "int"), ("binning", "<U")]]
    red_grouping = [[("qsolist_obj_name", "<U")], [("spec_name", "<U"), ("dispname","<U"),("decker","<U"), ("dichroic", "<U"), ("dispangle","float64"),("cenwave","float64"),("amp", "int"), ("binning", "<U")]]
    def __init__(self, matching_files):
        super().__init__(instr_name = 'LRIS', matching_files = matching_files)
        
    def is_metadata_complete(self, metadata,standard_as_science=False):
        """Determine if a PypeItMetadata object has enough data to be reduced.
        For LRIS minimum requirements for this are a science frame, a flat frame, and
        an arc frame.
        """
        if len(metadata.table) == 0:
            # This can happen if all of the files in this directory were removed from the metadata
            # due to unknown types.
            return False

        num_science_frames = np.sum(metadata.find_frames('science'))
        if standard_as_science:
            num_science_frames += np.sum(metadata.find_frames('standard'))
        num_flat_frames = np.sum(metadata.find_frames('pixelflat'))
        num_arc_frames = np.sum(metadata.find_frames('arc'))
        return (num_science_frames >= 1 and num_flat_frames >= 1 and num_arc_frames >= 1), f"sci {num_science_frames} flat {num_flat_frames} arc {num_arc_frames}"

    def add_extra_metadata(self, metadata):
        if self.file_to_object_map is not None:
            new_data = [self.file_to_object_map.get(filename, '') for filename in metadata['filename']]
        else:
            new_data = metadata['target'].copy()
        metadata.table.add_column(new_data,name='qsolist_obj_name')
        # Spectrograph class name will be set by parent class
        metadata.table.add_column([self.name] * len(metadata),name='spec_name')


    def extra_group_keys(self):
        # Return extra keys needed for grouping that aren't in the configuration keys
        return ['qsolist_obj_name', 'spec_name']

    
    def config_independent_frames(self):
        # We don't include 'dateobs' like the parent class because
        # adap_reorg_setup uses a date window to group files
        return {'bias': ['amp', 'binning'], 'dark': ['amp', 'binning']}

    def is_multi_class_instrument(self):
        # Used for instruments with multiple PypeIt spectrograph classes, like LRIS
        return True

    def group_multi_class_files(self, files):
        groups = dict()
        for file in files:
            with fits_open(file) as hdul:
                instrument = hdul[0].header['INSTRUME']
                # Use a blue LRIS spectrograph to get the mjd
                test_spec = KeckLRISBSpectrograph()
                mjd = test_spec.get_meta_value(hdul, "mjd",ignore_bad_header=True)
                if mjd is None:
                    # It might be under 'MJD' for keck_lris_red_mark4
                    mjd = hdul[0].header.get['MJD']
                    if mjd is None:
                        raise ValueError(f"Cannot get mjd for {file}")
                obs_date = time.Time(mjd,format="mjd").to_datetime().date()
                spec_name = get_lris_spec_name(obs_date,instrument=instrument)
                if spec_name not in groups:
                    groups[spec_name] = []

                groups[spec_name].append(file)
        return groups


class ADAP_LRISBlueOrigExtendedSpectrograph(ADAP_LRISExtendedSpectrograph,KeckLRISBOrigSpectrograph):
    def config_path_grouping(self):
        return self.blue_grouping
    
class ADAP_LRISBlueExtendedSpectrograph(ADAP_LRISExtendedSpectrograph,KeckLRISBSpectrograph):
    def config_path_grouping(self):
        return self.blue_grouping

class ADAP_LRISRedExtendedSpectrograph(ADAP_LRISExtendedSpectrograph,KeckLRISRSpectrograph):
    def config_path_grouping(self):
        return self.red_grouping
    
class ADAP_LRISRedMark4ExtendedSpectrograph(ADAP_LRISExtendedSpectrograph,KeckLRISRMark4Spectrograph):
    def config_path_grouping(self):
        return self.red_grouping

class ADAP_LRISRedOrigExtendedSpectrograph(ADAP_LRISExtendedSpectrograph,KeckLRISROrigSpectrograph):
    def config_path_grouping(self):
        return self.red_grouping


