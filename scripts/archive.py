#!/usr/bin/env python3

import os
import re
from functools import partial
from pathlib import Path
import traceback

from astropy.io import fits
from astropy.table import Table

from pypeit.archive import ArchiveMetadata
from pypeit.core.collate import SourceObject
from pypeit.specobjs import SpecObjs
from pypeit.spectrographs.util import load_spectrograph

class Messages:
    def __init__(self, unrecognized_messages = [], missing_file_messages = [], coadded_file_messages = []):
        self.unrecognized_messages = unrecognized_messages
        self.missing_file_messages = missing_file_messages
        self.coadded_file_messages = coadded_file_messages

    def __add__(self, other):
        return Messages(self.unrecognized_messages + other.unrecognized_messages,
                        self.missing_file_messages + other.missing_file_messages,
                        self.coadded_file_messages + other.self.coadded_file_messages)

    def __iadd__(self, other):
        self.unrecognized_messages += other.unrecognized_messages
        self.missing_file_messages += other.missing_file_messages
        self.coadded_file_messages += other.coadded_file_messages

        return self


def get_metadata_reduced(header_keys, file_info):
    """
    Gets the metadata from FITS files reduced by PypeIt. It is intended to be wrapped 
    by a functools partial object that passes in header_keys. file_info
    is then passed as in by the :obj:`pypeit.archive.ArchiveMetadata` object.

    The file_info is expected to be a tuple of filenames. If another data type
    is added to the ArchiveMetadata object, a list of ``None`` values will be returned.

    Args:
        header_keys (list of str):
            List of FITs header keywords to read from the file being added to the
            archive.

        file_info (str): A tuple containing the spec1d file's name, a spec1d text file related to that
                         file, a spec2d file related to the spec1d file, and the .pypeit file that was used 
                         to create these files.
    
    Returns:
        tuple: A tuple of two lists:

               **data_rows** (:obj:`list` of :obj:`list`):
               The metadata rows built from the redcued FITS file.

               **files_to_copy** (iterable):
               An iterable of tuples. Each tuple has a src file to copy to the archive
               and a destination pathname for that file in the archive. The destination
               pathname is relative to the archive's root directory.
        
    """
    # Source objects are handled by get_metadata_coadded
    if isinstance(file_info, tuple):
        if isinstance(file_info[0], SourceObject):
            # Source objects are handled by get_metadata_coadded
            return (None, None)
        elif len(file_info) == 2 and isinstance(file_info[0], Path) and isinstance(file_info[1], Path):
            # A file that doesn't get a metadata row but should get added to the archive
            # file_info[0]  is the source root directory, and file_info[1] is the full path to the file
            return (None, [(file_info[1], get_archive_reldir(file_info[0], file_info[1]))])

    # Anything else should be a tuple of source_root, spec1d path, spec1d text info path, spec2d path, pypeit path

    # Place the files in a subdir of the archive based on the observation date
    # This is intended to prevent any one directory from having too many files
    source_root = file_info[0]
    spec1d_file = file_info[1]
    header = fits.getheader(str(spec1d_file),0)
    dest_files = [None if file is None else get_archive_reldir(source_root,file) for file in file_info[1:]]

    # Extract koa id from source image filename in header
    id = extract_id(header)

    # Build data row, which starts with koaid, filenames within the archvie, + the metadata
    data_row = [id] + dest_files + [None if x not in header else header[x] for x in header_keys]

    return ([data_row], [(spec1d_file, dest_files[0])])

def get_metadata_coadded(spec1d_header_keys, spec_obj_keys, file_info):
    """
    Gets the metadata from a SourceObject instance used for the collating and coadding files.
    It is intended to be wrapped in by functools
    partial object that passes the desired spec1d and SpecObj keys. file_info
    is then passed in by the :obj:`pypeit.archive.ArchiveMetadata` object.

    If another type of file is added to the ArchiveMetadata object, the file_info
    argument will not be a SourceObject, In this case, a list of ``None`` values are 
    returned.

    Args:
        spec1d_header_keys (list of str):
            The keys to read fom the spec1d headers from the SourceObject.

        spec_obj_keys (list of str):
            The keys to read from the (:obj:`pypeit.specobj.SpecObj`) objects in the SourceObject.

        file_info (:obj:`pypeit.scripts.collate_1d.SourceObject`)): 
            The source object containing the headers, filenames and SpecObj information for a coadd output file.

    Returns:
        tuple: A tuple of two lists:

               **data_rows** (:obj:`list` of :obj:`list`):
               The metadata rows built from the source object.

               **files_to_copy** (iterable):
               An iterable of tuples. Each tuple has a src file to copy to the archive
               and a destination pathname for that file in the archive. The destination
               pathname is relative to the archive's root directory.
    """

    if not isinstance(file_info, tuple) or not isinstance(file_info[0], SourceObject):
        return (None, None)

    source_object = file_info[0]
    par_file = file_info[1]
    coadd2d_file = file_info[2]
    source_root = file_info[3]

    # Place the file in a subdir of the archive based on the observation date
    # This is intended to prevent any one directory from having too many files
    header = fits.getheader(str(source_object.coaddfile))
    coaddfile_dest = get_archive_reldir(source_root, source_object.coaddfile)

    if coadd2d_file is not None:
        coadd2d_file = get_archive_reldir(source_root, coadd2d_file)

    if par_file is not None:
        par_file = get_archive_reldir(source_root, par_file)



    result_rows = []
    for i in range(len(source_object.spec1d_header_list)):

        # Get the spec_obj metadata needed for the archive
        spec_obj = source_object.spec_obj_list[i]
        # Use getattr for the spec_obj data because one of the attributes is actually a property (med_s2n)
        spec_obj_data = [getattr(spec_obj, x) for x in spec_obj_keys]

        # Get the spec1d header metadata needed for the archive
        header = source_object.spec1d_header_list[i]

        # Get the KOAID of the original image for the spec1d
        id = extract_id(header)


        # Use the MJD in the spec1d file to build it's subdirectory, just like get_metadata_reduced does
        # when the spec1d is added to the archive
        spec1d_filename = get_archive_reldir(source_root, Path(source_object.spec1d_file_list[i]))

        header_data = [header[x] if x in header else None for x in spec1d_header_keys]
        result_rows.append([coaddfile_dest, coadd2d_file, par_file] + spec_obj_data + [id, spec1d_filename] + header_data)

    return (result_rows, [(source_object.coaddfile, coaddfile_dest)])

def extract_id(header):
    """
    Pull an id from a file's header.

    This will give preference to a KOAID, but will return an id based on the
    file name if a KOAID can't be found.  A KOAID is of the format
    II.YYYYMMDD.xxxxx.fits. See the `KOA FAQ
    <https://www2.keck.hawaii.edu/koa/public/faq/koa_faq.php>`_ for more
    information.

    Args:
        header (str):   A fits file header.

    Returns:
        str: The an id extracted from the header.
    """

    # First check for the KOAID keyword

    if 'KOAID' in header:
        return header['KOAID']
    else:
        # Attempt to pull KOAID from file name
        filename = header['FILENAME']
        if len(filename) >= 17:
            koaid = filename[0:17]
            if re.match(r'..\.\d{8}\.\d{5}$', koaid) is not None:
                # KOA seems to append .fits to the ID
                return koaid + ".fits"

        # For non KOA products, we use the filename
        return filename

def get_archive_reldir(source_root, source_file):
    """
    Builds a relative path name for a file within the archive based on the original source root.

    Args:
        source_root (:obj:`pathlib.Path`): The root path of the source directory structure. 
                                           This is removed from the final path.
        source_file (:obj:`pathlib.Path`): The full path of the source file to be placed in the 
                                           archive.

    Returns:
        :obj:`str`: Relative path for the file under he archive root.
    """
    relative_src_parts = source_file.relative_to(source_root).parts

    
    # The first two parts of the path should be the slit mask and observing config, which we keep
    final_path = Path(*relative_src_parts[0:2])

    # Strip out the extra directories not needed
    for part in relative_src_parts[2:]:
        if part in ['complete','reduce']:
            continue
        final_path = final_path / part
    return str(final_path)


def find_archvie_files_from_spec1d(spec1d_file):
    """
    Find files related to a spec1d file that should be copied to the archive. 
    Currently these are the spec1d text info file, the spec2d fits file,  and
    the .pypeit file. This function assumes a directory structure where the 
    .pypeit file is in the parent directory of the spec1d file, and the text
    file is in the same directory as the spec1d file. It will exit with an 
    error if a file cannot be found.

    Args:
        spec1d_file (:obj:`str`): 
            Filename of a spec1d file generated by PypeIt.

    Returns:
        tuple: Returns three strings:
        
               **spec1d_text_file** (:obj:`str`): The spec1d text file
               corresponding to the passed in spec1d file.
                                                
               **spec2d_file** (:obj:`str`): The spec2d file
               corresponding to the passed in spec1d file.

               **pypeit_file** (:obj:`str`): The .pypeit file
               corresponding to the passed in spec1d file.

               **missing_archive_msgs** (:obj:`list`): A list of messages for the 
               "collate_warnings.txt" file about missing files needed for archiving.
    """

    missing_archive_msgs = []

    # Check for a corresponding .txt file
    spec1d_text_file = spec1d_file.with_suffix(".txt")

    if not spec1d_text_file.exists():
        msg = f'Could not archive matching text file for {spec1d_file}, file not found.'
        missing_archive_msgs.append(msg)
        spec1d_text_file = None


    # Check for a corresponding spec2d file
    spec2d_file = spec1d_file.with_name(spec1d_file.name.replace("spec1d", "spec2d"))

    if not spec2d_file.exists():
        msg = f'Could not archive matching text file for {spec1d_file}, file not found.'
        missing_archive_msgs.append(msg)
        spec1d_text_file = None

    # Check for a corresponding .pypeit file
    # A file specified in the config or command line takes precedence. Otherwise search in the parent directory
    # of the spec1d file
    pypeit_file = None
    found_pypeit_files = list(spec1d_file.parent.parent.glob("*.pypeit"))

    if len(found_pypeit_files) == 0:
        missing_archive_msgs.append(f'Could not archive matching .pypeit file for {spec1d_file}, file not found.')
    elif len(found_pypeit_files) > 1:
        missing_archive_msgs.append(f'Could not archive matching .pypeit file for {spec1d_file}, found more than one file.')
    else:
        pypeit_file = found_pypeit_files[0]
    
    return spec1d_text_file, spec2d_file, pypeit_file, missing_archive_msgs

def read_coadd_history(header):
    """
    Read the HISTORY keyword information from a FITS file coadded by pypeit.

    Args:
        header (:obj:`str`): 
            Header from a FITS file coadded by PypeIt.

    Returns:
        :obj:`list` of :obj:`tuple`: Returns a list of tuples containing:
        
               **spec1d_file** (:obj:`str`): One of the spec1d files
               that was coadded.

               **obj_names**: (:obj:`list` of obj:`str`): A list of the pypeit object
               names for the spectra that were coadded.


    """
    current_spec1d_file = None
    current_object_list = []
    in_spec1d_filename = False
    history_start_pattern = re.compile(r'PypeIt Coadded (\d+) objects')
    history_spec1d_pattern = re.compile(r'From "(.*)')
    semester_pattern = re.compile(r'^(Semester)|(Program ID)') 
    total_objects = 0
    found_objects = 0
    spec1d_obj_list = []
    found_coadd_history = False
    for history_line in header['HISTORY']:

        # Look for the start of coadding history, in case they are any history lines
        # for other things before it
        start_match = history_start_pattern.search(history_line)
        if start_match is not None:
            found_coadd_history = True
            # Keep track of the total objects so we can tell when we're done
            total_objects = int(start_match.group(1))
            continue
        if not found_coadd_history:
            continue

        # Since the spec1d filename could theoretically span multiple lines,
        # check for the case where we're in the middle of parsing one
        if in_spec1d_filename:
            current_spec1d_file.append(history_line)
            if current_spec1d_file.endswith('"'):
                in_spec1d_filename = False
                current_spec1d_file = current_spec1d_file.rstrip('"')

        # Check for a line introducing a spec1d file
        spec1d_match = history_spec1d_pattern.match(history_line)
        if spec1d_match is not None:
            if current_spec1d_file is not None:
                spec1d_obj_list.append((current_spec1d_file, current_object_list))
                current_object_list = []

            current_spec1d_file = spec1d_match.group(1)
            if not current_spec1d_file.endswith('"'):
                # It continued onto another line
                in_spec1d_filename = True
            else:
                current_spec1d_file = current_spec1d_file.rstrip('"')

        elif semester_pattern.match(history_line) is not None:
            # Skip the additional info Semester/ProgId line
            continue
        else:
            # It's either an object id, or there's an additonal
            # history entry after the coadd entry. Check the
            # number of objects to make sure it's not an 
            # additional entry
            if found_objects >= total_objects:
                break
            else:
                found_objects += 1
                current_object_list.append(history_line)
    # Add the last spec1d/object list found                
    if current_spec1d_file is not None:
        spec1d_obj_list.append((current_spec1d_file, current_object_list))
        current_object_list = []

    return spec1d_obj_list


def create_metadata_archives(archive_root):
    """
    Create an archive with the desired metadata information.

    Metadata is written to three files in the `ipac
    <https://irsa.ipac.caltech.edu/applications/DDGEN/Doc/ipac_tbl.html>`_
    format:

        - ``reduced_files_meta.dat`` contains metadata for the spec1d and spec2d files
          in the archive. This file is only written if copy_to_archive
          is true.

        - ``coadded_files_meta.dat`` contains metadata for the coadded output files.
          This may have multiple rows for each file depending on how many
          science images were coadded. The primary key is a combined key of the
          source object name, filename, and koaid columns. This file is only written 
          if copy_to_archive is true.

    Args:
        archive_root (:obj:`str`):
            The path to archive the metadata and files

    Returns:
        :class:`list[pypeit.archive.ArchiveMetadata]`: Object for archiving files and/or
        metadata.
    """

    archive_metadata_list = []

    # The header keys and column names for reduced_files_meta.dat
    REDUCED_HEADER_KEYS  = ['RA', 'DEC', 'TARGET', 'PROGPI', 'SEMESTER', 'PROGID', 'DISPNAME', 'DECKER',   'BINNING', 'MJD', 'AIRMASS', 'EXPTIME']
    REDUCED_COLUMN_NAMES = ['ra', 'dec', 'target', 'progpi', 'semester', 'progid', 'dispname', 'slmsknam', 'binning', 'mjd', 'airmass', 'exptime']

    # The header keys and column names for coadded_files_meta.dat
    COADDED_SPEC1D_HEADER_KEYS  = ['DISPNAME', 'DECKER',   'BINNING', 'MJD', 'AIRMASS', 'EXPTIME','GUIDFWHM', 'PROGPI', 'SEMESTER', 'PROGID']
    COADDED_SPEC1D_COLUMN_NAMES = ['dispname', 'slmsknam', 'binning', 'mjd', 'airmass', 'exptime','guidfwhm', 'progpi', 'semester', 'progid']

    # The SpecObj keys and column names for coadded_files_meta.dat
    COADDED_SOBJ_KEYS  =        ['MASKDEF_OBJNAME', 'MASKDEF_ID', 'NAME',        'DET', 'RA',    'DEC',    'S2N', 'MASKDEF_EXTRACT', 'WAVE_RMS']
    COADDED_SOBJ_COLUMN_NAMES = ['maskdef_objname', 'maskdef_id', 'pypeit_name', 'det', 'objra', 'objdec', 's2n', 'maskdef_extract', 'wave_rms']

    # Create the ArchieMetadata objects for reduced_files_meta and coadded_files_meta
    reduced_names = ['koaid', 'spec1d_file', 'spec1d_info', 'spec2d_file', 'pypeit_file'] + REDUCED_COLUMN_NAMES
    reduced_metadata = ArchiveMetadata(archive_root / "reduced_files_meta.dat",
                                       reduced_names,
                                       partial(get_metadata_reduced, REDUCED_HEADER_KEYS),
                                       append=True)
    archive_metadata_list.append(reduced_metadata)

    coadded_formats = {'s2n':      '%.2f',
                        'wave_rms': '%.3f'}

    coadded_col_names = ['coadd1d_filename', 'coadd2d_filename', 'coadd1d_par_file'] + \
                        COADDED_SOBJ_COLUMN_NAMES + \
                        ['source_id', 'spec1d_filename'] + \
                        COADDED_SPEC1D_COLUMN_NAMES

    coadded_metadata = ArchiveMetadata(archive_root / "coadded_files_meta.dat",
                                        coadded_col_names,
                                        partial(get_metadata_coadded,
                                                COADDED_SPEC1D_HEADER_KEYS,
                                                COADDED_SOBJ_KEYS),
                                        append=True,
                                        formats = coadded_formats)                                             
    archive_metadata_list.append(coadded_metadata)

    # Return an archive object with the metadata objects
    return archive_metadata_list

def keep_in_archive(relative_path):
    path_parts = relative_path.parts
    # Exclude keck_deimos_A not under "reduce". These probably originate from the original
    # organizing of raw files
    for i in range(len(path_parts)):
        if path_parts[i] == "keck_deimos_A":
            if i == 0 or path_parts[i-1] !="reduce":
                return False

    parent = relative_path.parent
    ext = relative_path.suffix
    
    if ext == ".gz":
        if parent.name == "keck_deimos_A" and relative_path.name == "QA.tar.gz":
            return True
        elif parent.name == "2D_Coadd" and relative_path.name == "QA_coadd.tar.gz":
            return True
        else:
            if relative_path.name.lower().endswith(".fits.gz"):
                if parent.name in ['Science', 'Calibrations', '1D_Coadd', 'Science_coadd']:
                    return True
    elif ext == ".fits":
        if parent.name in ['Science', 'Calibrations', '1D_Coadd', 'Science_coadd']:
            return True
    elif ext == ".png":
        if parent.name == 'PNGs' and parent.parent.name == "QA_coadd":
            return True
    elif ext == ".par":
        if parent.name in ["keck_deimos_A", "1D_Coadd", "2D_Coadd"]:
            return True
    elif ext in ['.calib', '.pypeit', '.log']:
        if parent.name == "keck_deimos_A":
            return True
    elif ext =='.coadd2d':
        if parent.name == "2D_Coadd":
            return True
    elif ext == '.txt':
        if parent.name == '1D_Coadd':
            if relative_path.name == "collate_warnings.txt":
                return True
            elif relative_path.name.endswith(".log.txt"):
                return True
        elif parent.name == '2D_Coadd':
            if relative_path.name.endswith(".log.txt") and relative_path.name != "pypeit_setup_coadd2d.log.txt":
                return True
        elif relative_path.name.startswith("spec1d"):
            return True
    elif ext == '.dat' and relative_path.name =="collate_report.dat" and parent.name == '1D_Coadd':
        return True
        
    return False

def populate_archive(archive, source_archive_root, dirs_to_scan):

    messages = Messages()

    spec1d_map = {}
    coadded_files = []

    while len(dirs_to_scan) > 0:
        dir = dirs_to_scan.pop()
        for file in dir.iterdir():
            if file.is_dir():
                dirs_to_scan.append(file)
            # Check for .fits or .fits.gz
            elif file.suffix == ".fits" or (len(file.suffixes) >= 2 and file.suffixes[-2] == ".fits" and file.suffixes[-1] == ".gz"):
                
                # HDU1 will contain the DMODCLS key PypeIt uses to indicate the type of file
                try:
                    hdr = fits.getheader(file, 1)
                except:
                    messages.unrecognized_messages.append(f"Could not get HDU 1 FITS header from {file}")
                    continue

                if 'DMODCLS' in hdr:
                    if dir.name == "Science" and hdr['DMODCLS'] == 'SpecObj':
                        # Spec1d add to the archive. This will also find any
                        # associated spec2d, .txt and .pypeit files
                        (txt_file, spec2d_file, pypeit_file, msgs) = find_archvie_files_from_spec1d(file)
                        if len(msgs) > 0:
                            messages.missing_file_messages.append(msgs)

                        archive.add([(source_archive_root, file, txt_file, spec2d_file, pypeit_file)])

                        # Keep a map of spec1d files so we can find them
                        # easily when building the SourceObjects for
                        # coadded objects
                        spec1d_map[file.name] = file

                    elif hdr['DMODCLS'] == 'OneSpec':
                        # Coadded file. We save these for the end to make
                        # sure all the spec1ds that were used for it have been
                        # added first

                        coadded_files.append(file)
                    else:
                        if keep_in_archive(file.relative_to(source_archive_root)):
                            archive.add([(source_archive_root, file)])
                        else:
                            messages.unrecognized_messages.append(f"Unrecognized  PypeIt FITS file: {file}")
                else:
                    messages.unrecognized_messages.append(f"Could not get DMODCLS from FITS file: {file}")
            elif keep_in_archive(file.relative_to(source_archive_root)):
                archive.add([(source_archive_root, file)])
            else:
                messages.unrecognized_messages.append(f"{file} is not a file to archive.")
    
    # Go through the coadded files, use their associated collate.dat files to build SourceObjects, and
    # add those to the archive
    spec = load_spectrograph("keck_deimos")
    for coadded_file in coadded_files:
        source_object = None       
        try:
            dat_file = coadded_file.with_name("collate_report.dat")
            collate_report_table = Table.read(str(dat_file), format="ipac")
            related_rows = collate_report_table['filename'] == coadded_file.name
            # There will probably only be one object per spec1d file since we mosaic, but I do this
            # just in case.
            spec1d_grouped_table = collate_report_table[related_rows].group_by(['spec1d_filename'])
            for spec1d_group in spec1d_grouped_table.groups:
                spec1d_filename = spec1d_group[0]['spec1d_filename']
                full_spec1d_file = spec1d_map[spec1d_filename]
                sobjs = SpecObjs.from_fitsfile(full_spec1d_file, chk_version=False)
                for obj_name in spec1d_group['pypeit_name']:
                    sobj = sobjs[sobjs.name_indices(obj_name)][0]
                    if source_object is None:
                        source_object = SourceObject(sobj, sobjs.header, full_spec1d_file, spec, 'ra/dec')
                        source_object.coaddfile = coadded_file
                    else:
                        source_object.spec_obj_list.append(sobj)
                        source_object.spec1d_header_list.append(sobjs.header)
                        source_object.spec1d_file_list.append(full_spec1d_file)
        except Exception:
            message = f"Could not include {coadded_file} in archive because of an exception:\n"
            message += traceback.format_exc()
            messages.coadded_file_messages.append(message)
            source_object = None

        if source_object is not None:
            par_file = coadded_file.parent.joinpath('collate1d.par')
            if not par_file.exists():
                messages.missing_file_messages.append(f'Could not archive matching collate1d.par file for {coadded_file}.')
                par_file = None
            
            coadd2d_file = None
            coadd2d_dir = coadded_file.parent.with_name("2D_Coadd")
            if coadd2d_dir.exists() and coadd2d_dir.is_dir():
                coadd2d_files = list((coadd2d_dir / "Science_coadd").rglob("spec2d*.fits")) 
                if len(coadd2d_files) >= 1:
                    if coadd2d_files[0].exists():
                        coadd2d_file = coadd2d_files[0]
            if coadd2d_file is None:
                messages.missing_file_messages.append(f'Could not archive matching coadd2d file for {coadded_file}.')

            archive.add([(source_object, par_file, coadd2d_file, source_archive_root)])

    return messages

def write_messages(report_file, messages, extra_lines=[]):
    with open(report_file, "w") as f:

        print("Unrecognized files in source_dirs:", file=f)
        for msg in messages.unrecognized_messages:
            print(msg, file=f)

        print("Problems archiving coadded files:", file=f)
        for msg in messages.coadded_file_messages:
            print(msg, file=f)

        print("Missing files:", file=f)
        for msg in messages.missing_file_messages:
            print(msg, file=f)

        if len(extra_lines) >0:
            print("=======================================================", file=f)
            for line in extra_lines:
                print(line, file=f)
