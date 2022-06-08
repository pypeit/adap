#!/usr/bin/env python3

import os
from glob import glob
import re
from functools import partial
from pathlib import Path
import shutil
import traceback

from astropy.io import fits
from astropy.table import Table
from astropy.units.core import UnrecognizedUnit
from numpy.core.numeric import full

from pypeit import msgs
from pypeit.scripts import scriptbase
from pypeit.archive import ArchiveMetadata, ArchiveDir
from pypeit.core.collate import SourceObject
from pypeit.specobjs import SpecObjs
from pypeit.spectrographs.util import load_spectrograph

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
    if isinstance(file_info, tuple) and isinstance(file_info[0], SourceObject):
        return (None, None)

    # Anything else should be a tuple of header, spec1d file, spec1d text info file, spec2d file, pypeit file

    # Place the files in a subdir of the archive based on the observation date
    # This is intended to prevent any one directory from having too many files
    spec1d_file = file_info[0]
    header = fits.getheader(spec1d_file,0)
    subdir_name = get_archive_subdir(header)
    dest_files = [os.path.join(subdir_name, os.path.basename(x)) if x is not None else None for x in file_info]

    # Extract koa id from source image filename in header
    id = extract_id(header)

    # Build data row, which starts with koaid, filenames within the archvie, + the metadata
    data_row = [id] + dest_files + [None if x not in header else header[x] for x in header_keys]

    return ([data_row], zip(file_info, dest_files))

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

    # Place the file in a subdir of the archive based on the observation date
    # This is intended to prevent any one directory from having too many files
    header = fits.getheader(source_object.coaddfile)
    subdir_name = get_archive_subdir(header)
    coaddfile = os.path.join(subdir_name, os.path.basename(source_object.coaddfile))
    if par_file is not None:
        par_file_dest_basename =  os.path.splitext(os.path.basename(coaddfile))[0] + "_" + par_file.name
        par_file_dest = os.path.join(subdir_name, par_file_dest_basename)
    else:
        par_file_dest = None

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
        subdir_name = get_archive_subdir(header)
        spec1d_filename = os.path.join(subdir_name, os.path.basename(source_object.spec1d_file_list[i]))

        header_data = [header[x] if x in header else None for x in spec1d_header_keys]
        result_rows.append([coaddfile, par_file_dest] + spec_obj_data + [id, spec1d_filename] + header_data)

    return (result_rows, [(source_object.coaddfile, coaddfile), (par_file, par_file_dest)])

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

def get_archive_subdir(header):
    """
    Builds a subdirectory name for a file within the archive based on header keywords.  

    Args:
        header (:obj:`astropy.io.fits.Header`): FITS header of the file to put into the archive.

    Returns:
        :obj:`str`: Subdirectory under the archive root to place the file.  
    """
    if 'PROGID' in header and 'SEMESTER' in header:
        return header['SEMESTER'] + '_' + header['PROGID']
    else:
        # If there's not enough information in the header to determine the subdirectory name,
        # place the file in the root directory
        return ""

def find_archvie_files_from_spec1d(args, spec1d_file):
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
    if args.pypeit_file is not None:
        if not os.path.exists(args.pypeit_file):
            missing_archive_msgs.append(f"Could not archive passed in .pypeit file {args.pypeit_file}, file not found.")
        else:
            pypeit_file = args.pypeit_file
    else:
        found_pypeit_files = list(spec1d_file.parent.parent.glob("*.pypeit"))

        if len(found_pypeit_files) == 0:
            missing_archive_msgs.append(f'Could not archive matching .pypeit file for {spec1d_file}, file not found.')
        elif len(found_pypeit_files) > 1:
            missing_archive_msgs.append(f'Could not archive matching .pypeit file for {spec1d_file}, found more than one file.')
        else:
            pypeit_file = found_pypeit_files[0]
    
    return str(spec1d_text_file), str(spec2d_file), str(pypeit_file), missing_archive_msgs

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
    history_start_pattern = re.compile('PypeIt Coadded (\d+) objects')
    history_spec1d_pattern = re.compile('From "(.*)')
    semester_pattern = re.compile('^(Semester)|(Program ID)') 
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


def create_archive(archive_root, copy_to_archive):
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
        copy_to_archive (:obj:`bool`):
            If true, files will be stored in the archive.  If false, only
            metadata is stored.

    Returns:
        :class:`~pypeit.archive.ArchiveDir`: Object for archiving files and/or
        metadata.
    """
    # Make sure archive dir, if specified, exists
    os.makedirs(archive_root, exist_ok=True)

    archive_metadata_list = []

    # The header keys and column names for reduced_files_meta.dat
    REDUCED_HEADER_KEYS  = ['RA', 'DEC', 'TARGET', 'PROGPI', 'SEMESTER', 'PROGID', 'DISPNAME', 'DECKER',   'BINNING', 'MJD', 'AIRMASS', 'EXPTIME']
    REDUCED_COLUMN_NAMES = ['ra', 'dec', 'target', 'progpi', 'semester', 'progid', 'dispname', 'slmsknam', 'binning', 'mjd', 'airmass', 'exptime']

    # The header keys and column names for coadded_files_meta.dat
    COADDED_SPEC1D_HEADER_KEYS  = ['DISPNAME', 'DECKER',   'BINNING', 'MJD', 'AIRMASS', 'EXPTIME','GUIDFWHM', 'PROGPI', 'SEMESTER', 'PROGID']
    COADDED_SPEC1D_COLUMN_NAMES = ['dispname', 'slmsknam', 'binning', 'mjd', 'airmass', 'exptime','guidfwhm', 'progpi', 'semester', 'progid']

    # The SpecObj keys and column names for coadded_files_meta.dat
    COADDED_SOBJ_KEYS  =        ['MASKDEF_OBJNAME', 'MASKDEF_ID', 'NAME',        'DET', 'RA',    'DEC',    'med_s2n', 'MASKDEF_EXTRACT', 'WAVE_RMS']
    COADDED_SOBJ_COLUMN_NAMES = ['maskdef_objname', 'maskdef_id', 'pypeit_name', 'det', 'objra', 'objdec', 's2n',     'maskdef_extract', 'wave_rms']

    # Create the ArchieMetadata objects for reduced_files_meta and coadded_files_meta
    reduced_names = ['koaid', 'spec1d_file', 'spec1d_info', 'spec2d_file', 'pypeit_file'] + REDUCED_COLUMN_NAMES
    reduced_metadata = ArchiveMetadata(os.path.join(archive_root, "reduced_files_meta.dat"),
                                                reduced_names,
                                                partial(get_metadata_reduced, REDUCED_HEADER_KEYS),
                                                append=True)
    archive_metadata_list.append(reduced_metadata)

    coadded_formats = {'s2n':      '%.2f',
                        'wave_rms': '%.3f'}

    coadded_col_names = ['filename', 'par_file'] + \
                        COADDED_SOBJ_COLUMN_NAMES + \
                        ['source_id', 'spec1d_filename'] + \
                        COADDED_SPEC1D_COLUMN_NAMES

    coadded_metadata = ArchiveMetadata(os.path.join(archive_root, "coadded_files_meta.dat"),
                                        coadded_col_names,
                                        partial(get_metadata_coadded,
                                                COADDED_SPEC1D_HEADER_KEYS,
                                                COADDED_SOBJ_KEYS),
                                        append=True,
                                        formats = coadded_formats)                                             
    archive_metadata_list.append(coadded_metadata)

    # Return an archive object with the metadata objects
    return ArchiveDir(archive_root, archive_metadata_list, copy_to_archive=copy_to_archive)




class ArchiveScript(scriptbase.ScriptBase):

    @classmethod
    def get_parser(cls, width=None):

        parser = super().get_parser(description='Create an archive of fits files and metadata for submission to KOA.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('archive_dir', type=str, help="Directory to contain the archive.")
        parser.add_argument('source_dirs', type=str, nargs='*',
                            help='One or more source directories containing pypeit output to archive.')
        parser.add_argument('--no_copy', default=False, action='store_true', help="Just create metadata without copying files")
        parser.add_argument('--pypeit_file', type=str, help="PypeIt file to associate spec1d files. If not specified this script will look in the parent directory of each spec1d file.")
        parser.add_argument('--report', type=str, default="report.txt", help="Location of a report file indicating any missing files. Defaults to report.txt.")
        return parser

    @staticmethod
    def main(args):

        # Create the archive objects. This will create directories if needed, or open up
        # metadata for files in a pre-existing archive
        archive = create_archive(args.archive_dir, not args.no_copy)

        unrecognized_messages = []
        missing_file_messages = []
        spec1d_map = dict()
        coadded_files = []
        # Known extensions in pypeit directories that aren't directly read but shouldn't
        # cause warnings. .txt and .pypeit files will be archived when their associated
        # spec1d is archived.
        exts_to_skip = ['.pypeit', '.txt', '.png', '.calib', '.html', '.par', '.dat']

        # Recursively scan all of the source directories from the command line arguments
        dirs_to_scan = list([Path(x) for x in args.source_dirs])
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
                        unrecognized_messages.append(f"Could not get HDU 1 FITS header from {file}")
                        continue

                    if 'DMODCLS' in hdr:
                        if hdr['DMODCLS'] == 'SpecObj':
                            # Spec1d add to the archive. This will also find any
                            # associated spec2d, .txt and .pypeit files
                            (txt_file, spec2d_file, pypeit_file, messages) = find_archvie_files_from_spec1d(args, file)
                            if len(messages) > 0:
                                missing_file_messages.append(messages)

                            archive.add([(str(file), txt_file, spec2d_file, pypeit_file)])

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
                            # Ignore other pypeit files we don't want to archive
                            pass
                    else:
                        unrecognized_messages.append(f"Could not get DMODCLS from FITS file: {file}")
                elif file.suffix in exts_to_skip:
                    # Ignore these, they'll be picked up when adding spec1ds to the archive
                    pass
                else:
                    unrecognized_messages.append(f"{file} has an unknown extension")
        
        # Go through the coadded files, use their history to build SourceObjects, and
        # add those to the archive
        spec = load_spectrograph("keck_deimos")
        coadded_file_messages = []
        for coadded_file in coadded_files:            
            try:
                hdr = fits.getheader(coadded_file)
                spec1d_obj_lists = read_coadd_history(hdr)
                source_object = None
                for (spec1d_file, obj_list) in spec1d_obj_lists:
                    if spec1d_file not in spec1d_map:
                        coadded_file_messages.append(f"Could not include {coadded_file} in archive because one of its spec1d files: {spec1d_file} was not found in any of the source directories.")
                        source_object = None
                        break

                    full_spec1d_file = spec1d_map[spec1d_file]
                    sobjs = SpecObjs.from_fitsfile(full_spec1d_file)
                    
                    for obj_name in obj_list:
                        sobj = sobjs[sobjs.name_indices(obj_name)][0]
                        if source_object is None:
                            source_object = SourceObject(sobj, sobjs.header, full_spec1d_file, spec, 'ra/dec')
                            source_object.coaddfile = str(coadded_file)
                        else:
                            source_object.spec_obj_list.append(sobj)
                            source_object.spec1d_header_list.append(sobjs.header)
                            source_object.spec1d_file_list.append(full_spec1d_file)
            except Exception:
                message = f"Could not include {coadded_file} in archive because of an exception:\n"
                message += traceback.format_exc()
                coadded_file_messages.append(message)
                source_object = None

            if source_object is not None:
                par_file = coadded_file.parent.joinpath('collate1d.par')
                if par_file.exists():
                    archive.add([(source_object, par_file)])
                else:
                    missing_file_messages.append(f'Could not archive matching collate1d.par file for {coadded_file}.')
                    archive.add([(source_object, None)])

        archive.save()

        with open(args.report, "w") as f:
            print("Unrecognized files in source_dirs:", file=f)
            for msg in unrecognized_messages:
                print(msg, file=f)

            print("Problems archiving coadded files:", file=f)
            for msg in coadded_file_messages:
                print(msg, file=f)

            print("Missing files:", file=f)
            for msg in missing_file_messages:
                print(msg, file=f)

        if not args.no_copy:
            print(f"Copying README to archive root.")
            script_path = Path(__file__).parent.absolute().joinpath("archive_README")
            shutil.copy2(script_path, Path(args.archive_dir).joinpath("README"))



if __name__ == '__main__':
    ArchiveScript.entry_point()


