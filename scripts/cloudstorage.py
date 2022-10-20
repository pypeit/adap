"""
Cloud Storage integration.

S3 support requires installing "boto3" with pip

Google Drive support requires installing "google-api-python-client" with pip

.. include common links, assuming primary doc root is up one directory
.. include:: ../include/links.rst
"""

import fnmatch
from pathlib import Path
import mimetypes 
from abc import ABC, abstractmethod

# Try to import S3 packages
try:
    import boto3
    _s3_support_enabled = True
except:
    _s3_support_enabled = False

# Try to import Google Drive packages
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    _google_drive_support_enabled=True
except:
    _google_drive_support_enabled=False

class Storage(ABC):
    """
    Abstraction for accessing a cloud storage platform.

    Locations are represented as a POSIX path, regardless of how the underlying platform represents locations.

    """

    @abstractmethod
    def list_objects(self, path, patterns=[]):
        """
        List the objects within a given path, recursively descending into subdirectories.

        Args:
        path (str or pathlib.Path):  The Path to search.
        patterns (list of str):      A file glob pattern to search for.

        Return:
        (sequence of tuple):  Returns a tuple for each object found. The first element of the tuple
                              is the path of the file. the second is its size in bytes.
        """
        pass

    @abstractmethod
    def copy(self, source_path, dest_path):
        """
        Copies a file within this cloud platform from one path to another.

        Args:
        source_path (str): The path of the file to copy.
        dest_path (str): The path for the file of the object.
        """
        pass

    @abstractmethod
    def upload(self, source_file, dest_path):
        """
        Uploads a local file to the cloud.

        Args:
        source_path (str or pathlib.Path): The local file to upload.
        dest_path (str): The path in the cloud to upload to.
        """
        pass

    @abstractmethod
    def download(self, source_path, dest_dir):
        """
        Downloads a file from the cloud.

        Args:
        source_path (str): The path in the cloud to download from.
        source_path (str or pathlib.Path): The local path where the file will be placed.
        """
        pass

    @abstractmethod
    def delete(self, path):
        """
        Deletes an object in the cloud.

        Args:
        path (str): The path of the object to delete.
        """
        pass


class S3Storage(Storage):
    """
    Abstraction for accessing S3 storage.

    Args:
    endpoint_url (str):  The URL to the S3 storage provider

    """
    def __init__(self, endpoint_url):
        session = boto3.Session()
        self._s3_client = session.client('s3', endpoint_url = endpoint_url)

    def _get_bucket_and_key(self, path):
        """
        Helper method to separate the S3 bucket (top level directory) from the key (remainder of file name)
        """
        bucket_end_index = path.find('/')
        if bucket_end_index < 0:
            raise ValueError(f"S3 path does not have a bucket. {path}")
        elif bucket_end_index+1 >= len(path):
            # No key, it's just referring to the bucket
            return path[0:bucket_end_index], ""
        else:
            return path[0:bucket_end_index], path[bucket_end_index+1:]


    def list_objects(self, path, patterns=[]):
        """
        List the objects within a given S3 path (recursive).

        Args:
        path (str or pathlib.Path):  The Path within s3 to search, starting with the bucket name.
        patterns (list of str):      A file glob pattern to search for.

        Return:
        (sequence of tuple):  Returns a tuple for each object found. The first element of the tuple
                              is the path of the file. the second is its size in bytes.
        """
        bucket, key = self._get_bucket_and_key(path)
        paginator = self._s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            if 'Contents' in page:
                for object in page['Contents']:
                    if len(patterns) == 0:
                        yield (bucket + '/' + object['Key'], object['Size'])
                    else:
                        for pattern in patterns:
                            if fnmatch.fnmatchcase(object['Key'], pattern):
                                yield (bucket + '/' + object['Key'], object['Size'])


    def copy(self, source_path, dest_path):
        """
        Copies a file within S3 from one path to another.

        Args:
        source_path (str): The path of the object to copy.
        dest_path (str): The path for the copy of the object.

        """
        source_bucket, source_key = self._get_bucket_and_key(source_path)
        dest_bucket, dest_key = self._get_bucket_and_key(dest_path)
        source = {'Bucket': source_bucket, 'Key': source_key}
        self._s3_client.copy(source, dest_bucket, dest_key)

    def upload(self, source_file, dest_path):
        """
        Uploads a local file to S3.

        Args:
        source_path (str or pathlib.Path): The local file to upload.
        dest_path (str): The path in S3 to upload to, starting with the bucket name.
        """
        dest_bucket, dest_key = self._get_bucket_and_key(dest_path)

        return self._s3_client.upload_file(str(source_file), dest_bucket, dest_key)

    def download(self, source_path, dest_dir):
        """
        Downloads a file from S3.

        Args:
        source_path (str): The path in S3 to download from, starting with the bucket name.
        source_path (str or pathlib.Path): The local path where the file will be placed.
        """

        source_bucket, source_key = self._get_bucket_and_key(source_path)

        if isinstance(dest_dir, str):
            dest_dir = Path(dest_dir)
        dest_file = dest_dir / source_key.split("/")[-1]

        self._s3_client.download_file(source_bucket, source_key, str(dest_file) )
        return dest_file

    def delete(self, path):
        """
        Deletes an object in S3.

        Args:
        path (str): The path of the object to delete.
        """
        bucket, key = self._get_bucket_and_key(path)

        self._s3_client.delete_object(Bucket=bucket, Key=key)



class GoogleDriveStorage(Storage):
    """
    Abstraction for accessing Google Drivestorage.

    """

    class _GDriveFile():
        """
        Internal helper class representing a file in Google Drive. 
        
        For a file or folder that exists in Google Drive, this class should be initialized with
        the file_meta argument.  
        
        For a non-existent file or folder, this class should be initialized with the name and (optional) parent.

        Args:
        file_meta (dict): The metadata for the file as returned from Google Drive
        name (str):       The name of the file (used for non-existent files).
        parent (_GDriveFile()): The parent folder of this file, or None if the parent is unknown
                                (used for non-existent files.


        """
        def __init__(self, file_meta = None, name=None, parent=None):
            if file_meta is None and name is not None:
                self.exists = False
                self.id = None
                self.name = name
                self.size = None
                self.is_folder = False
            elif file_meta is not None and name is None:
                self.exists = True
                self.id = file_meta['id']
                self.name = file_meta['name']
                self.is_folder = (file_meta['mimeType'] == 'application/vnd.google-apps.folder')
                self.size = file_meta.get('size', 0)
            else:
                raise RuntimeError("Exactly one and only one of file_meta and name should be given.")

            self.parent = parent
            
            if self.parent is None:
                self.full_path = Path(self.name)
            else:
                self.full_path = self.parent.full_path / self.name


    def __init__(self, credentials_file):
        """
        Initialize the Google Drive api.

        Args:
        credentials_file (str): File name of a service account's credentials.
        """
        self._credentials = service_account.Credentials.from_service_account_file(credentials_file)
        self._service = build('drive', 'v3', credentials=self._credentials)
        self._files = self._service.files()

        # Add some PypeIt specific mimetypes
        mimetypes.add_type("text/plain", ".pypeit")
        mimetypes.add_type("text/plain", ".calib")
        mimetypes.add_type("text/plain", ".sorted")
        mimetypes.add_type("text/plain", ".log")



    def _look_for_objects(self, name=None, folder_to_list=None, folders_only=False, recursive=False):
        """
        Helper method to search for objects in Google Drive. All of the arguments are optional,
        in which case the method returns every object visible to the caller. 

        Args:
        name (str): Name of the file
        folder_to_list (_GDriveFile): A folder to search. Results will be restricted to this folder.
        folders_only (bool): Only return folders in the results.
        recursive (bool): If set, recursively return results for all folders.

        Return:
        Sequence of _GDriveFiles: All of the objects found in Google Drive.
        """
        fields = "nextPageToken,files(id,name,mimeType,parents,size)"
        folder_search_queue = [folder_to_list]
        page_token = None
        while page_token is not None or len(folder_search_queue) > 0:
            # Get the next folder to search, but only if we finished searching the previous one
            # (i.e. there's no page token left over from the last page)            
            if page_token is None:
                parent = folder_search_queue.pop()

            # Build the query for the list
            query_parts = []
            if name is not None:
                query_parts.append(f"name = '{name}'")
            if parent is not None:
                query_parts.append(f"'{parent.id}' in parents")
            if folders_only:
                query_parts.append("mimeType = 'application/vnd.google-apps.folder'")

            query = " and ".join(query_parts)

            # Run the list request to the Google API
            request = self._files.list(corpora='user', q=query, fields=fields, pageToken=page_token, supportsAllDrives=True, pageSize=50)
            result = request.execute()

            # Go through any results and return them
            for file_dict in result.get('files', []):
                file = GoogleDriveStorage._GDriveFile(file_meta=file_dict,parent=parent)
                yield file

                # For recursive searches, push any folders onto the queue
                if recursive is True and file.is_folder is True:
                    folder_search_queue.push(file)

            # Determine if there are more pages
            if 'nextPageToken' in result:
                page_token = result['nextPageToken']
            else:
                page_token = None
            
    def _find_file_by_path(self, path, not_exist_ok=False, parent=None):
        """
        Helper method to build a _GDriveFile object for a path. Because Google Drive
        doesn't use paths, it has to go through each path entry to find it.

        Args:
        path (str): The path of the file to search for.

        non_exist_ok  (bool): Whether or not non-existent path elements are okay. Defaults to False.
                              If this is False an exception is raised if the path doesn't exist.
        parent (_GDriveFile): The parent folder the path is relative to. Defaults to None.

        Return:
        _GDriveFile:  A file object representing the path (and all elements along the path).

        """
        # There's no single "root" path, just multiple items visible to the account
        if path is None or path=="":
            raise ValueError("Cannot support a 'root' folder with Google Drive")

        # Go through each part of the passed in path, and try to find it in Google Drive
        path_obj = Path(path)
        folder_to_list = parent
        for i in range(len(path_obj.parts)):
            
            # First determine if this should be a folder. This is used when building
            # non-existent paths, but also to reduce the number of items return from the
            # google API when only folders need be returned.
            if path_obj.parts[i] == "/":
                raise ValueError("Cannot support a 'root' folder with Google Drive")
            elif i < len(path_obj.parts) -1 :
                # This is not the last item in the path, it should be a folder
                should_be_folder = True
            elif i == len(path_obj.parts)-1:
                # It is the last item, it should only be a folder if it has a /
                if path_obj.parts[i].endswith("/"):
                    should_be_folder = True
                else:
                    should_be_folder = False
            else:
                should_be_folder = False

            if folder_to_list is not None:
                # There is a parent folder to list

                if not folder_to_list.exists:
                    # If that folder doesn't exist, there's nothing in it.
                    files = []
                elif not folder_to_list.is_folder:
                    raise ValueError(f"{folder_to_list.full_path} is not a folder.")
                else:
                    files = list(self._look_for_objects(name=path_obj.parts[i], folders_only=should_be_folder, folder_to_list=folder_to_list))
            else:
                files = list(self._look_for_objects(name=path_obj.parts[i], folders_only=should_be_folder))

            if len(files) == 0:
                # The file doesn't exist.
                if not_exist_ok:         
                    # Create a non-existent file object if this is desired by the caller
                    # We'll mark it a folder based its position in the path and if the original
                    # path was a folder
                    folder_to_list = GoogleDriveStorage._GDriveFile(name = path_obj.parts[i], parent=folder_to_list)
                    folder_to_list.is_folder = should_be_folder
                else:
                    # Caller wants the file to exist
                    raise ValueError(f"Path {'/'.join(path_obj.parts[0:i+1])} not found.")
            elif len(files) > 1:
                # Google Drive supports multiple items with the same name under a folder,
                raise ValueError(f"Path {'/'.join(path_obj.parts[0:i+1])} is ambiguous.")
            else:
                # Only one file is found, use it as the next folder to search
                # Or the file to return
                folder_to_list=files[0]

        return folder_to_list

    def _create_path(self, path):
        """
        Helper method to create all of the path elements of a non-existent path in Google Drive.

        Args:
        path (_GDriveFile): A path with one or more non-existent path elements.
        """
        paths_to_create = [path]
        
        # Go backwards through the path heiarchy until we find a parent that exists
        while paths_to_create[0].parent is not None and not paths_to_create[0].parent.exists:
            paths_to_create.insert(0, paths_to_create[0].parent)

        # Now go forwards thorugh the non-existent path elements, creating each one and
        # setting it as the parent as the next one.
        last_path_created = None
        for next_path in paths_to_create:
            
            if next_path.parent is None:
                raise ValueError(f"Cannot create path {next_path.full_path} from root in Google Drive.")

            # Set the parent id from the previusly created path
            if next_path.parent.id is None and last_path_created is not None:
                next_path.parent.id = last_path_created.id
            
            # Create the folder
            dir_create_body = { "name":next_path.name, "parents": [ next_path.parent.id ], "mimeType": "application/vnd.google-apps.folder" }
            request = self._files.create(body=dir_create_body, supportsAllDrives=True)
            result = request.execute()

            # Get the file id from Google
            if result is not None and "id" in result:
                next_path.id = result["id"]
            else:
                raise RuntimeError(f"Failed to create path {next_path.full_path} in Google Drive.")
            
            # Save this path to be the parent of the next one in the loop
            last_path_created  = next_path

    def list_objects(self, path, patterns=[]):
        """
        List the objects within a given Google Drive path (recursive).

        Args:
        path (str or pathlib.Path):  The path to search. If this is None all objects visible 
                                     in google drive are returned.
        patterns (list of str):      A file glob pattern to filter the items returned.

        Return:
        (sequence of tuple):  Returns a tuple for each object found. The first element of the tuple
                              is the path of the file. the second is its size in bytes.
        """
        raise NotImplementedError("Google Drive list objects has not been implemented yet.")

    def copy(self, source_path, dest_path):
        """
        Copies a file within Google Drive from one path to another.

        Args:
        source_path (str): The path of the object to copy.
        dest_path (str): The path for the copy of the object.

        """

        raise NotImplementedError("Google Drive copy has not been implemented yet.")

    def upload(self, source_file, dest_path):
        """
        Uploads a local file to Google Drive.

        Args:
        source_path (str or pathlib.Path): The local file to upload.
        dest_path (str): The path to upload to. This must not be None because there's no single "root"
                         in google drive. If this path does not exist it will be created.
        """

        source_path = Path(source_file)

        dest_file = self._find_file_by_path(dest_path, not_exist_ok=True)

        # Find the parent folder, and create any needed folders
        if not dest_file.is_folder:
            parent = dest_file.parent
            if parent is None:
                raise ValueError("Cannot upload to google drive without a parent folder")
            name = dest_file.name
        else:
            parent = dest_file
            name = source_path.name

        if not parent.exists:
            self._create_path(parent)

        # Get the mimetype. If this is None, google will guess on its own
        mimetype = mimetypes.guess_type(source_file)[0]

        # Get the media upload object
        media = MediaFileUpload(source_path, mimetype=mimetype)

        if dest_file.exists and not dest_file.is_folder:
            # Uploading to an existing file, we have to use update
            request = self._files.update(fileId=dest_file.id, media_body=media, supportsAllDrives=True)
        else:
            # Otherwise create a new file
            file_create_body = {"name": name, "parents": [parent.id]}
            request = self._files.create(body=file_create_body, media_body=media, supportsAllDrives=True)

        request.execute()

    def download(self, source_path, dest_dir):
        """
        Downloads a file from Google Drive.

        Args:
        source_path (str): The path in Google Drive to download from.
        source_path (str or pathlib.Path): The local path where the file will be placed.
        """
        raise NotImplementedError("Google Drive download has not been implemented yet.")

    def delete(self, path):
        """
        Deletes an object in google drive.

        Args:
        path (str): The path of the object to delete.
        """

        object_to_delete = self._find_file_by_path(path, not_exist_ok=True)
        if object_to_delete.exists:
            request = self._files.delete(fileId=object_to_delete.id)
            request.execute()

def initialize_cloud_storage(platform, platform_args):
    """
    Initialize cloud storage for a supported platform.

    Args:
    platform:  The cloud storage platform. Currently "S3" and "googledrive" are supported.
    platform_args: The arguments needed to initialize the platform.

    Return:
    (cloudstorage.Storage): A storage object that provides a simple interface to the cloud storage platform.
    """
    if platform == 's3' and _s3_support_enabled:
        return S3Storage(platform_args)
    if platform == "googledrive" and _google_drive_support_enabled:
        return GoogleDriveStorage(platform_args)
    else:
        raise RuntimeError(f"Cannot initialize cloud storage, '{platform}' is not a supported cloud URI scheme.")
