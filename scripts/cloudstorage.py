"""
Cloud Storage integration

.. include common links, assuming primary doc root is up one directory
.. include:: ../include/links.rst
"""

import urllib
import fnmatch
import os

try:
    import boto3
    import smart_open
    _s3_support_enabled = True
except:
    _s3_support_enabled = False

_supported_cloud_schemes = ['s3']
_s3_client = None

def initialize_cloud_storage(cloud_uri, args):
    scheme = urllib.parse.urlsplit(cloud_uri)[0]
    if scheme == 's3' and _s3_support_enabled:
        session = boto3.Session()
        global _s3_client
        _s3_client = session.client('s3', endpoint_url = args.endpoint_url)
    elif scheme == '':
        raise ValueError("Cannot initialize cloud storage, {cloud_uri} is not a URI")
    else:
        raise RuntimeError("Cannot initialize cloud storage, {scheme} is not a supported cloud URI scheme.")

def is_cloud_uri(potential_uri):
    scheme = urllib.parse.urlsplit(potential_uri)[0]
    if scheme in _supported_cloud_schemes:
        return True
    else:
        return False

def list_objects(cloud_uri, patterns):
    parsed_uri = urllib.parse.urlsplit(cloud_uri)    
    if parsed_uri.scheme == 's3' and _s3_client is not None:
        bucket = parsed_uri.netloc
        key = parsed_uri.path.lstrip('/')
        paginator = _s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            for object in page['Contents']:
                matches = False
                for pattern in patterns:
                    if fnmatch.fnmatchcase(object['Key'], pattern):
                        yield (parsed_uri.scheme + '://' + bucket + '/' + object['Key'], object['Size'])

    elif parsed_uri.scheme == '':
        raise ValueError("Cannot initialize cloud storage, {cloud_uri} is not a URI")
    else:
        raise RuntimeError("Cannot initialize cloud storage, {scheme} is not a supported cloud URI scheme.")

def open(cloud_uri,mode="r"):
    if _s3_support_enabled and _s3_client is not None:
        return smart_open.open(cloud_uri, mode, transport_params = {'client': _s3_client})
    else:
        raise RuntimeError(f"Cannot open {cloud_uri} from cloud storage because cloud storage has not been initialized")

def copy(source_uri, dest_uri):
    if _s3_client is None:
        raise RuntimeError(f"Cannot copy {source_uri} to {dest_uri}: S3 cloud storage has not been initialized.")
    parsed_source = urllib.parse.urlsplit(source_uri)    
    parsed_dest = urllib.parse.urlsplit(dest_uri)
    if parsed_source.scheme != 's3' or parsed_dest.scheme != 's3':
        raise ValueError(f"Cannot copy {source_uri} to {dest_uri}: only S3 to S3 copies are supported.")

    source = {'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')}
    _s3_client.copy(source, parsed_dest.netloc, parsed_dest.path.lstrip('/'))

def upload(source_file, dest_uri):
    if _s3_client is None:
        raise RuntimeError(f"Cannot upload {source_file} to {dest_uri}: S3 cloud storage has not been initialized.")
    parsed_dest = urllib.parse.urlsplit(dest_uri)
    if parsed_dest.scheme != 's3':
        raise ValueError(f"Cannot upload to {dest_uri}: only S3 uploads are supported.")

    _s3_client.upload_file(source_file, parsed_dest.netloc, parsed_dest.path.lstrip('/'))

def download(source_uri, dest_dir):
    if _s3_client is None:
        raise RuntimeError(f"Cannot download {source_uri} to {dest_dir}: S3 cloud storage has not been initialized.")
    parsed_source = urllib.parse.urlsplit(source_uri)
    if parsed_source.scheme != 's3':
        raise ValueError(f"Cannot download from {source_uri}: only S3 uploads are supported.")

    dest_file = os.path.join(dest_dir, os.path.basename(parsed_source.path))

    _s3_client.download_file(parsed_source.netloc, parsed_source.path.lstrip('/'), dest_file )
    return dest_file