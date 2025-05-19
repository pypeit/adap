""" Utilities for using rclone to access cloud locations"""
from pathlib import Path
import logging
from fnmatch import fnmatch

from utils import run_script

logger = logging.getLogger(__name__)

def get_cloud_path(args, source, adap="adap"):
    """Return the correct cloud enabled RClonePath for a given cloud provider.
    Args:
        args: The arguments to the script as returned by argparse
        source:  The cloud provider, currently either "gdrive" for Google Drive or "s3" for nautilus S3.
                 These are the remote names from rclone.conf.
    Return: An RClonePath for the cloud provider.
    """
    if source == "gdrive":
        source_loc = RClonePath(args.rclone_conf, "gdrive", "backups")
    else:
        source_loc = RClonePath(args.rclone_conf, "s3", "pypeit", adap, "raw_data_reorg")
    return source_loc

class RClonePath():
    """A :class:`pathlib.Path` like class for accessing remote cloud locations with rclone
    
    Args:
        rclone_conf (str or :obj:`pathlib.Path`): The rclone configuration file to use.
        service (str): The service from the configuration file to use. (e.g. s3, gdrive)
        path_components (list of str or :obj:`pathlib.Path`): The path components that make up the path.

    """
    def __init__(self, rclone_conf, service, *path_components):
        self.service=service
        if service not in ['s3', 'gdrive']:
            raise ValueError(f"Unknown service {service}")
        self.rclone_config = str(rclone_conf)

        self.path = Path(*path_components)

    def ls(self, recursive=False):
        paths_to_search = [self.path]
        combined_results = []
        while len(paths_to_search) != 0:
            path = paths_to_search.pop()
            results = run_script(["rclone", '--config', self.rclone_config, 'lsf', self.service + ":" + str(path)], return_output=True)
            for result in results:                    
                if recursive and result.endswith("/"):
                    paths_to_search.append(path / result)
                combined_results.append(RClonePath(self.rclone_config, self.service, path, result))
        return combined_results

    def glob(self, pattern):
        return [rp for rp in self.ls(False) if fnmatch(rp.path.name, pattern)]

    def rglob(self, pattern):
        return [rp for rp in self.ls(True) if fnmatch(rp.path.name, pattern)]

    def _copy(self, source, dest):
        # Run rclone copy with nice looking progress
        run_script(["rclone", '--config', self.rclone_config,  'copy', '-P', '--stats-one-line', '--stats', '60s', '--stats-unit', 'bits', '--retries-sleep', '60s', str(source), str(dest)])

    def unlink(self):
        run_script(["rclone", '--config', self.rclone_config,  'delete', str(self)], log_output=True)

    def download(self, dest):        
        logger.info(f"Downloading {self} to {dest}")
        self._copy(self, dest)

    def upload(self, source):
        logger.info(f"Uploading {source} to {self}")
        self._copy(source, self)

    def sync_from(self, path):
        logger.info(f"Syncing {self} from {path}")
        run_script(["rclone", '--config', self.rclone_config,  'sync', '-P', '--stats-one-line', '--stats', '60s', '--stats-unit', 'bits', str(path), str(self)], log_output=True)                

    def __str__(self):
        return f"{self.service}:{self.path}"
    
    def __truediv__(self, other):
        if isinstance(other, RClonePath):
            if self.rclone_config != other.rclone_config:
                raise ValueError("Cannot combine rclone paths with different configurations.")
            if self.service != other.service:
                raise ValueError("Cannot combine rclone paths from different services.")

            return RClonePath(self.rclone_config, self.service, self.path, other.path)
        else:
            return RClonePath(self.rclone_config, self.service, self.path, other)

    def __getattr__(self, name):
        if name in ["root", "parents", "parent", "name", "suffix", "suffixes", "stem"]:
            return getattr(self.path, name)
        else:
            return getattr(super(), name)