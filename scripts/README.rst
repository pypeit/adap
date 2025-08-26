

Script Description
==================

See the `Overall Workflow <../workflow.rst>`_ documentation for an overview of
how these scripts are used.

Queue Scripts
-------------

These scripts run in the Nautilus kubernetes cluster and pull tasks from a queue.

``test_from_queue.py``
    Delays for a random amount of time for each task on the work queue. Useful for testing the workqueue framework.

``reduce_from_queue.py``
    Runs PypeIt to reduce datastes on a work queue.

``colalte1d_from_queue.py``
    Runs ``pypeit_collate1d`` to flux and perform 1D coadding.

``coadd2d_from_queue.py``
    Runs ``pypeit_setup_coadd2d`` and ``pypeit_coadd_2dspec`` to do 2D coadding.

``run_scorecard_on_queue.py``
    Re-runs the scorecard on each item in the queue and updates the
    scorecard spreadsheet with the results.

``sync_backups_from_queue.py``
    Re-syncs the S3 data to the shared Google Drive backups.

``trimming_setup_from_queue.py``
    Runs ``trimming_setup.py`` on each dataset without doing any reductions.

Preparing Data
---------------

``adap_reorg_setup.py``
    Reorganizes raw data to the standard directory structure.

``download_dataset.sh``
    Downloads a dataset from S3 with options to only download logs, or everything including raw files.

``fix_headers.py``
    Fixes headers for a few known bad FITS files. 

Managing Nautilus Work Queues
-----------------------------

``workqueue_deployment.yml``
    Deploys the `Redis <https://redis.io/>`_ work queue server.

``load_nautilus_redis_queue.sh``
    Loads datasets onto a work queue on the Redis server.


Helper Scripts
--------------

``trimming_setup.py``
    Generates a PypeIt setup file from raw data, being careful to trim extra calibration frames
    that could confuse the reduction.

``useful_warnings.py``
    Finds interesting warning messages from the logs and loads them to a separate file.

``scorecard.py``
    Generates scorecard CSV files to evaluate the quality of a reduction.

``update_gsheet_scorecard.py``
    Updates the Scorecard Google Sheet with the information from a scorecard CSV file.

``tar_qa.sh``
    Tars up the QA PNGs to limit the number of files that need to be transferred back and forth.

``run_pypeit_parallel.py``
    Runs pypeit in parallel on any .pypeit files it finds.


Archiving
---------

``local_archive.py``
    Re-organizes reduced data for delivery to KOA. This script works on files in a local
    file system.

``remote_archive.py``
    Re-organizes reduced data for delivery to KOA. This script runs in nautilus and downloads 
    datasets one at a time from a work queue to reorganize them in Google Drive.

``display_nautilus_redis_hash.sh``
    View a Redis hash table. Used to monitor the status of remote_archive.py.


Libraries
---------

These python modules are used by other scripts.

``utils.py``
    Utility methods related to running tasks from a redis work queue.

``rclone.py``
    A wrapper that allows using the `rclone <https://rclone.org/>`_ utility in a similar way
    to a Python Path object.

``gspread_utils.py``
    Utilities for working with the `gspread <https://pypi.org/project/gspread/>`_ package used for working with
    Google Sheets.

``archive.py``
    The common code used to create archive metadata for KOA. shared
    between ``local_archive.py`` and ``remote_archive.py``.

