Depreciated and unused components
=================================

These files are in the repository but are not steps above. Nothing here needs to be run.

Superseded by the redis queue
-----------------------------

Before the queue moved to redis, the dataset list was downloaded from the sheet into a
CSV on a shared persistent volume, and pods locked that file to claim work. That path is
still checked in but is inert: nothing reads the CSV it produces.

* `download_work_queue_from_gs.py <scripts/download_work_queue_from_gs.py>`_, run by
  `init_workqueue.yml <nautilus_jobs/init_workqueue.yml>`_ and
  `refresh_workqueue.yml <nautilus_jobs/refresh_workqueue.yml>`_, writes
  ``/work_queue/work_queue_2023.csv`` on the ``pypeit-adap-work-queue`` PVC.
  `upload_workqueue_to_s3.yml <nautilus_jobs/upload_workqueue_to_s3.yml>`_ copies that CSV
  to S3.

  Note that these jobs do still write ``IN QUEUE`` back into the spreadsheet as a side
  effect of reading it, so running one will mark rows queued that no redis queue knows
  about. Use the ``init`` sentinel in
  `Populate the queue <workflow.rst#populate-the-queue>`_ instead.

* `persist_volume.yml <nautilus_jobs/persist_volume.yml>`_ declares that PVC. It is still
  required, because `adap-coadd2d-queue.yml <nautilus_jobs/adap-coadd2d-queue.yml>`_,
  `adap-run-scorecard-on-queue.yml <nautilus_jobs/adap-run-scorecard-on-queue.yml>`_,
  `adap-sync-backups-from-queue.yml <nautilus_jobs/adap-sync-backups-from-queue.yml>`_ and
  `adap-stage-raw-queue.yml <nautilus_jobs/adap-stage-raw-queue.yml>`_ all still mount it
  even though none of them read it.

A second, older container
-------------------------

`kube_tests/pypeit.docker <kube_tests/pypeit.docker>`_ is an earlier image definition,
superseded by `config/pypeit_lris_adap.docker <config/pypeit_lris_adap.docker>`_ and
referenced by no job. It builds on Ubuntu 20.04 with a Python 3.8 Miniconda, clones
PypeIt's default branch rather than ``lris_adap``, pushes to ``profxj/pypeit`` on Docker
Hub rather than the Nautilus registry, and installs neither rclone nor redis, gspread,
boto3 or pykoa — so it cannot run the adap scripts at all.

Deprecated scripts
------------------

All three live under ``scripts/depreciated/`` and carry a deprecation notice in their
module docstring. None of them run as they stand: moving them out of ``scripts/``
means their bare imports no longer resolve, on top of the problems noted below.

* `coadd2d_from_queue.py <scripts/depreciated/coadd2d_from_queue.py>`_ ran
  ``pypeit_setup_coadd2d`` and ``pypeit_coadd_2dspec`` over a ``coadd status`` tab whose
  entries were dataset *prefixes*, writing a ``2D_Coadd`` directory. **2D coaddition is
  not part of the current workflow**, which coadds in 1D only with
  `Flux calibrate and coadd 1D <workflow.rst#flux-calibrate-and-coadd-1d>`_, so the
  script has moved to ``scripts/depreciated/`` and
  `adap-coadd2d-queue.yml <nautilus_jobs/adap-coadd2d-queue.yml>`_ is not applied.

  Three things would need fixing to revive it. Its ``from utils import ... RClonePath``
  raises ``ImportError``, because ``RClonePath`` lives in
  `scripts/rclone.py <scripts/rclone.py>`_ and ``utils.py`` neither defines nor
  re-exports it. It builds its S3 path as ``pypeit/adap/raw_data_reorg`` and its Drive
  path as ``backups/`` instead of going through ``get_cloud_path``, so it reads a
  different root than the reduce stage writes to. And from its new subdirectory its bare
  imports no longer resolve, since only ``scripts/`` is on ``sys.path``.

  It is also the only caller of ``get_reduce_params`` in
  `scripts/utils.py <scripts/utils.py>`_, whose fallback to ``config/default_pypeit_config``
  names a file that no longer exists — so that function has no live caller either.

* `stage_raw_data_from_queue.py <scripts/depreciated/stage_raw_data_from_queue.py>`_, run by
  `adap-stage-raw-queue.yml <nautilus_jobs/adap-stage-raw-queue.yml>`_, staged raw data
  into ``raw_data_reorg`` from a KOA metadata inventory. The KOA download job now writes
  that tree directly, so there is nothing left to stage. Its command line has been moved
  to the redis work queue so it initializes if revived, but ``stage_task`` still looks for
  raw files under ``<dataset>/complete/raw`` and reads the instrument from the first
  component of the dataset name, neither of which matches this branch's naming.

* `collate1d_from_queue.py <scripts/depreciated/collate1d_from_queue.py>`_ was written for DEIMOS. It
  runs ``pypeit_collate_1d`` against ``config/default.collate1d``, which exists only on
  the DEIMOS branches, and no job in `nautilus_jobs <nautilus_jobs>`_ invokes it. On this
  branch 1D coadding is
  `Flux calibrate and coadd 1D <workflow.rst#flux-calibrate-and-coadd-1d>`_.

Known rough edges
=================

Nothing here is a defect in the checked-in files any more; these are two constraints of
the setup that are worth knowing before changing it.

* Google authentication always comes from
  ``$HOME/.config/gspread/service_account.json``, gspread's built-in default. There is no
  option to point it elsewhere, so ``$HOME`` has to be right in any container that runs
  these scripts.
* Redis has no password and no persistence. Anything in the namespace can read or drain
  the queue, and a restart of the pod loses it.

