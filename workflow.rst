Overall Workflow
================

This branch reduces Keck LRIS data pulled from KOA. It follows the same shape as the
DEIMOS workflow documented in ``workflow.rst`` on the ``main`` branch — a Google Sheet
drives a redis work queue, Nautilus jobs pop datasets off it, and results land in
Nautilus S3 and Google Drive — but four things differ enough to be worth stating up
front:

* Raw data comes straight from KOA rather than from a hand-organized disk, so there is
  no ``adap_reorg_setup.py`` step.
* Datasets are named "*target*/*date*/*instrument*", and the red and blue arms are
  separate datasets.
* Every job in `nautilus_jobs <nautilus_jobs>`_ runs the same container, built from
  `config/pypeit_lris_adap.docker <config/pypeit_lris_adap.docker>`_.
* The work queue is reached through a Kubernetes Service, so no yaml needs to be edited
  with a redis pod IP.

1.  Build and push the container.

    All of the jobs run one image, built from
    `config/pypeit_lris_adap.docker <config/pypeit_lris_adap.docker>`_. It carries the
    ``pypeit_env`` virtualenv, a PypeIt checkout of the ``lris_adap`` branch, rclone,
    the aws CLI, and every package the adap scripts import, so the jobs install nothing
    of their own except a PypeIt reinstall after they update its checkout.

    The awscli download in the Dockerfile is x86_64 only, so build for the Nautilus
    nodes explicitly::

        docker build --platform linux/amd64 \
            -t gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023 \
            -f config/pypeit_lris_adap.docker .

        docker push gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023

    The jobs use ``imagePullPolicy: Always``, so re-pushing the same tag is picked up on
    the next pod. Note that the reinstall line in each yaml is
    ``pip install --no-build-isolation -e '.[dev]'``; without that flag pip builds
    against an unpatched ``vcs_versioning`` and hits the bug the Dockerfile patches.

2.  Download the raw data from KOA.

    The target list is a text file with one ``<name> <ra> <dec>`` per line, ra and dec in
    degrees; blank lines and lines starting with ``#`` are ignored. Upload it where the
    job expects it::

        aws --endpoint $ENDPOINT_URL s3 cp targets.txt s3://pypeit/adap_2023/koa_to_download/targets.txt

    Then run the download job, which searches KOA for each target and fetches the
    matching science, arc, and flat frames::

        kubectl create -f nautilus_jobs/adap_koa_download.yml

    `download_lib <scripts/download_lib>`_ organizes what it finds into::

        <target>/<YYYYMMDD>/LRIS/raw_r
        <target>/<YYYYMMDD>/LRISBLUE/raw_b

    and the job uploads that tree to ``s3://pypeit/adap_2023/raw_data_reorg/``, which is
    the root every later stage reads from (see ``get_cloud_path`` in
    `scripts/rclone.py <scripts/rclone.py>`_).

    A **dataset** on this branch is therefore a three-part path — target, UT date,
    instrument — for example::

        J1030+0524/20120415/LRIS
        J1030+0524/20120415/LRISBLUE

    The instrument component must be ``LRIS`` or ``LRISBLUE``; it selects the raw
    subdirectory (``raw_r`` or ``raw_b``), and combined with the date it selects the
    PypeIt spectrograph via ``get_lris_spec_name`` in
    `scripts/extended_spec_mixins.py <scripts/extended_spec_mixins.py>`_
    (``keck_lris_red_orig``, ``keck_lris_red``, ``keck_lris_red_mark4``,
    ``keck_lris_blue_orig``, or ``keck_lris_blue``). Only public KOA data is reachable;
    nothing here logs in for proprietary data.

3.  Deploy the scripts and config to S3.

    **The git checkout is not what runs.** Every job clones this repository and then
    overwrites ``scripts/`` and ``config/`` from S3::

        aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/scripts_2023/ scripts/ --recursive
        aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/config_2023/  config/  --recursive

    So editing a script here has no effect on the cloud until it is pushed the other
    way::

        aws --endpoint $ENDPOINT_URL s3 cp --no-progress scripts/ s3://pypeit/adap/scripts_2023/ --recursive
        aws --endpoint $ENDPOINT_URL s3 cp --no-progress config/  s3://pypeit/adap/config_2023/  --recursive

    Some files only exist in S3. ``config/exclude_files.txt``, read by
    `trimming_setup.py <scripts/trimming_setup.py>`_ to drop bad raw frames, is one of
    them.

4.  Initial configuration.

    `trimming_setup.py <scripts/trimming_setup.py>`_ generates the ``.pypeit`` files for
    a dataset. It starts from a per-spectrograph default::

        config/keck_lris_red_default_pypeit_config
        config/keck_lris_red_mark4_default_pypeit_config
        config/keck_lris_blue_default_pypeit_config

    and writes the setup into a ``reduce`` subdirectory of the dataset.

    Per-dataset overrides are found by convention rather than configuration. The script
    globs ``config/`` for files whose name is the dataset with ``/`` replaced by ``_``,
    followed by a suffix, and the **last underscore-separated token of the filename
    becomes the output subdirectory**. That is why the default output directory is called
    ``reduce``: a tailored file named ``..._reduce.ini`` produces exactly the same
    subdirectory. A second file named ``..._alt.ini`` would produce a parallel ``alt``
    reduction of the same data.

    Two forms are accepted:

        ``.ini`` — parameters only. The file metadata section of the ``.pypeit`` file is
        still generated from the raw frames. See
        `HIRES_G191B2B_RED_C5_wg360_ech0_xd-0.24_2x2_2011-02-26_reduce.ini
        <config/HIRES_G191B2B_RED_C5_wg360_ech0_xd-0.24_2x2_2011-02-26_reduce.ini>`_ for
        the shape of one.

        any other suffix — a complete PypeIt input file. Only the path to the raw data is
        rewritten; everything else is used as given.

5.  Prepare the scorecard and the work queue tab.

    Reduction results are summarized into a Google Sheets scorecard:
    `scorecard.py <scripts/scorecard.py>`_ produces the rows and
    `update_gsheet_scorecard.py <scripts/update_gsheet_scorecard.py>`_ uploads them.

    The queue itself is a tab, named ``WorkQueue`` by default, with three columns:
    ``dataset``, ``status``, and ``pod``. Fill the dataset column with the datasets to
    process; the jobs write ``status`` and ``pod`` as they run, and only rows with a
    blank status are loaded into the queue. Two details are easy to trip over:

        **Datasets must start on row 4.** ``init_work_queue`` in
        `scripts/utils.py <scripts/utils.py>`_ begins reading there, so anything above
        row 4 is treated as headers.

        **The sheet is named on the command line** as
        ``[key=]<spreadsheet>/<worksheet>[@<status column>]``, where the key is the id
        from the spreadsheet URL and the status column defaults to ``B``. The pod name
        goes in the column to its right.

    The jobs in this directory do not all point at the same spreadsheet: the reduce jobs
    use ``key=15ealTQOBLB0I…``, sensfunc and flux use ``key=1TADKd3OgbA…``, and the
    coadd2d and scorecard jobs still say ``Scorecard/…``. Check the yaml you are about to
    apply.

6.  Start the work queue server.

    The queue is a redis instance in Nautilus. Both the Deployment and the Service that
    fronts it live in one file::

        kubectl apply -f nautilus_jobs/persist_volume.yml
        kubectl apply -f nautilus_jobs/workqueue_deployment.yml

    Every job then reaches it at ``redis://adap-workqueue:6379`` — unlike the 2019
    workflow, there is no pod IP to look up and paste into the yamls. Redis has no
    persistence configured, so restarting that pod discards the queue; the queue keys are
    ``adap_2023_q`` and ``adap_2023_lock``.

7.  Populate the queue.

    There is no ``load_nautilus_redis_queue.sh`` on this branch. Push directly with
    ``redis-cli`` inside the pod::

        POD=$(kubectl get pods -l k8s-app=adap-workqueue -o name)
        kubectl exec $POD -- redis-cli lpush adap_2023_q init

    The ``init`` sentinel tells the first pod that claims it to initialize the queue from
    the spreadsheet, marking each queued row ``IN QUEUE``. Alternatively, push dataset
    names directly to run a specific set without touching the sheet::

        kubectl exec $POD -- redis-cli lpush adap_2023_q J1030+0524/20120415/LRIS J1030+0524/20120415/LRISBLUE

    To inspect what is waiting::

        kubectl exec $POD -- redis-cli lrange adap_2023_q 0 -1

8.  Prepare the reduction yaml.

    Reductions are run by `reduce_from_queue.py <scripts/reduce_from_queue.py>`_ via
    `adap-reduce-lris-from-queue.yml <nautilus_jobs/adap-reduce-lris-from-queue.yml>`_.
    Before applying it, check:

        **parallelism** — the number of concurrent reductions. Each pod claims datasets
        until the queue is empty.

        **PypeIt branch** — the ``git checkout`` in the ``args`` section, normally
        ``lris_adap``. The commit actually used is captured in ``PYPEIT_COMMIT`` and
        recorded in the scorecard.

        **ephemeral storage** — the reduction runs entirely on the pod's ``emptyDir`` at
        ``/tmp``. LRIS datasets have needed 20 - 100 GiB; set requests and limits to
        match the datasets being run.

        **--scorecard_max_age** — how many days of scorecard rows the sheet update keeps.

        **pypeit_args** — anything after the four positional arguments is passed through
        to ``run_pypeit``, defaulting to ``-o``.

    The command line itself no longer needs editing:

    .. parsed-literal::

        python scripts/reduce_from_queue.py --rclone_conf config/rclone.conf
            --adap_root_dir /tmp/adap_root --scorecard_max_age 7
            key=15ealTQOBLB0I_BD-ZiN0lVeP1vt5P9oeGeiWOPN4bq0/WorkQueue
            **redis://adap-workqueue:6379** adap_2023 s3;

9.  Run the reduction.

    ::

        kubectl create -f nautilus_jobs/adap-reduce-lris-from-queue.yml

    Monitor with:

    .. parsed-literal::

        kubectl get pods
        kubectl logs -f *pod name from get pods*

    The pods also report progress in the ``WorkQueue`` tab, and each one copies its log
    to ``s3://pypeit/adap/results/<pod name>.log`` when it finishes.

    For each dataset a pod downloads the raw frames, runs ``trimming_setup.py``, runs
    ``run_pypeit`` on every generated ``.pypeit`` file while sampling peak memory, tars
    the QA directory, scores the result, uploads it, updates the scorecard, and deletes
    the local copy before claiming the next dataset.

10. Reduce a single dataset.

    For debugging one dataset, `adap-reduce-one.yml <nautilus_jobs/adap-reduce-one.yml>`_
    runs the same script and container with ``--dataset``, which skips the queue
    entirely. Set the ``DATASET`` environment variable in the yaml, then::

        kubectl create -f nautilus_jobs/adap-reduce-one.yml

    It does not touch the work queue status column, but it does still upload results and
    update the scorecard, overwriting any previous results for that dataset. It exits
    non-zero if the reduction fails and has ``backoffLimit: 0`` so the failure is not
    retried, and its whole run is teed to ``s3://pypeit/adap/results/<pod name>.log``.

11. Where the results land.

    Reductions are uploaded to both Nautilus S3 and Google Drive under the dataset::

        J1030+0524/20120415/LRIS/reduce

    That directory holds the PypeIt output, ``run_pypeit_stdout.txt``, ``QA.tar.gz``,
    ``scorecard.csv``, and a copy of the job log. A failed upload to Drive downgrades the
    dataset's status to ``WARNING`` rather than failing it.

12. Evaluate results and customize configuration.

    After looking at the reductions, add tailored config files under `config/ <config>`_
    using the naming convention from step 4, push them to
    ``s3://pypeit/adap/config_2023/``, blank out the status of the affected rows in the
    ``WorkQueue`` tab, and re-run. Re-running a dataset clears its previous ``reduce``
    directory in S3 first.

13. Re-score without re-reducing.

    When only the scorecard logic or its columns have changed,
    `run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ re-scores existing
    results in place::

        kubectl create -f nautilus_jobs/adap-run-scorecard-on-queue.yml

14. Generate sensitivity functions.

    `sensfunc_from_queue.py <scripts/sensfunc_from_queue.py>`_ picks standards out of the
    reduced data and builds sensitivity functions, driven by
    `config/sensfunc_config.ecsv <config/sensfunc_config.ecsv>`_, which lists the
    standard ids and extraction to use. Results go back into the dataset's ``reduce``
    directory as ``sens*`` files, replacing any earlier ones::

        kubectl create -f nautilus_jobs/adap-sensfunc-from-queue.yml

15. Flux calibrate and coadd 1D.

    `flux_coadd1d_from_queue.py <scripts/flux_coadd1d_from_queue.py>`_ fluxes the
    extracted spectra and coadds them. Like coadd2d below, it works at the dataset
    *prefix* level rather than on single datasets, and writes ``<prefix>/1D_Coadd``::

        kubectl create -f nautilus_jobs/adap_flux_codd1d_from_queue.yml

16. Coadd 2D.

    `coadd2d_from_queue.py <scripts/coadd2d_from_queue.py>`_ writes a ``2D_Coadd``
    directory::

        kubectl create -f nautilus_jobs/adap-coadd2d-queue.yml

    Coadding is done at a coarser level than reduction, so it uses a separate tab —
    ``coadd status`` — whose dataset column holds only a *prefix* of the dataset name,
    naming everything to be combined. Parameters for the ``.coadd2d`` files are resolved
    by ``get_reduce_params`` in `scripts/utils.py <scripts/utils.py>`_, which globs
    ``config/`` for the same dataset-prefix naming convention and falls back to
    ``config/default_pypeit_config``.

17. Back up to Google Drive.

    `sync_backup_from_queue.py <scripts/sync_backup_from_queue.py>`_ mirrors S3 to Drive
    for datasets on the queue::

        kubectl create -f nautilus_jobs/adap-sync-backups-from-queue.yml

    `backup_datasets.sh <scripts/backup_datasets.sh>`_, run by
    `backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_, does the same for an
    explicit list of datasets read from ``s3://pypeit/adap/scripts/backup_list.txt``.

18. Archive for KOA.

    `archive.py <scripts/archive.py>`_ flattens the reduction and coadd products into the
    layout KOA expects, alongside the metadata files described in
    `archive_README <scripts/archive_README>`_::

        python scripts/archive.py archive --copy <source> --report archive.report.txt

    There is no Nautilus job for this stage on this branch; the 2019 workflow's
    ``remote_archive.py`` and its yaml are only on ``main``.

Credentials
===========

Two Kubernetes secrets, a kubeconfig, and registry access are all that the jobs need.
They are documented separately, with the exact mount paths and file formats, in
`nautilus_jobs/CREDENTIALS.md <nautilus_jobs/CREDENTIALS.md>`_.

Deprecated
==========

Two queue scripts are kept for reference but are not part of this workflow. Both carry a
deprecation notice in their module docstring and log a warning if they are run.

* `stage_raw_data_from_queue.py <scripts/stage_raw_data_from_queue.py>`_, run by
  `adap-stage-raw-queue.yml <nautilus_jobs/adap-stage-raw-queue.yml>`_, staged raw data
  into ``raw_data_reorg`` from a KOA metadata inventory. The KOA download job in step 2
  now writes that tree directly, so there is nothing left to stage. Its command line has
  been moved to the redis work queue so it initializes if revived, but ``stage_task``
  still looks for raw files under ``<dataset>/complete/raw`` and reads the instrument
  from the first component of the dataset name, neither of which matches this branch's
  naming.

* `collate1d_from_queue.py <scripts/collate1d_from_queue.py>`_ was written for DEIMOS. It
  runs ``pypeit_collate_1d`` against ``config/default.collate1d``, which exists only on
  the DEIMOS branches, and no job in `nautilus_jobs <nautilus_jobs>`_ invokes it. On this
  branch 1D coadding is step 15, `flux_coadd1d_from_queue.py
  <scripts/flux_coadd1d_from_queue.py>`_.

Known rough edges
=================

* `coadd2d_from_queue.py <scripts/coadd2d_from_queue.py>`_ builds its S3 path as
  ``pypeit/adap/raw_data_reorg`` and its Drive path as ``backups/``, rather than going
  through ``get_cloud_path``, which uses ``pypeit/adap_2023/raw_data_reorg``. It reads
  from a different root than the reduce stage writes to, so it needs updating before the
  2D coadds will find this campaign's data.
* The ``--google_creds`` option that most scripts declare is never read. Google
  authentication always comes from ``$HOME/.config/gspread/service_account.json``.
* Redis has no password and no persistence. Anything in the namespace can read or drain
  the queue, and a restart of the pod loses it.
