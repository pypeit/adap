Overall Workflow
================

This branch reduces Keck LRIS data pulled from KOA. It follows the same shape as the
DEIMOS workflow documented in ``workflow.rst`` on the ``main`` branch — a Google Sheet
drives a redis work queue, Nautilus jobs pop datasets off it, and results land in
Nautilus S3 and Google Drive — but three things differ enough to be worth stating up
front:

* Datasets are named "*target*/*date*/*instrument*", and the red and blue arms are
  separate datasets.
* Every job in `nautilus_jobs <nautilus_jobs>`_ runs the same container, built from
  `config/pypeit_lris_adap.docker <config/pypeit_lris_adap.docker>`_.
* The work queue is reached through a Kubernetes Service, so no yaml needs to be edited
  with a redis pod IP.

The workflow below is in three parts: things done once when the cluster is set up, the
loop that reduces data, and the post-processing stages that run after a reduction is
good. `Known rough edges`_ at the end lists the places where the checked-in yamls do not
yet match this description — read it before applying anything.

Part 1 — One-time setup
=======================

Build and push the container
----------------------------

All of the jobs run one image, built from
`config/pypeit_lris_adap.docker <config/pypeit_lris_adap.docker>`_. It carries the
``pypeit_env`` virtualenv, a PypeIt checkout of the ``lris_adap`` branch, rclone, the aws
CLI, and every package the adap scripts import, so the jobs install nothing of their own
except a PypeIt reinstall after they update its checkout.

The awscli download in the Dockerfile is x86_64 only, so build for the Nautilus nodes
explicitly::

    docker build --platform linux/amd64 \
        -t gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023 \
        -f config/pypeit_lris_adap.docker .

    docker push gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023

The jobs use ``imagePullPolicy: Always``, so re-pushing the same tag is picked up on the
next pod. Note that the reinstall line in each yaml is
``pip install --no-build-isolation -e '.[dev]'``; without that flag pip builds against an
unpatched ``vcs_versioning`` and hits the bug the Dockerfile patches.

Two details of the image are worth knowing. ``psutil``, which
`reduce_from_queue.py <scripts/reduce_from_queue.py>`_ uses to sample how much memory a
reduction consumes, is not in the Dockerfile's own pip list — it arrives only through
PypeIt's ``[dev]`` extra, so the reduce stage rests on a PypeIt *development* dependency.
And the image carries its own ``adap`` clone, with ``WORKDIR`` pointing at it, that no job
uses: every job clones ``adap`` fresh under ``/tmp/adap_root``. That baked-in copy is
frozen at image build time, so an interactive shell in the container starts out in stale
code.

Set up the Google Sheet
-----------------------

One Google spreadsheet, named ``Scorecard``, drives the whole pipeline. It is both the
input — the list of datasets to process — and the output — per-dataset status and the
scorecard metrics. Each stage gets its own tab of that one spreadsheet, and every job
addresses it **by name**::

    Scorecard/WorkQueue
    Scorecard/coadd status

`google_sheet_setup.rst <google_sheet_setup.rst>`_ documents the tabs it needs, the
columns in each, and how to build one from scratch. The essentials:

    **The tabs.** ``WorkQueue`` holds the dataset list and status; ``latest``, ``Failed``
    and ``LRIS`` receive the scorecard; ``coadd status`` is the queue for the 2D coadd
    stage, whose entries are dataset *prefixes* rather than datasets.

    **Datasets must start on row 4.** ``init_work_queue`` in
    `scripts/utils.py <scripts/utils.py>`_ begins reading there, so anything above row 4
    is treated as headers.

    **The sheet is named on the command line** as
    ``<spreadsheet>/<worksheet>[@<status column>]``, where the status column defaults to
    ``B``. The pod name goes in the column to its right. Only rows with a blank status are
    loaded into the queue.

The scorecard updater derives the scorecard tabs from whatever spreadsheet the running
job was handed, so a stage pointed at a different spreadsheet writes its status somewhere
nobody is looking. Several checked-in yamls still carry ``key=<id>/WorkQueue`` arguments
naming two other spreadsheets; see `Known rough edges`_.

Deploy the scripts and config to S3
-----------------------------------

**The git checkout is not what runs.** Every job clones this repository and then
overwrites ``scripts/`` and ``config/`` from S3::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/scripts_2023/ scripts/ --recursive
    aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/config_2023/  config/  --recursive

So editing a script here has no effect on the cluster until it is pushed the other way::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress scripts/ s3://pypeit/adap/scripts_2023/ --recursive
    aws --endpoint $ENDPOINT_URL s3 cp --no-progress config/  s3://pypeit/adap/config_2023/  --recursive

Do this before every run in which a script or config file changed. Some files only exist
in S3 — ``config/exclude_files.txt``, read by
`trimming_setup.py <scripts/trimming_setup.py>`_ to drop bad raw frames, is one of them.

Note the two S3 prefix families, which are easy to confuse: ``s3://pypeit/adap/`` holds
the deployed scripts, config, and job logs, while ``s3://pypeit/adap_2023/`` holds this
campaign's raw data and the KOA target list.

Start the work queue
--------------------

The queue is a redis instance in Nautilus. Both the Deployment and the Service that
fronts it live in one file::

    kubectl apply -f nautilus_jobs/persist_volume.yml
    kubectl apply -f nautilus_jobs/workqueue_deployment.yml

Every job then reaches it at ``redis://adap-workqueue:6379`` — unlike the 2019 workflow,
there is no pod IP to look up and paste into the yamls. Redis has no persistence
configured, so restarting that pod discards the queue; the queue keys are ``adap_2023_q``
and ``adap_2023_lock``.

The ``persist_volume.yml`` PVC is applied first not because the queue uses it — redis
holds the queue in memory — but because several job yamls still mount it at
``/work_queue`` and their pods will not schedule if the claim does not exist. Nothing
reads what is in it; see `Known rough edges`_.

Part 2 — Running a reduction campaign
=====================================

Download the raw data from KOA
------------------------------

The target list is a text file with one ``<name> <ra> <dec>`` per line, ra and dec in
degrees; blank lines and lines starting with ``#`` are ignored. Upload it where the job
expects it::

    aws --endpoint $ENDPOINT_URL s3 cp targets.txt s3://pypeit/adap_2023/koa_to_download/targets.txt

Then run the download job, which searches KOA for each target and fetches the matching
science, arc, and flat frames::

    kubectl create -f nautilus_jobs/adap_koa_download.yml

`download_lib <scripts/download_lib>`_ organizes what it finds into::

    <target>/<YYYYMMDD>/LRIS/raw_r
    <target>/<YYYYMMDD>/LRISBLUE/raw_b

and the job uploads that tree to ``s3://pypeit/adap_2023/raw_data_reorg/``, which is the
root every later stage reads from (see ``get_cloud_path`` in
`scripts/rclone.py <scripts/rclone.py>`_).

A **dataset** on this branch is therefore a three-part path — target, UT date,
instrument — for example::

    J1030+0524/20120415/LRIS
    J1030+0524/20120415/LRISBLUE

The instrument component must be ``LRIS`` or ``LRISBLUE``; it selects the raw
subdirectory (``raw_r`` or ``raw_b``), and combined with the date it selects the PypeIt
spectrograph via ``get_lris_spec_name`` in
`scripts/extended_spec_mixins.py <scripts/extended_spec_mixins.py>`_
(``keck_lris_red_orig``, ``keck_lris_red``, ``keck_lris_red_mark4``,
``keck_lris_blue_orig``, or ``keck_lris_blue``). Only public KOA data is reachable;
nothing here logs in for proprietary data.

Reduction configuration
-----------------------

`trimming_setup.py <scripts/trimming_setup.py>`_ generates the ``.pypeit`` files for a
dataset. It starts from a per-spectrograph default::

    config/keck_lris_red_default_pypeit_config
    config/keck_lris_red_mark4_default_pypeit_config
    config/keck_lris_blue_default_pypeit_config

and writes the setup into a ``reduce`` subdirectory of the dataset.

Per-dataset overrides are found by convention rather than configuration. The script globs
``config/`` for files whose name is the dataset with ``/`` replaced by ``_``, followed by
a suffix, and the **last underscore-separated token of the filename becomes the output
subdirectory**. That is why the default output directory is called ``reduce``: a tailored
file named ``..._reduce.ini`` produces exactly the same subdirectory. A second file named
``..._alt.ini`` would produce a parallel ``alt`` reduction of the same data.

Two forms are accepted:

    ``.ini`` — parameters only, in PypeIt's parameter-block syntax. The file metadata
    section of the ``.pypeit`` file is still generated from the raw frames. A tailored
    file for one dataset is named, for example,
    ``J1030+0524_20120415_LRIS_reduce.ini``.

    any other suffix — a complete PypeIt input file. Only the path to the raw data is
    rewritten; everything else is used as given.

Only the per-spectrograph defaults are checked in. Per-dataset override files live in
``s3://pypeit/adap/config_2023/`` and are not in this repository, so anything added under
`config/ <config>`_ has to be pushed there before it takes effect — and the overrides
already in use are only visible by listing that prefix::

    aws --endpoint $ENDPOINT_URL s3 ls s3://pypeit/adap/config_2023/

Populate the queue
------------------

There is no ``load_nautilus_redis_queue.sh`` on this branch. Push directly with
``redis-cli`` inside the pod::

    POD=$(kubectl get pods -l k8s-app=adap-workqueue -o name)
    kubectl exec $POD -- redis-cli lpush adap_2023_q init

The ``init`` sentinel tells the first pod that claims it to initialize the queue from the
spreadsheet, marking each queued row ``IN QUEUE``. Alternatively, push dataset names
directly to run a specific set without touching the sheet::

    kubectl exec $POD -- redis-cli lpush adap_2023_q J1030+0524/20120415/LRIS J1030+0524/20120415/LRISBLUE

To inspect what is waiting::

    kubectl exec $POD -- redis-cli lrange adap_2023_q 0 -1

Run the reduction
-----------------

Reductions are run by `reduce_from_queue.py <scripts/reduce_from_queue.py>`_ via
`adap-reduce-lris-from-queue.yml <nautilus_jobs/adap-reduce-lris-from-queue.yml>`_.
Before applying it, check:

    **parallelism** — the number of concurrent reductions. Each pod claims datasets until
    the queue is empty.

    **PypeIt branch** — the ``git checkout`` in the ``args`` section, normally
    ``lris_adap``. The commit actually used is captured in ``PYPEIT_COMMIT`` and recorded
    in the scorecard.

    **ephemeral storage** — the reduction runs entirely on the pod's ``emptyDir`` at
    ``/tmp``. LRIS datasets have needed 20 - 100 GiB; set requests and limits to match the
    datasets being run.

    **--scorecard_max_age** — how many days of scorecard rows the sheet update keeps.

    **pypeit_args** — anything after the four positional arguments is passed through to
    ``run_pypeit``, defaulting to ``-o``.

The command line itself no longer needs editing:

.. parsed-literal::

    python scripts/reduce_from_queue.py --rclone_conf config/rclone.conf
        --adap_root_dir /tmp/adap_root --scorecard_max_age 7
        **Scorecard/WorkQueue** **redis://adap-workqueue:6379** adap_2023 s3

Then::

    kubectl create -f nautilus_jobs/adap-reduce-lris-from-queue.yml

Monitor with:

.. parsed-literal::

    kubectl get pods
    kubectl logs -f *pod name from get pods*

The pods also report progress in the ``WorkQueue`` tab, and each one copies its log to
``s3://pypeit/adap/results/<pod name>.log`` when it finishes.

For each dataset a pod downloads the raw frames, runs ``trimming_setup.py``, runs
``run_pypeit`` on every generated ``.pypeit`` file while sampling peak memory, tars the QA
directory, scores the result, uploads it, updates the scorecard, and deletes the local
copy before claiming the next dataset.

Reduce a single dataset
-----------------------

For debugging one dataset, `adap-reduce-one.yml <nautilus_jobs/adap-reduce-one.yml>`_ runs
the same script and container with ``--dataset``, which skips the queue entirely. Set the
``DATASET`` environment variable in the yaml, then::

    kubectl create -f nautilus_jobs/adap-reduce-one.yml

It does not touch the work queue status column, but it does still upload results and
update the scorecard, overwriting any previous results for that dataset. It exits non-zero
if the reduction fails and has ``backoffLimit: 0`` so the failure is not retried, and its
whole run is teed to ``s3://pypeit/adap/results/<pod name>.log``.

Where the results land
----------------------

Reductions are uploaded to both Nautilus S3 and Google Drive under the dataset::

    J1030+0524/20120415/LRIS/reduce

That directory holds the PypeIt output, ``run_pypeit_stdout.txt``, ``QA.tar.gz``,
``scorecard.csv``, and a copy of the job log. A failed upload to Drive downgrades the
dataset's status to ``WARNING`` rather than failing it.

Iterate
-------

After looking at the reductions, add tailored config files under `config/ <config>`_ using
the convention in `Reduction configuration`_, push them to
``s3://pypeit/adap/config_2023/``, blank out the status of the affected rows in the
``WorkQueue`` tab, and re-run from `Populate the queue`_. Re-running a dataset clears its
previous ``reduce`` directory in S3 first.

Part 3 — Post-processing
========================

Each of these is a queue-driven job on the same ``run_task_on_queue`` loop as the
reduction, so the queue is populated and monitored exactly as above.

Re-score without re-reducing
----------------------------

When only the scorecard logic or its columns have changed,
`run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ re-scores existing
results in place::

    kubectl create -f nautilus_jobs/adap-run-scorecard-on-queue.yml

Generate sensitivity functions
------------------------------

`sensfunc_from_queue.py <scripts/sensfunc_from_queue.py>`_ picks standards out of the
reduced data and builds sensitivity functions, driven by
`config/sensfunc_config.ecsv <config/sensfunc_config.ecsv>`_, which lists the standard ids
and extraction to use. Results go back into the dataset's ``reduce`` directory as
``sens*`` files, replacing any earlier ones::

    kubectl create -f nautilus_jobs/adap-sensfunc-from-queue.yml

Flux calibrate and coadd 1D
---------------------------

`flux_coadd1d_from_queue.py <scripts/flux_coadd1d_from_queue.py>`_ fluxes the extracted
spectra and coadds them. Like the 2D coadd below, it works at the dataset *prefix* level
rather than on single datasets, and writes ``<prefix>/1D_Coadd``::

    kubectl create -f nautilus_jobs/adap_flux_codd1d_from_queue.yml

Coadd 2D
--------

`coadd2d_from_queue.py <scripts/coadd2d_from_queue.py>`_ writes a ``2D_Coadd``
directory::

    kubectl create -f nautilus_jobs/adap-coadd2d-queue.yml

Coadding is done at a coarser level than reduction, so it uses a separate tab —
``coadd status`` — whose dataset column holds only a *prefix* of the dataset name, naming
everything to be combined. Parameters for the ``.coadd2d`` files are resolved by
``get_reduce_params`` in `scripts/utils.py <scripts/utils.py>`_, which globs ``config/``
for the same dataset-prefix naming convention. **Every prefix therefore needs a matching
custom config file**: the fallback for a prefix with no match is
``config/default_pypeit_config``, which no longer exists. See `Known rough edges`_.

Back up to Google Drive
-----------------------

`sync_backup_from_queue.py <scripts/sync_backup_from_queue.py>`_ mirrors S3 to Drive for
datasets on the queue::

    kubectl create -f nautilus_jobs/adap-sync-backups-from-queue.yml

`backup_datasets.sh <scripts/backup_datasets.sh>`_, run by
`backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_, does the same for an explicit
list of datasets read from ``s3://pypeit/adap/scripts/backup_list.txt``.

Archive for KOA
---------------

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

Not part of this workflow
=========================

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
  about. Use the ``init`` sentinel in `Populate the queue`_ instead.

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

Both carry a deprecation notice in their module docstring and log a warning if they are
run.

* `stage_raw_data_from_queue.py <scripts/stage_raw_data_from_queue.py>`_, run by
  `adap-stage-raw-queue.yml <nautilus_jobs/adap-stage-raw-queue.yml>`_, staged raw data
  into ``raw_data_reorg`` from a KOA metadata inventory. The KOA download job now writes
  that tree directly, so there is nothing left to stage. Its command line has been moved
  to the redis work queue so it initializes if revived, but ``stage_task`` still looks for
  raw files under ``<dataset>/complete/raw`` and reads the instrument from the first
  component of the dataset name, neither of which matches this branch's naming.

* `collate1d_from_queue.py <scripts/collate1d_from_queue.py>`_ was written for DEIMOS. It
  runs ``pypeit_collate_1d`` against ``config/default.collate1d``, which exists only on
  the DEIMOS branches, and no job in `nautilus_jobs <nautilus_jobs>`_ invokes it. On this
  branch 1D coadding is `Flux calibrate and coadd 1D`_.

Known rough edges
=================

Places where the checked-in files do not match the workflow above. Each needs a change to
a yaml or a script, not to this document.

Two jobs cannot be applied as checked in
----------------------------------------

Kubernetes object names must be DNS-1123 labels, which do not allow underscores. Two
job names have one::

    nautilus_jobs/init_workqueue.yml:4              name: adap_2023-init-workqueue
    nautilus_jobs/adap_flux_codd1d_from_queue.yml:4 name: adap_2023-flux-from-queue

``kubectl create`` rejects both outright, so `Flux calibrate and coadd 1D`_ cannot run
until that name is fixed. (The filename's ``codd1d`` typo is harmless but worth renaming
at the same time.)

Three spreadsheets instead of one
---------------------------------

Every job should be pointed at ``Scorecard/<tab>``, as in `Set up the Google Sheet`_. As
checked in, they are split three ways:

=============================================  ===================================
Job                                            Sheet argument
=============================================  ===================================
adap-reduce-lris-from-queue.yml                ``key=15ealTQOBLB0I…/WorkQueue``
adap-reduce-from-queue.yml                     ``key=15ealTQOBLB0I…/WorkQueue``
adap-reduce-one.yml                            ``key=15ealTQOBLB0I…/WorkQueue``
adap-sensfunc-from-queue.yml                   ``key=1TADKd3OgbA…/WorkQueue``
adap_flux_codd1d_from_queue.yml                ``key=1TADKd3OgbA…/WorkQueue``
init_workqueue.yml, refresh_workqueue.yml      ``key=1TADKd3OgbA…/WorkQueue``
adap-stage-raw-queue.yml                       ``key=1TADKd3OgbA…/WorkQueue@B``
adap-run-scorecard-on-queue.yml                ``Scorecard/WorkQueue``
adap-sync-backups-from-queue.yml               ``Scorecard/WorkQueue``
adap-coadd2d-queue.yml                         ``Scorecard/coadd status``
=============================================  ===================================

Because the scorecard tabs are resolved from whichever spreadsheet the running job was
given, a reduction and a post-processing stage pointed at different spreadsheets write
their status into different sheets. Settle on ``Scorecard`` and make the yamls agree
before a campaign.

A duplicate reduce job
----------------------

`adap-reduce-from-queue.yml <nautilus_jobs/adap-reduce-from-queue.yml>`_ and
`adap-reduce-lris-from-queue.yml <nautilus_jobs/adap-reduce-lris-from-queue.yml>`_ are
identical except that the former misspells the variable as ``PYPEIT_COMMMIT`` — three
``M``\ s — in both of its ``echo`` lines, so the PypeIt commit is logged as an empty
string. Use the ``lris`` one; the other should be deleted.

Two S3 script prefixes
----------------------

`init_workqueue.yml <nautilus_jobs/init_workqueue.yml>`_ and
`backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_ pull their scripts from
``s3://pypeit/adap/scripts/``, the un-suffixed DEIMOS-era prefix, while every other job
uses ``s3://pypeit/adap/scripts_2023/``. Deploying as described in
`Deploy the scripts and config to S3`_ does not update what those two jobs run.

Other
-----

* `coadd2d_from_queue.py <scripts/coadd2d_from_queue.py>`_ builds its S3 path as
  ``pypeit/adap/raw_data_reorg`` and its Drive path as ``backups/``, rather than going
  through ``get_cloud_path``, which uses ``pypeit/adap_2023/raw_data_reorg``. It reads
  from a different root than the reduce stage writes to, so it needs updating before the
  2D coadds will find this campaign's data.
* ``get_reduce_params`` in `scripts/utils.py <scripts/utils.py>`_ falls back to
  ``config/default_pypeit_config`` when a dataset prefix has no custom config file, and
  that file no longer exists — it was superseded by the three per-spectrograph defaults,
  ``keck_lris_red_default_pypeit_config``,
  ``keck_lris_red_mark4_default_pypeit_config`` and
  ``keck_lris_blue_default_pypeit_config``. The path is read without checking for it, so
  the 2D coadd stage fails on any prefix that does not match a custom file.

  It cannot simply be repointed at one of the three. Unlike
  `trimming_setup.py <scripts/trimming_setup.py>`_, which selects its default with
  ``args.spectrograph``, ``get_reduce_params`` is given only the dataset prefix, and a
  prefix such as ``J1030+0524`` names neither the arm nor the detector era, so there is
  no single correct default to choose. Deciding what the fallback should be — infer the
  spectrograph from the data being coadded, or require a per-prefix config file and fail
  with a clear message — is an open design question.
* ``dataset_to_spec`` in `scripts/metadata_info.py <scripts/metadata_info.py>`_ expects
  the DEIMOS-era dataset layout, in which the first path component is the instrument and
  the third is a PypeIt spectrograph name. On this branch the first component is the
  target, so it silently returns a bogus spectrograph name rather than raising::

      J1030+0524/20120415/LRIS  ->  keck_j1030+0524
      J1030+0524                ->  keck_j1030+0524

  It is called by `run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ and
  `flux_coadd1d_from_queue.py <scripts/flux_coadd1d_from_queue.py>`_, so
  `Re-score without re-reducing`_ and `Flux calibrate and coadd 1D`_ both start from a
  spectrograph name that does not exist. ``get_lris_spec_name`` in
  `scripts/extended_spec_mixins.py <scripts/extended_spec_mixins.py>`_ is the right
  function for this branch's naming, but it needs an observation date, and the coadd
  stages are handed a dataset *prefix* that need not contain one — so fixing this means
  deciding where the date comes from, not just swapping the call.
* Google authentication always comes from
  ``$HOME/.config/gspread/service_account.json``, gspread's built-in default. There is no
  option to point it elsewhere, so ``$HOME`` has to be right in any container that runs
  these scripts.
* Redis has no password and no persistence. Anything in the namespace can read or drain
  the queue, and a restart of the pod loses it.
