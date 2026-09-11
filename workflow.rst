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

One detail of the image is worth knowing: it carries its own ``adap`` clone, with
``WORKDIR`` pointing at it, that no job uses — every job clones ``adap`` fresh under
``/tmp/adap_root``. That baked-in copy is frozen at image build time, so an interactive
shell in the container starts out in stale code.

The Dockerfile installs ``psutil`` explicitly even though PypeIt's ``[dev]`` extra also
provides it, because `reduce_from_queue.py <scripts/reduce_from_queue.py>`_ needs it to
sample a reduction's memory use. Leave it in that list rather than relying on PypeIt to
supply it.

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
    and ``LRIS`` receive the scorecard. ``coadd status`` is the queue for the 2D coadd
    stage, which is not part of the current workflow, so that tab is unused.

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

**The git checkout is not what runs.** Every job in this workflow clones this repository
and then overwrites ``scripts/`` and ``config/`` from S3::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/scripts_2023/ scripts/ --recursive
    aws --endpoint $ENDPOINT_URL s3 cp --no-progress s3://pypeit/adap/config_2023/  config/  --recursive

`backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_ is the one exception. It clones
nothing, and pulls `backup_datasets.sh <scripts/backup_datasets.sh>`_, its dataset list and
an ``rclone.conf`` straight from ``s3://pypeit/adap/scripts/``. That is the un-suffixed
prefix, so the deploy below does not reach it — just as it does not reach the superseded
queue jobs under `Not part of this workflow`_, which read from the same older location.

So editing a script here has no effect on the cluster until it is pushed the other way::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress scripts/ s3://pypeit/adap/scripts_2023/ --recursive \
        --exclude "*__pycache__/*" --exclude "*.pyc"
    aws --endpoint $ENDPOINT_URL s3 cp --no-progress config/  s3://pypeit/adap/config_2023/  --recursive

Do this before every run in which a script or config file changed.

The ``--exclude`` flags keep ``__pycache__`` out of S3. Without them the push ships
compiled bytecode — including ``.pyc`` files for scripts that no longer exist — which then
copies back down into every pod. It is clutter rather than a hazard, since Python ignores
a ``.pyc`` whose source is missing or changed, but there is no reason to carry it. If a
previous push already uploaded some, clear it once with::

    aws --endpoint $ENDPOINT_URL s3 rm s3://pypeit/adap/scripts_2023/ --recursive \
        --exclude "*" --include "*__pycache__/*"

**One-time:** `backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_ reads its rclone
configuration from ``s3://pypeit/adap/scripts/rclone.conf``, which the ``config/`` deploy
above does not write. Put a copy there::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress config/rclone.conf s3://pypeit/adap/scripts/rclone.conf

That only needs redoing if the rclone configuration itself changes. Repeating it is
harmless, so it can simply be included in a deploy if that is easier to remember.

``config/exclude_files.txt`` needs particular care. It lists raw frames to drop from every
reduction, one file name per line, and `trimming_setup.py <scripts/trimming_setup.py>`_
reads it for every dataset **without checking that it exists first** — so if it is missing
from the deployed config, every reduction fails with ``FileNotFoundError``. It is checked
in containing only comments, which excludes nothing; blank lines and comments are ignored,
so an effectively empty file is fine. Because the deploy above copies S3 *over* the
checkout rather than replacing it, that checked-in copy is what gets used until an S3 copy
overwrites it — do not delete it from the repository.

After editing it, push it with the rest of ``config/``, or on its own::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress config/exclude_files.txt s3://pypeit/adap/config_2023/exclude_files.txt

The per-dataset override files described under `Reduction configuration`_ are the config
that genuinely exists only in S3; none of them are in this repository.

Note the two S3 prefix families, which are easy to confuse: ``s3://pypeit/adap/`` holds
the deployed scripts, config, and job logs, while ``s3://pypeit/adap_2023/`` holds this
campaign's raw data and the KOA target list.

Start the work queue
--------------------

The queue is a redis instance in Nautilus. Both the Deployment and the Service that
fronts it live in one file::

    kubectl apply -f nautilus_jobs/persist_volume.yml
    kubectl apply -f nautilus_jobs/workqueue_deployment.yml

Check that it came up before going on::

    kubectl get pods -l k8s-app=adap-workqueue

Every queue-driven job then reaches it at ``redis://adap-workqueue:6379`` — unlike the
2019 workflow, there is no pod IP to look up and paste into the yamls. The keys are
``adap_2023_q`` for the queue itself and ``adap_2023_lock`` for the lock that serialises
spreadsheet updates. Both are built from the ``adap_2023`` argument on each job's command
line, so changing that argument moves a job to an entirely different queue.

**The queue does not survive the pod.** The container runs a bare ``redis-server`` and
mounts no volume, so anything redis writes lands on the pod's ephemeral storage and is
gone when the pod is replaced. Adding ``save`` directives would not change that — keeping
a queue across a restart would need a volume for redis to write into. After any restart,
re-seed the queue as in `Populate the queue`_.

**Do not scale the Deployment past** ``replicas: 1``. The Service load-balances across
every pod matching its selector, so a second replica would be a second, independent redis.
Jobs would be split across two queues, and ``adap_2023_lock`` would no longer serialise
anything, because the pods holding it would not be talking to the same server.

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
science, arc, and flat frames — standard stars included, which the
`Generate sensitivity functions`_ stage later depends on::

    kubectl create -f nautilus_jobs/adap_koa_download.yml

**The job runs** `download_lib <scripts/download_lib>`_ **from S3**, not from the image
and not from the git checkout. Like every other job it overwrites ``scripts/`` from
``s3://pypeit/adap/scripts_2023/`` before running, so the deployed copy is what executes.
The recursive push in `Deploy the scripts and config to S3`_ carries the subdirectory
along, so an ordinary deploy is enough — but the whole directory has to arrive, because
the modules import each other by bare name (``import Query``, ``import Night``) and a
partial copy fails at import. Check before running the job::

    aws --endpoint $ENDPOINT_URL s3 ls s3://pypeit/adap/scripts_2023/download_lib/

That must list at least ``download.py``, ``DownloadUtils.py``, ``Night.py``, ``Query.py``
and ``Target.py``; those five are what ``download.py`` pulls in. If the listing is empty
or short, push just that directory::

    aws --endpoint $ENDPOINT_URL s3 cp --no-progress scripts/download_lib/ \
        s3://pypeit/adap/scripts_2023/download_lib/ --recursive \
        --exclude "*__pycache__/*" --exclude "*.pyc"

`download_lib <scripts/download_lib>`_ organizes what it finds into::

    <target>/<YYYYMMDD>/LRIS/raw_r
    <target>/<YYYYMMDD>/LRISBLUE/raw_b

and the job uploads that tree to ``s3://pypeit/adap_2023/raw_data_reorg/``. That is the
root ``get_cloud_path`` in `scripts/rclone.py <scripts/rclone.py>`_ returns, and every
stage in this workflow reads through it.

Three tolerances decide what comes down, none of them configurable:

    **5 arcsec** — the radius of the KOA cone search around the target's ra and dec
    (``circle <ra> <dec> 0.00139`` in ``query_position``). A target whose catalogue
    position is off by more than this finds nothing at all.

    **20 arcsec** — a science frame is kept if its pointing is within this of the target
    (``match_sci_target`` in
    `download_lib/DownloadUtils.py <scripts/download_lib/DownloadUtils.py>`_).

    **0.5 arcmin** — a science frame is *also* kept if its pointing matches a PypeIt
    archive standard within this radius, regardless of the target. This is deliberate,
    and it is how standard stars reach the raw tree:
    `sensfunc_from_queue.py <scripts/sensfunc_from_queue.py>`_ finds them in the reduced
    data later. Arcs and flats are matched by instrument configuration instead, not by
    position.

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

Populate the queue
------------------

There is no ``load_nautilus_redis_queue.sh`` on this branch. Push directly with
``redis-cli`` inside the redis pod::

    POD=$(kubectl get pods -l k8s-app=adap-workqueue -o name | head -1)
    kubectl exec $POD -- redis-cli lpush adap_2023_q init

The ``head -1`` matters: if an old redis pod is still terminating alongside the new one,
``-o name`` returns two names and ``kubectl exec`` fails.

The ``init`` sentinel tells the first pod that claims it to initialize the queue from the
spreadsheet, pushing every dataset whose status is blank and marking those rows
``IN QUEUE``. Alternatively, push dataset names directly to run a specific set without
touching the sheet::

    kubectl exec $POD -- redis-cli lpush adap_2023_q J1030+0524/20120415/LRIS J1030+0524/20120415/LRISBLUE

**Push** ``init`` **before any dataset names, or not at all.** The queue is filled with
``lpush`` and drained with ``rpop``, so whatever goes in first comes out first::

    lpush init; lpush A B C   ->  stored as [C, B, A, init], claimed as init, A, B, C

``run_task_on_queue`` recognises the sentinel only on a pod's *first* claim. An ``init``
reaching a pod any later is treated as an ordinary dataset name: the pod runs the
reduction on a dataset called ``init``, then tries to write a status for it, and because
``init`` is not in column A the sheet update logs
``Did not find init to update status!``.

**The sentinel is consumed even when initialization fails.** If reading the spreadsheet
raises, the pod logs ``Failed initializing`` and exits — but ``init`` has already been
popped, so the queue is empty and nothing was queued. Push it again. That is safe at any
time: a second initialization finds no blank-status rows, because the first one marked
them ``IN QUEUE``, and so queues nothing.

**Populate the queue before starting the job that drains it.** A pod claims its first
dataset with a blocking ``brpop`` that gives up after ``--queue_timeout`` seconds, 120 by
default; on timeout it logs ``No more datasets in queue, exiting`` and exits zero, having
done nothing.

That timeout also puts a clock on initialization whenever ``parallelism`` is greater than
one. The pods that did not claim ``init`` spend those same 120 seconds waiting while the
first pod reads the spreadsheet, and ``init_work_queue`` retries rate-limited Google API
calls with 30, 60, 60 and 90 second backoffs. A slow sheet read can outlast the timeout
and leave every other pod exiting empty-handed. Raise ``--queue_timeout`` in the yaml if
that happens.

To inspect what is waiting::

    kubectl exec $POD -- redis-cli lrange adap_2023_q 0 -1

That prints head to tail, and datasets are claimed from the tail, so the **last** line
listed is the next one to be processed.

To discard the queue and start over, which is worth doing before re-seeding so that
entries left over from an abandoned run are not processed a second time::

    kubectl exec $POD -- redis-cli del adap_2023_q

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

Reduction configuration
~~~~~~~~~~~~~~~~~~~~~~~

Nothing here is run by hand. The pod invokes
`trimming_setup.py <scripts/trimming_setup.py>`_ once per dataset; it generates the
``.pypeit`` files and creates the ``reduce`` directory they are written into. What you
supply is the configuration it reads, and that has to be in S3 before the job starts.

This is where the division of labour sits.
`download_lib <scripts/download_lib>`_ decides, from KOA's archive metadata, which frames
to bring down at all. ``trimming_setup.py`` then works from the headers of the frames that
actually arrived: PypeIt classifies the frame types itself, and every arc and flat it
finds is left in place for PypeIt to combine — calibration frames are no longer trimmed
down to a fixed count. Two exclusions still apply, and both comment the frame out in the
``.pypeit`` file rather than deleting it, so it can be re-enabled by hand: anything listed
in ``config/exclude_files.txt``, and any frame PypeIt types as ``bias`` or ``dark``.

``trimming_setup.py`` has to run in the pod because the ``.pypeit`` file records the local
path to the downloaded raw data.

It starts from a per-spectrograph default, one for each of the five names
``get_lris_spec_name`` can return::

    config/keck_lris_red_orig_default_pypeit_config
    config/keck_lris_red_default_pypeit_config
    config/keck_lris_red_mark4_default_pypeit_config
    config/keck_lris_blue_orig_default_pypeit_config
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

Most of these are queue-driven jobs on the same ``run_task_on_queue`` loop as the
reduction, so their queues are populated and monitored exactly as above. Two are not:
`backup_datasets.sh <scripts/backup_datasets.sh>`_ works from an explicit list of datasets
rather than a queue, and `Archive for KOA`_ is run by hand, with no Nautilus job at all.

Re-score without re-reducing
----------------------------

When only the scorecard logic or its columns have changed,
`run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ re-scores existing
results in place::

    kubectl create -f nautilus_jobs/adap-run-scorecard-on-queue.yml

Generate sensitivity functions
------------------------------

`sensfunc_from_queue.py <scripts/sensfunc_from_queue.py>`_ picks standards out of the
reduced data and builds sensitivity functions. Results go back into the dataset's
``reduce`` directory as ``sens*`` files, replacing any earlier ones::

    kubectl create -f nautilus_jobs/adap-sensfunc-from-queue.yml

The arguments handed to ``pypeit_sensfunc`` come from
`config/sensfunc_config.ecsv <config/sensfunc_config.ecsv>`_. Every column except ``id``
becomes a command line option when it is non-empty, and ``id`` says which datasets the row
applies to. The rows are searched in order of decreasing specificity:

    1. the name of the ``spec1d`` file being processed
    2. the dataset, then each shorter prefix of it
    3. the PypeIt spectrograph the dataset reduces with
    4. ``DEFAULT``

Rung 3 is what the checked-in file uses: one row per LRIS version, so that the blue arm
gets ``--algorithm UVIS`` and the red arm ``--algorithm IR`` without naming a single
dataset. The spectrograph is derived from the dataset's date and arm with
``get_lris_spec_name``, the same function the reduce stage uses.

``DEFAULT`` must stay in the file. ``get_senfunc_args`` raises if it is missing, and it is
what catches a dataset whose name will not parse into a date and an arm.

To tune one dataset rather than a whole arm, add a row keyed on the dataset name; to tune
a single exposure, key it on the ``spec1d`` file name.

Flux calibrate and coadd 1D
---------------------------

`flux_coadd1d_from_queue.py <scripts/flux_coadd1d_from_queue.py>`_ fluxes the extracted
spectra and coadds them. It works at the dataset *prefix* level rather than on single
datasets, and writes ``<prefix>/1D_Coadd``. This is the only coadd stage in the current
workflow; 2D coaddition is not performed, see `Deprecated scripts`_::

    kubectl create -f nautilus_jobs/adap_flux_codd1d_from_queue.yml

Back up to Google Drive
-----------------------

`sync_backup_from_queue.py <scripts/sync_backup_from_queue.py>`_ mirrors S3 to Drive for
datasets on the queue::

    kubectl create -f nautilus_jobs/adap-sync-backups-from-queue.yml

`backup_datasets.sh <scripts/backup_datasets.sh>`_, run by
`backup_datasets.yml <nautilus_jobs/backup_datasets.yml>`_, does the same for an explicit
list of datasets read from ``s3://pypeit/adap/scripts/backup_list.txt``.

Both copy each ``reduce*`` directory from ``get_cloud_path``'s S3 root to
``gdrive:backups/<dataset>/<reduce dir>``. Note that this is a *second* Drive tree: the
reduce stage already uploads its results to ``gdrive:DATA`` as it goes, so ``backups/``
is a separate mirror rather than the copy the reduction itself wrote.

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

All three live under ``scripts/depreciated/`` and carry a deprecation notice in their
module docstring. None of them run as they stand: moving them out of ``scripts/``
means their bare imports no longer resolve, on top of the problems noted below.

* `coadd2d_from_queue.py <scripts/depreciated/coadd2d_from_queue.py>`_ ran
  ``pypeit_setup_coadd2d`` and ``pypeit_coadd_2dspec`` over a ``coadd status`` tab whose
  entries were dataset *prefixes*, writing a ``2D_Coadd`` directory. **2D coaddition is
  not part of the current workflow**, which coadds in 1D only with
  `Flux calibrate and coadd 1D`_, so the script has moved to ``scripts/depreciated/`` and
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
adap-stage-raw-queue.yml (deprecated)          ``key=1TADKd3OgbA…/WorkQueue@B``
adap-run-scorecard-on-queue.yml                ``Scorecard/WorkQueue``
adap-sync-backups-from-queue.yml               ``Scorecard/WorkQueue``
adap-coadd2d-queue.yml (deprecated)            ``Scorecard/coadd status``
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
* `run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ builds every one of
  its paths with a ``complete`` component — ``<dataset>/complete/reduce`` — which is the
  DEIMOS layout produced by the old reorganization step. The reduce stage on this branch
  writes ``<dataset>/reduce`` with no ``complete`` level, so the two disagree in seven
  places: the cloud source root, the ``reduce*`` glob, the download destination, the local
  scorecard directory, both upload destinations, and the csv path handed to
  ``update_gsheet_scorecard.py``.

  The practical effect is that the glob for ``<dataset>/complete/reduce*`` matches
  nothing, so the task logs "No reduce paths found" and returns ``FAILED`` for every
  dataset without re-scoring anything. It never reaches the sheet update. The cleanup in
  the ``finally`` block then calls ``rmtree`` on a dataset directory that was never
  created, so the log also carries a ``FileNotFoundError`` traceback on top of the real
  cause.

  Note that `scorecard.py <scripts/scorecard.py>`_ itself is tolerant of both layouts —
  ``get_dataset_from_reduce_path`` recognizes ``<dataset>/complete/reduce/<spec>`` and
  ``<dataset>/reduce`` alike. Only this driver hardcodes ``complete``, so `Re-score
  without re-reducing`_ needs those paths brought in line with the reduce stage before it
  will run.
* Google authentication always comes from
  ``$HOME/.config/gspread/service_account.json``, gspread's built-in default. There is no
  option to point it elsewhere, so ``$HOME`` has to be right in any container that runs
  these scripts.
* Redis has no password and no persistence. Anything in the namespace can read or drain
  the queue, and a restart of the pod loses it.
