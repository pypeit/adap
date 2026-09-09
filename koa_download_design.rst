Queue-driven KOA Download — Design
==================================

This document proposes replacing the one-off KOA download job with a queue-driven stage
that reads target coordinates from the ``Scorecard`` spreadsheet and writes the datasets
it discovers back into the ``WorkQueue`` tab. **This design has now been implemented**, so what follows describes both the reasoning and
the code that came out of it. The pieces are
`koa_download_from_queue.py <scripts/koa_download_from_queue.py>`_,
`download_lib/TargetList.py <scripts/download_lib/TargetList.py>`_, the target and
dataset helpers in `gspread_utils.py <scripts/gspread_utils.py>`_, the queue and throttle
changes in `utils.py <scripts/utils.py>`_, and
`adap-koa-download-from-queue.yml <nautilus_jobs/adap-koa-download-from-queue.yml>`_. The
sheet it needs is `google_sheet_setup.rst <google_sheet_setup.rst>`_.

Two things named here as future work were deliberately **not** done: pushing discovered
datasets straight onto the dataset queue, for the reason under `Writing datasets back`_,
and the per-target tolerance columns under `Open questions`_. Part 2 of
`workflow.rst <workflow.rst>`_ still describes the old ``targets.txt`` job and needs
updating to match.

The problem
-----------

Part 2 of `workflow.rst <workflow.rst>`_ reads as though "Download the raw data from KOA"
and "Populate the queue" connect. They do not. The download job takes a text file of
targets uploaded by hand to ``s3://pypeit/adap_2023/koa_to_download/targets.txt``, and the
queue takes a list of datasets typed by hand into column A of the ``WorkQueue`` tab.
Nothing in `scripts <scripts>`_ writes column A — every script that touches the sheet only
ever reads ``col_values(1)``. So the operator has to look at what actually landed under
``raw_data_reorg/`` and transcribe the dataset names into the sheet. A typo, or a target
KOA returned nothing for, is not caught until a reduce pod fails on it.

The information needed to fill that gap already exists and is thrown away.
``fill_dates`` in `download_lib/DownloadUtils.py <scripts/download_lib/DownloadUtils.py>`_
computes ``fn_date`` and ``instr`` for every night it downloads, which together with
``query.target.name`` is exactly the ``<target>/<date>/<instrument>`` dataset name. It
returns ``None``.

Why the download is a second queue, not part of the existing one
----------------------------------------------------------------

The obvious framing — "make the download just another stage of the dataset queue" —
cannot work, because of a unit-of-work mismatch:

    The dataset queue's unit is a **dataset**, ``<target>/<date>/<instrument>``.

    The download's unit is a **target**, ``<name> <ra> <dec>``.

The date and the arm are *discovered* by the KOA query. Until ``query_position`` has run,
there is no way to know that ``J1030+0524`` was observed on 20120415 at all, let alone
that both arms have usable data. Enqueueing datasets in order to download them is
circular.

So this is a second queue whose unit is a target, feeding the existing dataset queue:

.. parsed-literal::

    targets tab  ->  **adap_2023_targets_q**  ->  download pods  ->  raw_data_reorg in S3
                                                        |
                                                        +-------->  WorkQueue tab (col A)
                                                                          |
                                                    **adap_2023_q**  <----+  (init sentinel)
                                                          |
                                                          v
                                                     reduce pods

Nothing about the dataset queue changes. `run_task_on_queue <scripts/utils.py>`_ is
already generic over "an item with a status in a sheet column" — it does not know or care
that the items are currently datasets — and the queue and lock keys are built from the
``work_queue`` positional argument, so a distinct key is free. The target stage passes
``adap_2023_targets`` and gets ``adap_2023_targets_q`` and ``adap_2023_targets_lock``
without a line of new harness code. The ``init`` sentinel then seeds the target queue from
the ``targets`` tab by the same mechanism that seeds the dataset queue from ``WorkQueue``.

Why bulk retrieval stays out of the reduce stage
-------------------------------------------------

A tempting further step is to drop the S3 copy and have reduce pods pull straight from
KOA. It should not be taken:

    **S3 is a cache, and it earns its keep.** Reductions are re-run constantly while
    per-dataset configuration is tuned. Re-fetching from KOA on every iteration is slow
    and is rude to an external service we do not control.

    **The pods want different resources.** Reduce pods are sized for reduction — 20 to
    100 GiB of ephemeral storage and 8+ GiB of memory. Download pods want bandwidth and
    patience.

    **It conflates two failures.** "KOA was unreachable" and "the reduction failed" would
    land in the same status column, and the retry for each is different.

Keep ``KOA -> pod -> S3`` as its own stage. Make it queue-driven; do not merge it.

Throttling
----------

KOA should not be asked to serve many simultaneous requests. The working assumption for
this design is **a maximum of two concurrent downloads**, and the mechanism has to be
something that cannot be defeated by a careless ``kubectl create``.

What pykoa actually does
~~~~~~~~~~~~~~~~~~~~~~~~

Worth stating precisely, because it determines everything else. ``Koa.download`` is a
plain serial ``for`` loop over the rows of the metadata table — no threads, no pool, no
connection reuse across processes. One pod therefore holds **exactly one** connection to
KOA at a time, no matter how many files or how many nights a target has.

That makes the arithmetic simple and exact:

    concurrent KOA connections == number of running download pods

pykoa also checks ``os.path.exists`` on each destination file and skips ones already
present, so its downloads resume correctly at file granularity. That property is currently
defeated at the adap layer; see `Resume and retry`_ below.

There is no inter-file delay anywhere in ``Koa.download``. A single pod will issue
requests back to back for as long as its target's file list runs.

Primary throttle: pod count
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because concurrency equals pod count, ``parallelism`` in the Job spec *is* the throttle,
and it is exact rather than approximate::

    spec:
      parallelism: 2

This is the whole mechanism for the ordinary case, and it needs no code. Note that the
queue is pull-based, so queue depth does not affect concurrency — a thousand targets
waiting are still served by two pods.

Backstop: a redis semaphore
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``parallelism`` caps one Job. It does not stop a second download Job, or a hand-run retry
job, from being applied alongside the first. If that matters — and during a campaign, when
someone is retrying a handful of stubborn targets while the main job still runs, it does —
a counting semaphore in the redis instance that is already there will bound the total.

The simplest form that blocks rather than spins is a list of tokens. Seed it once with as
many tokens as permitted concurrent downloads, then have each pod ``BLPOP`` a token before
calling ``fill_dates`` and ``LPUSH`` it back in a ``finally``::

    kubectl exec $POD -- redis-cli del adap_2023_koa_slots
    kubectl exec $POD -- redis-cli lpush adap_2023_koa_slots slot1 slot2

**A token is leaked if a pod dies while holding one**, which permanently lowers the
concurrency limit until the key is re-seeded. This is the same class of problem as the
work queue lock having no TTL, and it should be treated the same way — the re-seed command
above belongs in the runbook, and the pod should log loudly when it waits more than a few
minutes for a token. A leak that silently reduces throughput to zero is worse than no
semaphore at all, so this is optional, and ``parallelism: 2`` alone is a defensible
starting point.

Per-file pacing
~~~~~~~~~~~~~~~

If two pods issuing back-to-back requests still proves to be too much, the next lever is a
short sleep between files. That requires wrapping ``Koa.download`` rather than calling it
per table, since it loops internally over the whole table. The cheapest approximation
without touching pykoa is to call it once per *table* — which the code already does, via
the separate science, arc and flat tables — and sleep between those calls. Start without
this; add it only if KOA's behaviour demands it.

Resume and retry
~~~~~~~~~~~~~~~~

Throttling and retry are the same subject: the more aggressively a download is throttled,
the more often it is interrupted, and the more the resume path matters. Three things in
the current code need attention before that path is trustworthy.

    **Failures were silent — now fixed.** ``Koa.download`` catches per-file errors
    itself, prints them and continues to the next row, so it returns normally even when
    every frame failed; wrapping it in ``try``/``except`` therefore caught almost nothing.
    A KOA outage produced a dataset directory with some or none of its frames, which was
    uploaded to S3 and reduced as though it were complete.

    ``verify_download`` in `DownloadUtils.py <scripts/download_lib/DownloadUtils.py>`_ now
    compares what is on disk against the ``koaid`` column of each final table after
    ``file_cleanup`` has run, and ``check_night`` collapses a night's science, arc and
    flat tables into one ``COMPLETE`` / ``INCOMPLETE`` / ``FAILED`` result per dataset.
    What is on disk is the authority: a download call that raised while every frame is
    nonetheless present is noted and passes, and an unreadable final table fails rather
    than reporting zero frames as complete. ``fill_dates`` returns those results,
    `download.py <scripts/download_lib/download.py>`_ summarises them and exits non-zero,
    and `adap_koa_download.yml <nautilus_jobs/adap_koa_download.yml>`_ carries that status
    through its ``| tee`` with ``pipefail`` so the Job actually fails. A target KOA has no
    data for still exits zero — that is not a failure.

    The queue task inherits all of this: it reports ``FAILED`` for the target when
    ``fill_dates`` returns any dataset that is not ``COMPLETE``.

    **Partial downloads were not resumed — now fixed.** ``check_if_downloaded`` returned
    ``True`` if *any one* file from the table was present, and ``Query.download`` then
    skipped the call entirely, so a download interrupted after one frame of thirty never
    fetched the other twenty-nine.

    An earlier draft of this document said that pykoa's own per-file resume would handle
    this correctly if it were allowed to run. **That is wrong**, and it is worth recording
    why, because it is not obvious. ``Koa.download`` skips frames it finds at
    ``lev0/<koaid>`` under the output directory, but ``file_cleanup`` moves the frames up
    out of ``lev0`` as soon as the night finishes. On any later attempt pykoa looks into
    an empty ``lev0``, sees nothing, and re-fetches the entire table. Its resume works
    within one run and not across them.

    So ``check_if_downloaded`` now requires *all* frames rather than any, and
    ``Query.download`` asks KOA only for what is missing: ``missing_frames`` checks both
    ``lev0`` and the cleaned-up location, and ``write_retry_table`` writes a copy of the
    final table holding just the outstanding rows to hand to ``Koa.download``. That makes
    the resume explicit rather than dependent on where the frames that did arrive ended
    up. A table already complete costs no KOA call at all.

    **Retries re-queried — now fixed.** ``Query`` caches its ``.tbl`` results in
    ``outdir`` and returns early if the file is present, but ``outdir`` is on the pod's
    ephemeral ``/tmp``, so a retry on a fresh pod re-ran every KOA query from scratch.
    ``restore_query_cache`` and ``save_query_cache`` in
    `koa_download_from_queue.py <scripts/koa_download_from_queue.py>`_ keep the tables in
    ``s3://pypeit/adap_2023/koa_queries/<target>/`` and restore them before querying, so a
    retry costs only the frames that are actually missing. The cache is written even for a
    target that turned out to have no data, so re-running the queue does not re-ask KOA
    about it.

The ``targets`` tab
-------------------

A new tab in the same ``Scorecard`` spreadsheet — the same one that holds ``WorkQueue`` —
because the scorecard updater derives its tabs from whatever spreadsheet the running job
was handed.

=======  ==============  =========================================================
Column   Contents        Notes
=======  ==============  =========================================================
A        target          Target name, one per row, starting at **row 4**.
B        status          Written by the job. Leave blank to queue a target.
C        pod             Written by the job.
D        ra              Degrees.
E        dec             Degrees.
=======  ==============  =========================================================

Row 4 and the blank-status convention are not choices; ``init_work_queue`` in
`scripts/utils.py <scripts/utils.py>`_ begins at row 4 and queues only blank rows, and the
pod column is always the one immediately right of the status column. Columns D and E are
new — nothing in the existing harness reads past the pod column, so they are free to add.

Reading them needs one helper in `scripts/gspread_utils.py <scripts/gspread_utils.py>`_,
returning ``Target`` objects. It replaces ``parse_target_file`` in
`download_lib/download.py <scripts/download_lib/download.py>`_ for the queue path only;
``download.py`` keeps its text-file entry point for local and manual runs.

Changes to download_lib
-----------------------

Small and additive.

    ``fill_dates`` **already returns** a ``(dataset, status, detail)`` tuple per dataset,
    where dataset is the ``<target>/<date>/<instrument>`` name — done as part of the
    verification work under `Resume and retry`_. The queue task needs the names of the
    ``COMPLETE`` ones and nothing further.

    ``check_if_downloaded`` **has already lost** its any-file short circuit, so a
    partially downloaded dataset now resumes instead of restarting. Nothing further is
    needed for the queue stage.

    Nothing else in `download_lib <scripts/download_lib>`_ changes. It is already
    organised as target -> query -> date -> instrument, which is the decomposition this
    design needs.

The new job script
------------------

``scripts/koa_download_from_queue.py``, modelled on
`test_from_queue.py <scripts/test_from_queue.py>`_ — which is a complete queue-driven job
in 43 lines, because `run_task_on_queue <scripts/utils.py>`_ does the work. The task
function:

1. Takes a token from the semaphore, if one is in use.
2. Looks up the popped target's ra and dec in the ``targets`` tab.
3. Restores any cached query tables for that target from S3.
4. Runs ``Query.query_position`` and ``fill_dates``.
5. Verifies the downloaded files against the final tables.
6. Uploads the tree to ``s3://pypeit/adap_2023/raw_data_reorg/`` and the query tables to
   the cache location.
7. Appends the discovered datasets to the ``WorkQueue`` tab.
8. Returns ``COMPLETE`` or ``FAILED``, and releases the token in a ``finally``.

Its positional arguments follow the existing convention, with the target queue's key in
the ``work_queue`` slot::

    python scripts/koa_download_from_queue.py --rclone_conf config/rclone.conf
        --adap_root_dir /tmp/adap_root
        Scorecard/targets redis://adap-workqueue:6379 adap_2023_targets s3

The job yaml is `adap_koa_download.yml <nautilus_jobs/adap_koa_download.yml>`_ with the
``targets.txt`` fetch dropped, ``parallelism: 2``, and the redis service reachable — it
needs no new credentials, since it already has the S3 secret and the container already
carries the gspread service account.

Writing datasets back
---------------------

The riskiest part of the design, and the one to build carefully.

Each target pod appends its discovered datasets to column A of the ``WorkQueue`` tab with
a **blank** status, so that the ordinary ``init`` sentinel will queue them. It must skip
names already present in column A, so that re-running a target does not duplicate rows.

Two pods appending to the same tab at the same time will corrupt it if unguarded, and
gspread offers no transactional append — the sequence is read column A, compute the new
rows, append, and it has to be atomic. Take the **dataset** queue's lock,
``adap_2023_lock``, for this, not the target queue's. They are separate keys, and only the
former guards that tab. This is also why the lock's missing TTL is a prerequisite rather
than a nicety; see `Prerequisites`_.

An optional refinement is to ``LPUSH`` the new dataset names directly onto ``adap_2023_q``
as well, so a reduce job can consume datasets as they are discovered instead of waiting
for the download stage to finish. **Do not do this first.** Reduce pods claim
non-blockingly after their first item and exit the moment the queue is momentarily empty,
so a reduce job running alongside a slow KOA query would simply die. Run the two stages
sequentially until that behaviour is fixed.

Prerequisites
-------------

Two existing rough edges become load-bearing once there are two queues, and should be
fixed first.

    **The work queue lock has no TTL.** ``lock_workqueue`` in
    `scripts/utils.py <scripts/utils.py>`_ calls ``redis_server.lock(name)`` with every
    default: no ``timeout``, so the lock never expires, and ``blocking_timeout=None``, so
    waiters block forever. A pod killed between acquire and release wedges every other pod
    silently. This design increases lock traffic — every target pod takes it to append
    rows — so give it both a ``timeout`` and a ``blocking_timeout``.

    **The init sentinel is only honoured on a pod's first claim.**
    ``run_task_on_queue`` compares against ``"init"`` only for the initial blocking
    ``claim_dataset``; popped later in the loop it is passed to the task as if it were an
    item. With two queues there are two sentinels in circulation and two chances to hit
    this. Handle ``"init"`` inside the loop as well.

What this retires
-----------------

``targets.txt`` and the ``s3://pypeit/adap_2023/koa_to_download/`` prefix disappear.
`adap_koa_download.yml <nautilus_jobs/adap_koa_download.yml>`_ is superseded by the queue
job and moves to the "Not part of this workflow" section of
`workflow.rst <workflow.rst>`_, alongside the CSV-era queue jobs it will then resemble.

Part 2 of the workflow becomes a single sequence with no hand transcription in it:

1. Fill in the ``targets`` tab.
2. Push ``init`` to ``adap_2023_targets_q``.
3. Run the download job.
4. Push ``init`` to ``adap_2023_q``.
5. Run the reduction.

Open questions
--------------

* How KOA behaves under two sustained serial connections is genuinely unknown. Start the
  job at ``parallelism: 1``, watch one full target, and raise it to 2 only after seeing
  the request pattern. The design assumes 2 is safe; it does not establish it.
* Whether the 5 arcsec cone search, 20 arcsec science match and 0.5 arcmin standard-star
  match should become columns in the ``targets`` tab rather than constants in
  `DownloadUtils.py <scripts/download_lib/DownloadUtils.py>`_. Per-target tolerances would
  be useful for targets with poor catalogue positions, but it widens the change.
* **Settled:** a target that yields no datasets is marked ``NO DATA`` rather than
  ``COMPLETE``. It is not a failure, but those rows are worth finding later — a target
  whose catalogue position is off by more than the 5 arcsec cone search looks exactly the
  same, and the two are only told apart by checking the position.
* **Settled:** the query tables are cached at
  ``s3://pypeit/adap_2023/koa_queries/<target>/``, beside the raw data rather than under
  the ``adap/`` prefix with the logs, so that everything belonging to a campaign is under
  one prefix.
