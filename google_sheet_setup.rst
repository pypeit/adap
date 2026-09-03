Google Sheet Setup
==================

One Google spreadsheet, named ``Scorecard``, drives the whole pipeline. It is both the
input — the list of datasets to process — and the output — per-dataset status and the
scorecard metrics for every reduction. Each stage of the workflow gets its own tab of that
one spreadsheet. This document describes the tabs it must contain, the columns in each,
and how the jobs address it.

The scorecard updater is given only the *spreadsheet* part of the name the reduce job is
passed (it calls ``args.gsheet.split("/")[0]``), so the queue tab and the scorecard tabs
have to live in the **same spreadsheet**.

How the sheet is named
----------------------

Every job takes the sheet as one command line argument, in this form::

    [key=]<spreadsheet>/<worksheet>[@<status column>]

Address it **by name**, so that every stage is visibly pointed at the same spreadsheet::

    Scorecard/WorkQueue
    Scorecard/coadd status

Opening by name requires the service account to be able to *find* the file, so if the
spreadsheet lives on a shared drive the service account has to be a member of that drive.

The ``key=`` form is also accepted, and takes the id out of the spreadsheet's URL between
``/d/`` and ``/edit``. It is a fallback for when a name cannot be resolved; it obscures
which spreadsheet a job is actually using, which is how the yamls came to be split across
three of them. The parsing lives in ``open_spreadsheet`` in
`scripts/gspread_utils.py <scripts/gspread_utils.py>`_.

The optional ``@<column>`` names the column the status is written to; it defaults to
``B``, and the pod name always goes in the column immediately to its right.

**Use only columns A to Y.** ``index_to_column_name`` in
`scripts/gspread_utils.py <scripts/gspread_utils.py>`_ derives the pod column by taking
the *first character* of the converted name, so column 27 becomes ``AA`` and is truncated
back to ``A``. A status column of ``Z`` therefore resolves its pod column to ``A``, and
the update range ``Z<n>:A<n>`` is normalised by Sheets to ``A<n>:Z<n>`` — which writes the
status into column A and overwrites the dataset name. Nothing warns you; the row is simply
destroyed.

Required tabs
-------------

Five tabs in total, of which four are needed for a reduction campaign and the fifth only
if the 2D coadd job is run:

=================  ==========================  ==============================================
Tab                Name comes from             Needed
=================  ==========================  ==============================================
``WorkQueue``      the job's command line      always
``latest``         hardcoded                   always
``Failed``         hardcoded                   always
``LRIS``           hardcoded                   always
``coadd status``   the job's command line      only for the 2D coadd job
=================  ==========================  ==============================================

Only three of those names are actually fixed in the code. ``latest``, ``Failed`` and
``LRIS`` are hardcoded in `update_gsheet_scorecard.py <scripts/update_gsheet_scorecard.py>`_::

    sheets = ['latest', 'Failed','LRIS', ]

``WorkQueue`` and ``coadd status`` are only a convention: they are the worksheet names the
yamls happen to pass, so renaming either one means editing every yaml that names it, not
the scripts.

Input: the ``WorkQueue`` tab
----------------------------

Three columns, and the dataset list starts on **row 4**:

=======  ==============  =========================================================
Column   Contents        Notes
=======  ==============  =========================================================
A        dataset         The datasets to process, one per row, starting at row 4.
B        status          Written by the jobs. Leave blank to queue a dataset.
C        pod             Written by the jobs: the pod that claimed the dataset.
=======  ==============  =========================================================

Rows 1 to 3 are yours — a title row and whatever notes are useful. ``init_work_queue`` in
`scripts/utils.py <scripts/utils.py>`_ starts reading at row 4, so anything above that is
ignored when the queue is loaded.

Dataset names on this branch are ``<target>/<YYYYMMDD>/<LRIS|LRISBLUE>``, for example::

    J1030+0524/20120415/LRIS
    J1030+0524/20120415/LRISBLUE

The red and blue arms are separate datasets and get their own rows. Status updates match
the dataset by **exact string comparison** on column A, so trailing characters or a
renamed dataset mean the status silently fails to update (the job logs it and carries on).

The status column moves through these values:

``<blank>``
    Eligible. Only blank rows are loaded when the queue is initialized.

``IN QUEUE``
    Written when the dataset is pushed onto the redis queue.

``In Progress``
    Written by the pod that pops the dataset, along with its name in column C.

``COMPLETE``, ``FAILED``, ``WARNING``
    The result. ``WARNING`` means the work and the S3 upload succeeded but the Google
    Drive copy did not; it is set by both
    `reduce_from_queue.py <scripts/reduce_from_queue.py>`_ and
    `sensfunc_from_queue.py <scripts/sensfunc_from_queue.py>`_.

**To re-run a dataset, blank its status** and re-initialize the queue. Rows with any
status at all are skipped.

Output: the ``latest``, ``Failed`` and ``LRIS`` tabs
----------------------------------------------------

These three receive the scorecard. `scorecard.py <scripts/scorecard.py>`_ writes a
``scorecard.csv`` per reduction and
`update_gsheet_scorecard.py <scripts/update_gsheet_scorecard.py>`_ merges it into each
tab.

All three have the same shape: **row 1 is a header, data starts at row 2**, and the
header must have exactly the 31 columns of ``scorecard.csv``, in order, ending at column
``AE``::

    dataset, science_file, date, status, sn_percentile_16, sn_percentile_50,
    sn_percentile_84, bad_slit_count, det_count, slit_count,
    slit_std_chi_out_of_range, slit_rms_over_thresh, slit_spec_flex_over_thresh,
    total_bad_flags, bad_wv_count, bad_tilt_count, bad_flat_count, skip_flat_count,
    bad_skysub_count, bad_extract_count, object_count, object_fracpos_over_thresh,
    obj_rms_over_thresh, object_flex_shift_over_thresh, object_without_opt_with_box,
    object_without_opt_wo_box, maskdef_extract_count, exec_time, mem_usage,
    git_commit, reduce_dir

The column *count* is checked on every update, and a mismatch is fatal::

    ValueError: CSV file does not match the columns in <spreadsheet>/<worksheet>

Five of the names have to be spelled exactly as above, because the updater looks them up
by name rather than by position: ``dataset``, ``science_file``, ``reduce_dir`` and
``date`` are sort keys, and ``status`` is what the ``Failed`` tab filters on.

The *order* matters just as much, and for a different reason. ``build_array_from_rows``
assigns column types positionally — four string columns, then three floats, then an
integer for every column from the fifth up to the last five, then two more strings::

    ['U256', 'U22', 'datetime64[D]', 'U8'] + [float,float,float] \
        + [int for x in data_rows[0][4:-5]] + ['U40', 'U20']

So inserting, removing or reordering a column anywhere in the header makes the updater
parse the wrong type for everything after it. The simplest way to create these tabs is to
paste in the header line of a ``scorecard.csv`` produced by a real reduction.

The tabs differ only in what they keep:

``LRIS``
    Everything, sorted by dataset, then science file, then reduce directory. Only the
    dataset comparison is case-insensitive — it is the one field the updater uppercases
    before sorting — so science file and reduce directory sort case-sensitively.

``Failed``
    Only rows whose status is ``FAILED``, most recent first.

``latest``
    A rolling window. Rows older than the age limit, measured back from the *oldest row in
    the incoming data* rather than from today, are deleted — so a pause in reductions does
    not empty the tab.

    The limit reaches the updater as its third positional argument, ``latest_days``. The
    reduce jobs set it from ``--scorecard_max_age``, which is ``7`` in the queue jobs
    today. `run_scorecard_on_queue.py <scripts/run_scorecard_on_queue.py>`_ instead passes
    a hardcoded ``10000``, so re-scoring existing results does not age anything out.
    (``latest_days`` is declared with ``default = 5``, but that default is unreachable
    because argparse positionals are required; the help text saying it defaults to 5 is
    wrong.)

Each update rewrites the block of rows belonging to a dataset, inserting or deleting rows
so the block is the right size, and keeps the tab in sorted order. **Do not keep
hand-maintained rows or notes in these three tabs** — they will be moved or overwritten.
Add extra columns to the right of ``AE`` at your own risk; the count check reads the
header row.

Optional: the ``coadd status`` tab
----------------------------------

`coadd2d_from_queue.py <scripts/coadd2d_from_queue.py>`_ uses its own tab because coadding
is done at a coarser level than reduction. It has the same three columns as ``WorkQueue``,
but column A holds only a *prefix* of a dataset name, describing everything to be
combined, for example::

    J1030+0524
    J1030+0524/20120415

Sharing and permissions
-----------------------

The jobs authenticate as a Google service account, so:

* Share the spreadsheet with the service account's ``client_email`` as an **Editor**. It
  writes the status, pod and scorecard columns, so read-only access is not enough.
* If the spreadsheet lives on a shared drive, make sure the service account is a member
  of that drive, or it will not be able to resolve the name ``Scorecard``.
* The Sheets API must be enabled in the service account's project.

The credential itself, where it has to be mounted, and the Drive side of the same account
are documented in `nautilus_jobs/CREDENTIALS.md <nautilus_jobs/CREDENTIALS.md>`_.

Which job uses which sheet
--------------------------

Every job should be given ``Scorecard/<tab>``. Several checked-in yamls still carry
``key=`` arguments naming two other spreadsheets, and because the scorecard tabs are
resolved from whichever spreadsheet the running job was handed, a reduction and a
post-processing stage pointed at different spreadsheets write their status into different
sheets. The current state of each yaml is tabulated under "Known rough edges" in
`workflow.rst <workflow.rst>`_. Make them agree before a campaign.

Creating a sheet from scratch
-----------------------------

1.  Create the spreadsheet, on the shared drive if the results are shared.
2.  Create five tabs named ``WorkQueue``, ``latest``, ``Failed``, ``LRIS``, and — if 2D
    coadds will be run — ``coadd status``.
3.  In ``WorkQueue``, put a header in row 1 (``dataset``, ``status``, ``pod``), leave rows
    2 and 3 for notes, and list the datasets from row 4 down. Leave columns B and C empty.
4.  In ``latest``, ``Failed`` and ``LRIS``, paste the 31 column header above into row 1
    and leave the rest empty.
5.  Share the spreadsheet with the service account's ``client_email`` as an Editor.
6.  Name the spreadsheet ``Scorecard`` and make sure every job yaml passes
    ``Scorecard/WorkQueue`` — or ``Scorecard/coadd status`` for the 2D coadd job.
7.  Initialize the queue as described under "Populate the queue" in
    `workflow.rst <workflow.rst>`_.
