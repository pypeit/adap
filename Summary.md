## The big picture

`adap` isn't a library — it's the **orchestration layer that runs [PypeIt](https://github.com/pypeit/PypeIt) at scale on the NRP/Nautilus Kubernetes cluster**. It has no code of its own that reduces spectra; it decides *what* to reduce, hands work to pods, moves data in and out of cloud storage, and scores the results.

This branch (`adap_2023`) is the Keck LRIS campaign. [workflow.rst](workflow.rst) is the operational procedure; this file is the mental model behind it.

Four external systems form the control plane:

| System | Role |
|---|---|
| **Google Sheet** | Human-facing dashboard. One spreadsheet named `LRIS-ADAP`, one tab per stage. Column A = target or dataset names, a status column = `IN QUEUE` / `In Progress` / `COMPLETE` / `FAILED` / `WARNING` / `NO DATA`, plus scorecard tabs |
| **Redis** (in-cluster) | The actual work queues + distributed lock + the KOA download semaphore. Sheet API has no locking, so Redis arbitrates between parallel pods |
| **S3** (Ceph via `rook-ceph-rgw-nautiluss3.rook`) | Primary data store: raw data, reduce products, KOA query cache, logs |
| **Google Drive** | Secondary backup of results, via rclone |

## The four directories
- [nautilus_jobs/](nautilus_jobs/) — Kubernetes `Job`/`Deployment` YAML. One file per pipeline stage.
- [scripts/](scripts/) — the Python that actually runs inside the pods.
- [config/](config/) — PypeIt parameter files (per-spectrograph defaults + per-dataset overrides) and [rclone.conf](config/rclone.conf).
- [kube_tests/](kube_tests/) — one stale, pre-2023 Dockerfile, superseded by [config/pypeit_lris_adap.docker](config/pypeit_lris_adap.docker) and referenced by nothing.

## Two queues, not one

The thing most worth internalizing about this branch: there are **two** redis queues, and they have different units of work.

    targets tab  ->  adap_2023_targets_q  ->  download pods  ->  raw_data_reorg in S3
                                                    |
                                                    +------->  WorkQueue tab (col A)
                                                                      |
                                                adap_2023_q  <--------+  (init sentinel)
                                                      |
                                                      v
                                            reduce / sensfunc / coadd pods

The download stage's unit is a **target** (`<name> <ra> <dec>`); every stage after it works on a **dataset** (`<target>/<date>/<instrument>`). The date and the arm are *discovered* by the KOA query, so a dataset cannot be enqueued before the query that finds it has run — enqueueing datasets in order to download them would be circular. Hence the second queue, feeding the first. [koa_download_design.rst](koa_download_design.rst) is the reasoning in full.

This costs no new harness code: [`run_task_on_queue`](scripts/utils.py#L254) is generic over "an item with a status in a sheet column", and the queue and lock keys are built from each job's `work_queue` positional argument. `adap_2023` gives `adap_2023_q` / `adap_2023_lock`; `adap_2023_targets` gives `adap_2023_targets_q` / `adap_2023_targets_lock`.

## The workflow

**1. Stand up the queue.** [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) is the Redis server plus the `adap-workqueue` Service that fronts it. A queue is then seeded by pushing the sentinel string `init` onto it with `redis-cli`; the first pod to claim `init` calls [`init_work_queue`](scripts/utils.py#L190), which reads that queue's tab and pushes every row with a blank status, marking those rows `IN QUEUE`. Names can also be pushed directly to run a specific set without touching the sheet. [`claim_next`](scripts/utils.py#L234) honours the sentinel wherever it turns up in the queue, not just on a pod's first claim.

**2. Get raw data in.** [adap-koa-download-from-queue.yml](nautilus_jobs/adap-koa-download-from-queue.yml) runs [koa_download_from_queue.py](scripts/koa_download_from_queue.py) off the *target* queue. For each target a pod looks its ra/dec up in the `targets` tab (decimal degrees, columns D and E), restores any cached KOA query tables from `s3://pypeit/adap_2023/koa_queries/<target>/`, queries KOA, downloads the science, arc and flat frames for every night it finds, uploads the tree to `s3://pypeit/adap_2023/raw_data_reorg/`, and then **appends the datasets it downloaded and verified to column A of the `WorkQueue` tab** with a blank status — which is what closes the loop to the dataset queue and removes the hand transcription that used to sit between the two stages. That append takes the *dataset* queue's lock, `adap_2023_lock`, because that is the lock guarding that tab.

[scripts/download_lib/](scripts/download_lib/) does the querying and lays out the `<target>/<YYYYMMDD>/<LRIS|LRISBLUE>/raw_[rb]` tree. A **dataset** on this branch is that three-part path, and the red and blue arms are separate datasets. A target ends up `COMPLETE`, `NO DATA` or `FAILED`; `NO DATA` is not a failure, but is kept distinct so those rows can be found and their catalogue positions checked.

Concurrency against KOA is the **pod count**, exactly: `Koa.download` is a serial loop over a table's frames, so one pod holds one connection. `parallelism` in the Job is therefore the throttle, and [`koa_download_slot`](scripts/utils.py#L320) is a redis semaphore backstopping what `parallelism` cannot see — a second download job or a hand-run retry alongside the first.

(On the DEIMOS branches this stage is instead a hand-organized disk reorganized into `<mask>/<grating_angle_filter>/<date-range>/complete|incomplete/raw/` by a script that only exists on those branches.)

**3. Reduce.** This is the core. [adap-reduce-lris-from-queue.yml](nautilus_jobs/adap-reduce-lris-from-queue.yml) launches N parallel worker pods, each of which:

- updates the PypeIt checkout baked into the container (`git checkout lris_adap`, `pip install --no-build-isolation -e '.[dev]'`, records `PYPEIT_COMMIT`)
- clones `adap` **then overwrites `scripts/` and `config/` from S3** — see the gotcha below
- runs [reduce_from_queue.py](scripts/reduce_from_queue.py)

Every stage script shares one loop, [`run_task_on_queue`](scripts/utils.py#L254) in [utils.py](scripts/utils.py): claim an item off Redis (`brpop`, blocking, then non-blocking) → mark it `In Progress` in the sheet under a lock → run the task → write back `COMPLETE`/`FAILED`/`WARNING` → claim the next. A pod drains the queue and exits when it's empty. Failures are caught per-item, so one bad night never kills the pod.

The reduce task itself ([`reduce_dataset_task`](scripts/reduce_from_queue.py#L170)): parse the dataset name → download raw via rclone → [trimming_setup.py](scripts/trimming_setup.py) generates the `.pypeit` file (PypeIt classifies the frame types from the headers; calibrations are not trimmed, but `config/exclude_files.txt` entries and any `bias`/`dark` frames are commented out) → `run_pypeit` as a subprocess with memory sampled every 2s via psutil → tar the QA → [scorecard.py](scripts/scorecard.py) computes quality metrics → upload results to S3 *and* gdrive → [update_gsheet_scorecard.py](scripts/update_gsheet_scorecard.py) → `rmtree` the local copy so ephemeral storage doesn't fill.

[adap-reduce-one.yml](nautilus_jobs/adap-reduce-one.yml) runs the same script with `--dataset` to reduce one dataset without the queue, for debugging.

**4. Post-processing**, each its own queue-driven job with the same loop: [sensfunc_from_queue.py](scripts/sensfunc_from_queue.py) → [flux_coadd1d_from_queue.py](scripts/flux_coadd1d_from_queue.py). The coadd stage works at the dataset *prefix* level rather than on single datasets. Coadding is 1D only: [coadd2d_from_queue.py](scripts/depreciated/coadd2d_from_queue.py) is not in use and has moved to `scripts/depreciated/`. ([collate1d_from_queue.py](scripts/depreciated/collate1d_from_queue.py) is DEIMOS-era and deprecated alongside it.)

**5. Score and back up.** [run_scorecard_on_queue.py](scripts/run_scorecard_on_queue.py) re-scores without re-reducing; [sync_backup_from_queue.py](scripts/sync_backup_from_queue.py) and [backup_datasets.sh](scripts/backup_datasets.sh) mirror S3 → Drive; [archive.py](scripts/archive.py) is the long-term archive path.

## Three abstractions worth knowing

[`RClonePath`](scripts/rclone.py#L24) is a `pathlib.Path` lookalike for cloud storage — `/` composes, `.glob()`, `.rglob()`, `.upload()`, `.download()`, `.unlink()` all shell out to `rclone`. It means the same code addresses S3 and Drive interchangeably.

Config resolution is convention-over-configuration: [trimming_setup.py:167](scripts/trimming_setup.py#L167) loads `config/<spectrograph>_default_pypeit_config`, then globs for `config/<dataset-with-slashes-as-underscores>_*`. Drop a file named after a dataset into [config/](config/) and it overrides the default for that dataset only — no code change. The `_<suffix>` on that filename becomes the output subdirectory, so one dataset can produce several parallel reductions.

**What is on disk is the authority** for whether a download worked. `Koa.download` catches per-file errors itself, prints them and carries on, so it returns normally even when every frame failed — wrapping it in `try`/`except` catches almost nothing. [`verify_download`](scripts/download_lib/DownloadUtils.py) compares the directory against the `koaid` column of each final table *after* `file_cleanup` has run, and `check_night` collapses a night's science, arc and flat tables into one `COMPLETE`/`INCOMPLETE`/`FAILED` per dataset. Only datasets that verify `COMPLETE` are offered to the reduce stage.

## Gotchas I'd flag
- **The git repo is not what runs.** Every job does `git clone adap` and then `aws s3 cp s3://pypeit/adap/scripts_2023/ scripts/ --recursive`, which overwrites the checkout. S3 is the source of truth at runtime; the repo can silently drift from what's actually executing. Editing a script here has no effect until it's pushed to S3. This bites hardest on [download_lib/](scripts/download_lib/), whose modules import each other by bare name (`import Query`, `import Night`) — a partial copy in S3 fails at import.
- **Two S3 prefix families.** `s3://pypeit/adap/` holds the deployed scripts (`scripts_2023/`), config (`config_2023/`) and job logs; `s3://pypeit/adap_2023/` holds this campaign's raw data and KOA query cache. Every job now reads the `_2023`-suffixed prefixes, so one deploy updates them all — except [backup_datasets.yml](nautilus_jobs/backup_datasets.yml)'s `backup_list.txt`, which is hand-maintained in S3 and not in the repo.
- **Redis is reached through a Service.** [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) defines both the redis Deployment and an `adap-workqueue` Service, and every job connects to `redis://adap-workqueue:6379`. Applying the Deployment without the Service leaves the jobs unable to resolve the queue. There's no pod IP to paste into yamls on this branch. Do not scale it past `replicas: 1` — a second replica is a second, independent redis, and the lock stops serialising anything.
- **Nothing survives the redis pod.** It runs a bare `redis-server` with no volume, so both queues, the lock and the KOA slot pool are gone when the pod is replaced. Re-seed with `init` after any restart.
- **A killed download pod leaks a KOA slot**, permanently lowering the concurrency limit. The pool is self-seeding and guarded by a separate `_seeded` marker key (redis deletes an emptied list, so "pool missing" and "pool fully checked out" are otherwise indistinguishable), which means recovering needs *both* keys cleared: `redis-cli del adap_2023_targets_koa_slots adap_2023_targets_koa_slots_seeded`.
- **The file-based queue is still checked in and is inert.** [download_work_queue_from_gs.py](scripts/download_work_queue_from_gs.py), [init_workqueue.yml](nautilus_jobs/init_workqueue.yml), [refresh_workqueue.yml](nautilus_jobs/refresh_workqueue.yml) and [upload_workqueue_to_s3.yml](nautilus_jobs/upload_workqueue_to_s3.yml) are from the era when pods locked a CSV on a shared volume. Nothing reads that CSV any more — but reading the sheet still writes `IN QUEUE` back into it, so running one of those jobs marks rows queued that Redis knows nothing about.
- **The pre-queue KOA job is still in the tree.** [adap_koa_download.yml](nautilus_jobs/adap_koa_download.yml) reads a hand-uploaded `targets.txt` and writes nothing back to the sheet. It is retired in favour of the queue job — see [depreciated.rst](depreciated.rst) — but the file is still there to be applied by mistake. [download.py](scripts/download_lib/download.py)'s text-file entry point is deliberately kept, for local and by-hand runs.
- **[persist_volume.yml](nautilus_jobs/persist_volume.yml) is still required, for the wrong reason.** Redis holds the queue in memory, but four live job yamls still mount the `pypeit-adap-work-queue` PVC at `/work_queue` without reading it, and their pods won't schedule if the claim doesn't exist.
- **The sheet is addressed by name, not by key.** Every job passes `LRIS-ADAP/<tab>`, so the scorecard tabs — which are resolved from whichever spreadsheet the job was handed — all land in the same place. Keep it that way when adding a job.
- **`config/exclude_files.txt`** is read by [trimming_setup.py:164](scripts/trimming_setup.py#L164) for every dataset, with no existence check, so a deployed `config/` missing it fails every reduction. It is checked in holding only comments, which excludes nothing; a copy in `s3://pypeit/adap/config_2023/` overlays that one at runtime.
- [config/rclone.conf](config/rclone.conf) is committed and references a service-account JSON mounted from the `adap23-scorecard-gcloud` k8s secret; S3 creds come from the `prp-s3-credentials` secret. No keys are in the repo itself.

The clean version of the mental model: **the Google Sheet is the UI, Redis is the scheduler, S3 is the filesystem, and every stage is the same `run_task_on_queue` loop wrapped around a different PypeIt call — one loop over targets to fill the sheet, and one loop over datasets to work through it.**
