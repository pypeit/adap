## The big picture

`adap` isn't a library — it's the **orchestration layer that runs [PypeIt](https://github.com/pypeit/PypeIt) at scale on the NRP/Nautilus Kubernetes cluster**. It has no code of its own that reduces spectra; it decides *what* to reduce, hands work to pods, moves data in and out of cloud storage, and scores the results.

Four external systems form the control plane:

| System | Role |
|---|---|
| **Google Sheet** | Human-facing dashboard. Column A = dataset names, a status column = `IN QUEUE` / `In Progress` / `COMPLETE` / `FAILED`, plus scorecard tabs |
| **Redis** (in-cluster) | The actual work queue + distributed lock. Sheet API has no locking, so Redis arbitrates between parallel pods |
| **S3** (Ceph via `rook-ceph-rgw-nautiluss3.rook`) | Primary data store: raw data, reduce products, logs |
| **Google Drive** | Secondary backup of results, via rclone |

## The four directories
- [nautilus_jobs/](nautilus_jobs/) — Kubernetes `Job`/`Deployment` YAML. One file per pipeline stage.
- [scripts/](scripts/) — the Python that actually runs inside the pods.
- [config/](config/) — PypeIt parameter files (per-spectrograph defaults + per-dataset overrides) and [rclone.conf](config/rclone.conf).
- [kube_tests/](kube_tests/) — a scratch pod for interactive debugging on the cluster.

## The workflow

**1. Seed the queue.** [init_workqueue.yml](nautilus_jobs/init_workqueue.yml) reads the Google Sheet and pushes every dataset with a blank status into Redis, marking it `IN QUEUE` in the sheet. [refresh_workqueue.yml](nautilus_jobs/refresh_workqueue.yml) does the same additively (`--refresh`), so re-running doesn't clobber in-flight work. [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) is the Redis server itself.

**2. Get raw data in.** [adap_koa_download.yml](nautilus_jobs/adap_koa_download.yml) pulls from the Keck Observatory Archive into S3. [scripts/download_lib/](scripts/download_lib/) is the standalone target-name/coordinate-driven KOA querier. [adap_reorg_setup.py](scripts/adap_reorg_setup.py) then reorganizes flat raw dirs into the canonical hierarchy — `<mask>/<grating_angle_filter>/<date-range>/complete|incomplete/raw/` — where `complete` means "enough calibrations to actually reduce."

**3. Reduce.** This is the core. [adap-reduce-from-queue.yml](nautilus_jobs/adap-reduce-from-queue.yml) launches N parallel worker pods, each of which:

- builds its environment on the fly (checks out a PypeIt branch, `pip install -e .`, records `PYPEIT_COMMIT`)
- clones `adap` **then overwrites `scripts/` and `config/` from S3** — see the gotcha below
- runs [reduce_from_queue.py](scripts/reduce_from_queue.py)

Every stage script shares one loop, [`run_task_on_queue`](scripts/utils.py#L210) in [utils.py](scripts/utils.py): claim a dataset off Redis (`brpop`, blocking, then non-blocking) → mark it `In Progress` in the sheet under a lock → run the task → write back `COMPLETE`/`FAILED`/`WARNING` → claim the next. A pod drains the queue and exits when it's empty. Failures are caught per-dataset, so one bad night never kills the pod.

The reduce task itself ([`reduce_dataset_task`](scripts/reduce_from_queue.py#L170)): parse the dataset name → download raw via rclone → [trimming_setup.py](scripts/trimming_setup.py) generates the `.pypeit` file (selecting the *best* arcs/flats by lamp set, exposure time, elevation, MJD, rather than using everything) → `run_pypeit` as a subprocess with memory sampled every 2s via psutil → tar the QA → [scorecard.py](scripts/scorecard.py) computes quality metrics → upload results to S3 *and* gdrive → [update_gsheet_scorecard.py](scripts/update_gsheet_scorecard.py) → `rmtree` the local copy so ephemeral storage doesn't fill.

**4. Post-processing**, each its own queue-driven job with the same loop: [sensfunc_from_queue.py](scripts/sensfunc_from_queue.py) → [flux_coadd1d_from_queue.py](scripts/flux_coadd1d_from_queue.py) → [coadd2d_from_queue.py](scripts/coadd2d_from_queue.py) → [collate1d_from_queue.py](scripts/collate1d_from_queue.py).

**5. Score and back up.** [run_scorecard_on_queue.py](scripts/run_scorecard_on_queue.py) re-scores without re-reducing; [sync_backup_from_queue.py](scripts/sync_backup_from_queue.py) and [backup_datasets.sh](scripts/backup_datasets.sh) mirror S3 → Drive; [archive.py](scripts/archive.py) is the long-term archive path.

## Two abstractions worth knowing

[`RClonePath`](scripts/rclone.py#L24) is a `pathlib.Path` lookalike for cloud storage — `/` composes, `.glob()`, `.rglob()`, `.upload()`, `.download()`, `.unlink()` all shell out to `rclone`. It means the same code addresses S3 and Drive interchangeably.

Config resolution is convention-over-configuration: [trimming_setup.py:411](scripts/trimming_setup.py#L411) loads `config/<spectrograph>_default_pypeit_config`, then globs for `config/<dataset-with-slashes-as-underscores>_*`. Drop a file named after a dataset into [config/](config/) and it overrides the default for that dataset only — no code change. The `_<suffix>` on that filename becomes the output subdirectory, so one dataset can produce several parallel reductions.

## Gotchas I'd flag
- **The git repo is not what runs.** Every job does `git clone adap` and then `aws s3 cp s3://pypeit/adap/scripts_2023/ scripts/ --recursive`, which overwrites the checkout. S3 is the source of truth at runtime; the repo can silently drift from what's actually executing. Editing a script here has no effect until it's pushed to S3.
- **Redis is reached through a Service.** [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) defines both the redis Deployment and an `adap-workqueue` Service, and every job connects to `redis://adap-workqueue:6379`. Applying the Deployment without the Service leaves the jobs unable to resolve the queue.
- **The YAMLs disagree with each other.** PypeIt branches vary by job (`lris_adap`, `develop`, `hires_flux_coadd1d`, tag `1.13.0`), as do adap branches (`adap_2023`, `adap_2020`, `utils_test`, `main`) and Google Sheet keys. The older jobs (`coadd2d`, `scorecard`, `sync-backups`, `stage-raw`) still point at `utils_test` and a 2020-era sheet, so they're stale relative to the `adap_2023` line.
- **`config/exclude_files.txt`** is read by [trimming_setup.py:409](scripts/trimming_setup.py#L409) but isn't in the repo — it comes from `s3://pypeit/adap/config_2023/` at runtime.
- [config/rclone.conf](config/rclone.conf) is committed and references a service-account JSON mounted from the `adap23-scorecard-gcloud` k8s secret; S3 creds come from the `prp-s3-credentials` secret. No keys are in the repo itself.

The clean version of the mental model: **the Google Sheet is the UI, Redis is the scheduler, S3 is the filesystem, and every stage is the same `run_task_on_queue` loop wrapped around a different PypeIt call.**
