## The big picture

`adap` isn't a library — it's the **orchestration layer that runs [PypeIt](https://github.com/pypeit/PypeIt) at scale on the NRP/Nautilus Kubernetes cluster**. It has no code of its own that reduces spectra; it decides *what* to reduce, hands work to pods, moves data in and out of cloud storage, and scores the results.

This branch (`adap_2023`) is the Keck LRIS campaign. [workflow.rst](workflow.rst) is the operational procedure; this file is the mental model behind it.

Four external systems form the control plane:

| System | Role |
|---|---|
| **Google Sheet** | Human-facing dashboard. One spreadsheet named `Scorecard`, one tab per stage. Column A = dataset names, a status column = `IN QUEUE` / `In Progress` / `COMPLETE` / `FAILED` / `WARNING`, plus scorecard tabs |
| **Redis** (in-cluster) | The actual work queue + distributed lock. Sheet API has no locking, so Redis arbitrates between parallel pods |
| **S3** (Ceph via `rook-ceph-rgw-nautiluss3.rook`) | Primary data store: raw data, reduce products, logs |
| **Google Drive** | Secondary backup of results, via rclone |

## The four directories
- [nautilus_jobs/](nautilus_jobs/) — Kubernetes `Job`/`Deployment` YAML. One file per pipeline stage.
- [scripts/](scripts/) — the Python that actually runs inside the pods.
- [config/](config/) — PypeIt parameter files (per-spectrograph defaults + per-dataset overrides) and [rclone.conf](config/rclone.conf).
- [kube_tests/](kube_tests/) — a scratch pod for interactive debugging on the cluster.

## The workflow

**1. Stand up the queue.** [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) is the Redis server plus the `adap-workqueue` Service that fronts it. The queue is then seeded by pushing the sentinel string `init` onto it with `redis-cli`; the first pod to claim `init` calls [`init_work_queue`](scripts/utils.py#L166), which reads the `WorkQueue` tab and pushes every dataset with a blank status, marking those rows `IN QUEUE`. Dataset names can also be pushed directly to run a specific set without touching the sheet.

**2. Get raw data in.** [adap_koa_download.yml](nautilus_jobs/adap_koa_download.yml) pulls from the Keck Observatory Archive straight into `s3://pypeit/adap_2023/raw_data_reorg/`, which is the root every later stage reads from. [scripts/download_lib/](scripts/download_lib/) is the target-name/coordinate-driven KOA querier that does the work and lays out the `<target>/<YYYYMMDD>/<LRIS|LRISBLUE>/raw_[rb]` tree. A **dataset** on this branch is that three-part path, and the red and blue arms are separate datasets.

(On the DEIMOS branches this stage is instead a hand-organized disk reorganized into `<mask>/<grating_angle_filter>/<date-range>/complete|incomplete/raw/` by a script that only exists on those branches.)

**3. Reduce.** This is the core. [adap-reduce-lris-from-queue.yml](nautilus_jobs/adap-reduce-lris-from-queue.yml) launches N parallel worker pods, each of which:

- updates the PypeIt checkout baked into the container (`git checkout lris_adap`, `pip install --no-build-isolation -e '.[dev]'`, records `PYPEIT_COMMIT`)
- clones `adap` **then overwrites `scripts/` and `config/` from S3** — see the gotcha below
- runs [reduce_from_queue.py](scripts/reduce_from_queue.py)

Every stage script shares one loop, [`run_task_on_queue`](scripts/utils.py#L210) in [utils.py](scripts/utils.py): claim a dataset off Redis (`brpop`, blocking, then non-blocking) → mark it `In Progress` in the sheet under a lock → run the task → write back `COMPLETE`/`FAILED`/`WARNING` → claim the next. A pod drains the queue and exits when it's empty. Failures are caught per-dataset, so one bad night never kills the pod.

The reduce task itself ([`reduce_dataset_task`](scripts/reduce_from_queue.py#L170)): parse the dataset name → download raw via rclone → [trimming_setup.py](scripts/trimming_setup.py) generates the `.pypeit` file (selecting the *best* arcs/flats by lamp set, exposure time, elevation, MJD, rather than using everything) → `run_pypeit` as a subprocess with memory sampled every 2s via psutil → tar the QA → [scorecard.py](scripts/scorecard.py) computes quality metrics → upload results to S3 *and* gdrive → [update_gsheet_scorecard.py](scripts/update_gsheet_scorecard.py) → `rmtree` the local copy so ephemeral storage doesn't fill.

[adap-reduce-one.yml](nautilus_jobs/adap-reduce-one.yml) runs the same script with `--dataset` to reduce one dataset without the queue, for debugging.

**4. Post-processing**, each its own queue-driven job with the same loop: [sensfunc_from_queue.py](scripts/sensfunc_from_queue.py) → [flux_coadd1d_from_queue.py](scripts/flux_coadd1d_from_queue.py) → [coadd2d_from_queue.py](scripts/coadd2d_from_queue.py). The two coadd stages work at the dataset *prefix* level rather than on single datasets. ([collate1d_from_queue.py](scripts/collate1d_from_queue.py) is DEIMOS-era and deprecated on this branch.)

**5. Score and back up.** [run_scorecard_on_queue.py](scripts/run_scorecard_on_queue.py) re-scores without re-reducing; [sync_backup_from_queue.py](scripts/sync_backup_from_queue.py) and [backup_datasets.sh](scripts/backup_datasets.sh) mirror S3 → Drive; [archive.py](scripts/archive.py) is the long-term archive path.

## Two abstractions worth knowing

[`RClonePath`](scripts/rclone.py#L24) is a `pathlib.Path` lookalike for cloud storage — `/` composes, `.glob()`, `.rglob()`, `.upload()`, `.download()`, `.unlink()` all shell out to `rclone`. It means the same code addresses S3 and Drive interchangeably.

Config resolution is convention-over-configuration: [trimming_setup.py:412](scripts/trimming_setup.py#L412) loads `config/<spectrograph>_default_pypeit_config`, then globs for `config/<dataset-with-slashes-as-underscores>_*`. Drop a file named after a dataset into [config/](config/) and it overrides the default for that dataset only — no code change. The `_<suffix>` on that filename becomes the output subdirectory, so one dataset can produce several parallel reductions.

## Gotchas I'd flag
- **The git repo is not what runs.** Every job does `git clone adap` and then `aws s3 cp s3://pypeit/adap/scripts_2023/ scripts/ --recursive`, which overwrites the checkout. S3 is the source of truth at runtime; the repo can silently drift from what's actually executing. Editing a script here has no effect until it's pushed to S3.
- **Two S3 prefix families.** `s3://pypeit/adap/` holds the deployed scripts, config and job logs; `s3://pypeit/adap_2023/` holds this campaign's raw data and KOA target list. Two jobs ([init_workqueue.yml](nautilus_jobs/init_workqueue.yml), [backup_datasets.yml](nautilus_jobs/backup_datasets.yml)) still pull scripts from the un-suffixed `s3://pypeit/adap/scripts/`, so deploying to `scripts_2023/` doesn't update them.
- **Redis is reached through a Service.** [workqueue_deployment.yml](nautilus_jobs/workqueue_deployment.yml) defines both the redis Deployment and an `adap-workqueue` Service, and every job connects to `redis://adap-workqueue:6379`. Applying the Deployment without the Service leaves the jobs unable to resolve the queue. There's no pod IP to paste into yamls on this branch.
- **The file-based queue is still checked in and is inert.** [download_work_queue_from_gs.py](scripts/download_work_queue_from_gs.py), [init_workqueue.yml](nautilus_jobs/init_workqueue.yml), [refresh_workqueue.yml](nautilus_jobs/refresh_workqueue.yml) and [upload_workqueue_to_s3.yml](nautilus_jobs/upload_workqueue_to_s3.yml) are from the era when pods locked a CSV on a shared volume. Nothing reads that CSV any more — but reading the sheet still writes `IN QUEUE` back into it, so running one of those jobs marks rows queued that Redis knows nothing about.
- **[persist_volume.yml](nautilus_jobs/persist_volume.yml) is still required, for the wrong reason.** Redis holds the queue in memory, but four live job yamls still mount the `pypeit-adap-work-queue` PVC at `/work_queue` without reading it, and their pods won't schedule if the claim doesn't exist.
- **The YAMLs disagree about which spreadsheet to use.** The image, the adap branch (`adap_2023`) and the PypeIt branch (`lris_adap`) are now consistent across all jobs, but the sheet argument isn't: reduce jobs use `key=15ealTQOBLB0I…`, sensfunc/flux/stage-raw use `key=1TADKd3OgbA…`, and coadd2d/scorecard/sync-backups use `Scorecard/…`. All of them should be `Scorecard/<tab>`. Since the scorecard tabs are resolved from whichever spreadsheet the job was handed, stages pointed at different spreadsheets write status where nobody is looking.
- **Two jobs can't be applied at all.** [init_workqueue.yml](nautilus_jobs/init_workqueue.yml) and [adap_flux_codd1d_from_queue.yml](nautilus_jobs/adap_flux_codd1d_from_queue.yml) have underscores in their `metadata.name`, which isn't legal for a Kubernetes object, so `kubectl create` rejects them.
- **[adap-reduce-from-queue.yml](nautilus_jobs/adap-reduce-from-queue.yml) is a stale duplicate** of the `-lris-` one, differing only by a `PYPEIT_COMMMIT` typo that logs the commit as an empty string.
- **`config/exclude_files.txt`** is read by [trimming_setup.py:410](scripts/trimming_setup.py#L410) but isn't in the repo — it comes from `s3://pypeit/adap/config_2023/` at runtime.
- [config/rclone.conf](config/rclone.conf) is committed and references a service-account JSON mounted from the `adap23-scorecard-gcloud` k8s secret; S3 creds come from the `prp-s3-credentials` secret. No keys are in the repo itself.

The clean version of the mental model: **the Google Sheet is the UI, Redis is the scheduler, S3 is the filesystem, and every stage is the same `run_task_on_queue` loop wrapped around a different PypeIt call.**
