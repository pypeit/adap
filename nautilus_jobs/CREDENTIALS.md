# Credentials the ADAP jobs need

Every job in this directory needs at most two credentials inside the cluster (a
Google service account and a set of Nautilus S3 keys), plus a kubeconfig for
whoever applies them and registry access for the container image. This is the
full list, what reads each one, and how it has to be installed.

## 1. Google service account JSON — secret `adap23-scorecard-gcloud`

One credential covers both Google Sheets and Google Drive:

| Used for | By |
|---|---|
| Reading the work queue, writing status and scorecard rows | `gspread`, via [gspread_utils.py](../scripts/gspread_utils.py) and `update_gsheet_status` / `init_work_queue` in [utils.py](../scripts/utils.py) |
| Uploading results and backups to the shared Drive | rclone's `gdrive` remote in [config/rclone.conf](../config/rclone.conf) |

**The mount path is not negotiable.** `gspread_utils.open_spreadsheet` calls
`gspread.service_account()` with no filename, so gspread looks in its built-in
default location, `$HOME/.config/gspread/service_account.json`, and
`config/rclone.conf` names that same path for the Drive remote. No command line option
points it anywhere else. This is also why the container sets
`ENV HOME=/home/pypeitusr` — see [config/pypeit_lris_adap.docker](../config/pypeit_lris_adap.docker).

Create it with the key named `credentials`, which is what every job mounts with
`subPath: credentials`:

```
kubectl create secret generic adap23-scorecard-gcloud \
    --from-file=credentials=/path/to/service_account.json
```

Three things must also be granted on the Google side; the JSON alone is not enough:

- The **Sheets API and Drive API** must be enabled in the service account's GCP project.
- Each spreadsheet must be **shared with the service account's `client_email`** as an
  Editor, since the jobs write status columns back. That covers every sheet named in
  the job yamls (`15ealTQOBLB0I…`, `1TADKd3OgbA…`, and the `Scorecard` sheets).
- The service account must be a **member of the shared drive** named by
  `team_drive` in `config/rclone.conf`, with permission to add content, or the
  `gdrive:` uploads in [reduce_from_queue.py](../scripts/reduce_from_queue.py),
  [sync_backup_from_queue.py](../scripts/sync_backup_from_queue.py) and
  [backup_datasets.sh](../scripts/backup_datasets.sh) will fail.

Mounted by every job except `adap_koa_download.yml` and `upload_workqueue_to_s3.yml`,
which only use S3.

## 2. Nautilus Ceph S3 keys — secret `prp-s3-credentials`

Read three different ways, all from the same file: the `aws` CLI in each job's
script, rclone's `s3` remote (`env_auth = true`, which falls through to the shared
credentials file), and `boto3` in [cloudstorage.py](../scripts/cloudstorage.py).

It must be an AWS ini file with a **`[default]` profile** — no `AWS_PROFILE` is set
anywhere, so all three consumers resolve the default profile:

```
[default]
aws_access_key_id = <key>
aws_secret_access_key = <secret>
```

```
kubectl create secret generic prp-s3-credentials \
    --from-file=credentials=/path/to/aws_credentials_ini
```

Also keyed `credentials`, mounted at `/home/pypeitusr/.aws/credentials`. The keys
need read and write access to the `pypeit` bucket (`s3://pypeit/adap/…` and
`s3://pypeit/adap_2023/…`). Every job mounts this secret.

`ENDPOINT_URL` and `S3_ENDPOINT` in the job yamls are endpoint configuration rather
than credentials: `ENDPOINT_URL` is what the `aws` calls and the scripts'
`--endpoint_url` default use, and nothing in `scripts/` reads `S3_ENDPOINT`. To run
the same scripts from outside the cluster, use
[config/rclone_external_s3.conf](../config/rclone_external_s3.conf), which points at
`https://s3-west.nrp-nautilus.io` with the same keys.

## 3. Your Nautilus kubeconfig

No file here references it, but nothing can be applied without it: the config from
the NRP portal, normally in `~/.kube/config`. Two consequences for setup:

- **Secrets are namespace scoped.** None of these yamls set a `namespace`, so they
  are created in the current context's namespace, and both secrets above must exist
  in that same namespace. The `adap-workqueue` Service name likewise only resolves
  from inside it.
- Nautilus requires pods to run as non-root, which is why every job sets
  `runAsUser: 1001` and the credentials are mounted under `/home/pypeitusr`.

## 4. GitLab registry access for the container image

All jobs run `gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023`,
built from [config/pypeit_lris_adap.docker](../config/pypeit_lris_adap.docker).

**Pushing** it requires `docker login gitlab-registry.nrp-nautilus.io` with a GitLab
personal access token scoped `write_registry`.

**Pulling** requires nothing extra as long as that project's registry is public. If
it is private, the pods will fail with `ImagePullBackOff` and need a pull secret:

```
kubectl create secret docker-registry gitlab-registry \
    --docker-server=gitlab-registry.nrp-nautilus.io \
    --docker-username=<deploy-token-username> \
    --docker-password=<deploy-token>
```

referenced by adding `imagePullSecrets: [{name: gitlab-registry}]` to each pod spec.

## What needs no credential

- **KOA downloads.** [download_lib/Query.py](../scripts/download_lib/Query.py) and
  [koa_download.py](../scripts/koa_download.py) call `Koa.query_*` and `Koa.download`
  with no `cookiepath`, so only public archive data is reachable. Proprietary data
  would need a `Koa.login` cookie, which nothing here sets up.
- **GitHub clones** of PypeIt and adap, which use public HTTPS.
- **Redis.** No `requirepass` is set, so the work queue is protected only by being a
  ClusterIP Service inside the namespace. Anything in the namespace can read or
  drain it.

## Minimum sequence for a working setup

```
kubectl create secret generic adap23-scorecard-gcloud --from-file=credentials=service_account.json
kubectl create secret generic prp-s3-credentials    --from-file=credentials=aws_credentials
# share the sheets with the service account email, and add it to the shared drive

docker build --platform linux/amd64 \
    -t gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023 \
    -f config/pypeit_lris_adap.docker .
docker push gitlab-registry.nrp-nautilus.io/bradh/pypeitcontainers/pypeit_lris_adap:adap_2023

kubectl apply -f nautilus_jobs/persist_volume.yml
kubectl apply -f nautilus_jobs/workqueue_deployment.yml   # Deployment and Service
kubectl apply -f nautilus_jobs/init_workqueue.yml
kubectl apply -f nautilus_jobs/adap-reduce-lris-from-queue.yml
```
