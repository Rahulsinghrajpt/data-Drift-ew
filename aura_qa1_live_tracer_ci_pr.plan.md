---
name: Aura QA1 live tracer CI
overview: "Align Aura QA1 live-tracer pytest with existing GitHub Actions: extend the reusable lambda PR workflow and the all-environment wrapper. Reuse the same AWS credential pattern as deploy-sls-lambda.yaml without editing that file. Fork PRs skip the S3-backed run."
todos:
  - id: live-fetch
    content: Add S3 list/get + CSV parse helper; pytest_sessionstart in conftest mutates `_QA_RAW['sample_csv']` when `AURA_QA1_LIVE_TRACER=1` and `LIVE_TRACER_BUCKET` (default `prd-mm-vendor-sync`).
    status: pending
  - id: deps
    content: Add `boto3` to [lambda/data-ingestion-pipeline/requirements-dev.txt](MikGorilla.AI/lambda/data-ingestion-pipeline/requirements-dev.txt).
    status: pending
  - id: reusable-workflow
    content: "Extend [pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml) only: optional input useLiveAuraQa1S3Sample (default false). When true, add configure-aws-credentials step mirroring deploy-sls-lambda (same secrets/region pattern); set env; run `python -m pytest tests/ -v`. Do not change deploy-sls-lambda.yaml."
    status: pending
  - id: wrapper-jobs
    content: "Update [pr-status-checks_all_wrapper.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_all_wrapper.yaml): pass useLiveAuraQa1S3Sample only for data-ingestion; fork guard `github.event.pull_request.head.repo.full_name == github.repository` for live job; keep offline pytest path for forks."
    status: pending
  - id: iam
    content: Confirm the same credentials/secrets used in CI can ListBucket/GetObject on `prd-mm-vendor-sync` prefix `tracer/` (no deploy workflow edits).
    status: pending
isProject: false
---

# Aura QA1 live tracer CI (aligned with current workflows)

## Do not change `deploy-sls-lambda.yaml`

[deploy-sls-lambda.yaml](MikGorilla.AI/.github/workflows/deploy-sls-lambda.yaml) already defines `**aws-actions/configure-aws-credentials@v4**` with `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and region. **Leave that file unchanged.** For the PR live-tracer job, **copy the same step shape and secrets** into [pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml) (conditional on `useLiveAuraQa1S3Sample`) so pytest can call S3—no coupling or edits to the deploy workflow.

## How CI works today (this repo)

**1. Per-base-branch “PR status” (Swaven only)** — these do **not** run your local pytest:

- [pr-status-checks_main.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_main.yaml) → `pull_request` to `main` → Swaven `reusable_pr-pre-merge-validation.yaml`
- [pr-status-checks_staging.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_staging.yaml) → base `staging`
- [pr-status-checks_integration.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_integration.yaml) → base `integration`

**2. Lambda lint + unit tests (local reusable)** — this is where **data-ingestion `pytest tests/ -v`** runs today:

- [pr-status-checks_all_wrapper.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_all_wrapper.yaml): `on: pull_request` with `branches: [main, staging, integration]`; job `lint-test-data-ingestion-pipeline` calls [pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml) with `functionLocationDir: lambda/data-ingestion-pipeline`.
- [pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml): `workflow_call` only — checkout, Python 3.13, deps, `flake8`, `**pytest tests/ -v**` (no AWS today).

**Implement live tracer only in:** `pr-status-checks-lambda.yaml` + `pr-status-checks_all_wrapper.yaml` (+ test `conftest` / `requirements-dev.txt`).

## Recommended integration shape

1. **Extend** `pr-status-checks-lambda.yaml` with optional boolean input `useLiveAuraQa1S3Sample` (default `false`).
2. In `**lint-test-python`**, when that input is `true`:
  - Add a step `**Configure AWS credentials**` — same action and `with:` block style as [deploy-sls-lambda.yaml](MikGorilla.AI/.github/workflows/deploy-sls-lambda.yaml) (lines 49–54 pattern), **not** by modifying that file.
  - `if: inputs.useLiveAuraQa1S3Sample`
  - Job may need `permissions: id-token: write, contents: read` only if you later switch that step to OIDC; static keys as in deploy-sls-lambda do not require OIDC.
  - Test step env: `AURA_QA1_LIVE_TRACER=1`, `LIVE_TRACER_BUCKET=prd-mm-vendor-sync`, `AWS_REGION` (e.g. `eu-west-1`).
  - Use `python -m pytest tests/ -v` if you want parity with local Windows invocations.
3. **Update** `pr-status-checks_all_wrapper.yaml`: pass `useLiveAuraQa1S3Sample: true` only for the data-ingestion job, with a fork guard (`head.repo.full_name == github.repository`) so the live job does not run on forks. Keep offline pytest for everyone (existing job unchanged, or duplicate job pattern as in prior plan revision).

Optional: `paths: lambda/data-ingestion-pipeline/`** on the live job to save CI minutes.

## Test code (`conftest` + deps)

- Add `**boto3**` to [requirements-dev.txt](MikGorilla.AI/lambda/data-ingestion-pipeline/requirements-dev.txt).
- In [conftest.py](MikGorilla.AI/lambda/data-ingestion-pipeline/tests/conftest.py), `**pytest_sessionstart**`: when `AURA_QA1_LIVE_TRACER` is set, list `tracer/` on `LIVE_TRACER_BUCKET`, match Aura QA1 using the same rules as [mmm-data-transfer `find_tracer_files` / `is_valid_client_file](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-data-transfer/index.py)`, pick latest `LastModified`, `get_object`, fill `_QA_RAW["sample_csv"]`.

**Bucket:** Confirm in AWS; defaults in code are `**prd-mm-vendor-sync`**, not `prd-mmm-vendor-sync`.

## Out of scope

- **No edits** to [deploy-sls-lambda.yaml](MikGorilla.AI/.github/workflows/deploy-sls-lambda.yaml).
- Swaven-only workflows unchanged.
- Live run uses latest S3 object at workflow time, not S3 event-driven timing.

