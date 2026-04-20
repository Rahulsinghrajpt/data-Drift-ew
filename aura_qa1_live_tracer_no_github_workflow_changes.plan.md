---
name: Aura QA1 live tracer without workflow changes
overview: Implement code-only behavior so unit tests can load the latest Aura QA1 tracer CSV from prd-mm-vendor-sync when you run pytest with AWS credentials and an env flag. No edits to any file under .github/workflows.
todos:
  - id: loader-module
    content: Add a small module under tests/ (e.g. tests/live_tracer_loader.py) that uses boto3 to list tracer/, match auraqa1 + brand + retailer like production find_tracer_files, pick latest LastModified, get_object, return headers + rows for sample_csv.
    status: pending
  - id: conftest-hook
    content: In conftest.py, after loading aura_qa1.json, if env AURA_QA1_USE_LIVE_TRACER is truthy (e.g. 1/true), call loader; on success replace _QA_RAW['sample_csv'] and set os.environ TRACER_BUCKET to live bucket; on failure log and keep static JSON (or fail fast—pick one and document).
    status: pending
  - id: requirements
    content: Add boto3 to lambda/data-ingestion-pipeline/requirements-dev.txt.
    status: pending
  - id: docs
    content: "Short comment block at top of conftest or README snippet in pipeline folder: required env vars, aws sso login / profile, example PowerShell/bash commands. No .github file changes."
    status: pending
isProject: false
---

# Aura QA1 live tracer in unit tests (no GitHub workflow changes)

## Your requirement (in English)

- You do **not** want to change **any** Git workflow files (nothing under `.github/workflows/`).
- You want: whenever Aura QA1 data is present in `**prd-mm-vendor-sync`**, and whenever you **raise a PR** (or more precisely: whenever you **run the unit tests**), that **dataset should be used directly** in the unit tests instead of only the static rows in `aura_qa1.json`.

## What we will implement (repository code only)

1. `**boto3`** in `[lambda/data-ingestion-pipeline/requirements-dev.txt](MikGorilla.AI/lambda/data-ingestion-pipeline/requirements-dev.txt)` so tests can call S3 from your machine (or any runner that already has AWS credentials in the environment).
2. **A small loader** (e.g. `[tests/live_tracer_loader.py](MikGorilla.AI/lambda/data-ingestion-pipeline/tests/live_tracer_loader.py)`) that:
  - Uses bucket `**prd-mm-vendor-sync`** (override with env `LIVE_TRACER_BUCKET` if needed).
  - Lists prefix `tracer/`, applies the same **matching and exclusion** ideas as production (`[find_tracer_files` / `is_valid_client_file](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-data-transfer/index.py)`) for `**client_id` / `brand_id` / `retailer_id`** read from `[aura_qa1.json](MikGorilla.AI/lambda/data-ingestion-pipeline/tests/fixtures/aura_qa1.json)`.
  - Selects the **newest** object by S3 `LastModified` (and size tie-break like production).
  - Downloads the CSV, parses **header line + data lines**, and returns a structure compatible with `_QA_RAW["sample_csv"]` (`headers`, `rows`, `row_count`).
3. `**conftest.py`** hook (recommended: `**pytest_sessionstart`**) that:
  - If `**AURA_QA1_USE_LIVE_TRACER`** is set to a truthy value (e.g. `1`), tries the loader.
  - **On success:** replaces `_QA_RAW["sample_csv"]` (and optionally sets `os.environ["TRACER_BUCKET"]` to the live bucket for code that reads env).
  - **On failure** (no credentials, no network, no matching file): either **fall back** to the committed JSON (safe default for CI) or **fail** the session (strict mode). The plan default is **fallback + print one clear warning** so existing CI keeps passing without workflow edits.
4. **Documentation** (only inside the pipeline tree, e.g. a short section in existing `[UNIT_TEST_COVERAGE.md](MikGorilla.AI/lambda/data-ingestion-pipeline/UNIT_TEST_COVERAGE.md)` or comments in `conftest`): how to run locally with live data, e.g. after `aws sso login` or configured profile:

```bash
   export AURA_QA1_USE_LIVE_TRACER=1
   export AWS_REGION=eu-west-1
   python -m pytest tests/ -v
   

```

## Important limitation (because workflows are unchanged)

GitHub Actions **does not** inject `AWS_ACCESS_KEY_ID` / role credentials into your current `[pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml)` job. So **on GitHub-hosted CI**, unless something else provides credentials (unusual without workflow or org-level changes), **boto3 will not be able to read `prd-mm-vendor-sync`**, and tests will keep using the **static** `aura_qa1.json` sample (fallback).

**What you get without touching workflows:**


| Where you run pytest        | With `AURA_QA1_USE_LIVE_TRACER=1` + valid AWS creds                |
| --------------------------- | ------------------------------------------------------------------ |
| Your laptop / dev box       | Latest Aura QA1 tracer from `prd-mm-vendor-sync` is used in tests. |
| Default GitHub PR check job | Still static fixture (no AWS); PR still validates logic.           |


If you later want **the same live behavior on every PR in GitHub**, that **requires** a workflow (or org-level runner) change to supply credentials—by your rule, that is out of scope for this plan.

## Out of scope (explicit)

- **No** edits to `[deploy-sls-lambda.yaml](MikGorilla.AI/.github/workflows/deploy-sls-lambda.yaml)`, `[pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml)`, `[pr-status-checks_all_wrapper.yaml](MikGorilla.AI/.github/workflows/pr-status-checks_all_wrapper.yaml)`, or any other file under `.github/`.

## Verification

- With flag **off**: `pytest` behaves exactly as today (static JSON).
- With flag **on** and valid IAM (read `tracer/` on `prd-mm-vendor-sync`): `_QA_RAW["sample_csv"]` reflects the latest file; run a couple of existing tests that use `qa_sample_csv_bytes` / `qa_sample_df` to confirm.

