---
name: Local Unit Tests First
overview: Create pytest unit tests for all 4 data-ingestion-pipeline lambdas using a local configurable JSON fixture for auraqa1 test data. No AWS credentials needed. Run and verify locally first; CI auto-runs on PR.
todos:
  - id: infra
    content: Create pytest.ini, update requirements-dev.txt, create tests/ directory skeleton with all __init__.py files
    status: completed
  - id: fixture
    content: Create tests/fixtures/aura_qa1.json with all auraqa1 configurable test values
    status: completed
  - id: conftest
    content: Create tests/conftest.py -- load fixture, provide all shared pytest fixtures and sys.path setup
    status: completed
  - id: test-get-client
    content: Write tests/mmm-get-client/test_index.py (11 tests)
    status: completed
  - id: test-stale
    content: Write tests/mmm-stale-data-check/test_index.py (10 tests)
    status: completed
  - id: test-slack
    content: Write tests/mmm-data-ingestion-slack/test_index.py (39 tests)
    status: completed
  - id: test-transfer
    content: Write tests/mmm-data-transfer/test_index.py (50 tests)
    status: completed
  - id: run-local
    content: Run pytest tests/ -v locally and fix any failures
    status: completed
isProject: false
---

# Data Ingestion Pipeline -- Local Unit Tests (Phase 1)

## Approach

- All test data lives in a single local JSON fixture file (`aura_qa1.json`)
- No AWS calls at test time -- everything is mocked
- Tests run locally with `pytest tests/ -v`
- CI already auto-runs on PR (no workflow changes needed)
- Later: add S3 auto-pickup as Phase 2 enhancement

---

## File Structure

```
data-ingestion-pipeline/
  pytest.ini
  requirements-dev.txt                 # add pytest-mock
  tests/
    __init__.py
    conftest.py                        # loads fixture, provides shared fixtures
    fixtures/
      aura_qa1.json                    # all auraqa1 test values
    mmm-get-client/
      __init__.py
      test_index.py                    # 11 tests
    mmm-stale-data-check/
      __init__.py
      test_index.py                    # 10 tests
    mmm-data-ingestion-slack/
      __init__.py
      test_index.py                    # 39 tests
    mmm-data-transfer/
      __init__.py
      test_index.py                    # 50 tests
```

---

## Step 1: Infrastructure files

`**pytest.ini**` at `data-ingestion-pipeline/`:

```ini
[pytest]
testpaths = tests
pythonpath = .
```

`**requirements-dev.txt**` -- add `pytest-mock` to existing `flake8` + `pytest`:

```
flake8
pytest
pytest-mock
```

---

## Step 2: `tests/fixtures/aura_qa1.json`

Single configurable file. When auraqa1 data changes, update this file and all tests adapt.

```json
{
  "client": {
    "client_id": "auraqa1",
    "brand_id": "auraqa1-US",
    "brand_name": "auraqa1-US",
    "retailer_id": "walmart",
    "retailer_name": "Walmart",
    "country": "US"
  },
  "environment": {
    "stage": "qa",
    "aws_region": "eu-west-1",
    "pipeline_info_table": "mmm-qa-pipeline-infos",
    "transfer_logs_table": "mmm-qa-data-transfer-logs",
    "audit_logs_bucket": "mmm-qa-audit-logs",
    "tracer_bucket": "qa-mm-vendor-sync",
    "vip_bucket_prefix": "mmm-qa-data-auraqa1",
    "slack_lambda_name": "sls-data-ingestion-pipeline-mmm-data-ingestion-slack-qa"
  },
  "pipeline_info_record": { ... },
  "sot_schema": { "column_order": [...], "required_columns": [...], "media_channels": [...] },
  "sample_csv": { "headers": "...", "rows": [...], "row_count": 5 },
  "stale_data": { ... },
  "slack_alerts": { "alert_types": [...] },
  "validation": { ... }
}
```

Full JSON shown in previous plan -- all values are for `client_id=auraqa1`.

---

## Step 3: `tests/conftest.py`

Loads `aura_qa1.json` once (session-scoped) and provides fixtures:

- `qa_config` -- full dict
- `qa_client` -- `qa_config["client"]`
- `qa_env` -- `qa_config["environment"]`
- `qa_pipeline_info` -- DynamoDB-shaped record from fixture
- `qa_sot_columns` -- column order list
- `qa_sample_csv_bytes` -- CSV bytes built from `sample_csv.headers + rows`
- `qa_sample_df` -- pandas DataFrame (skipped if pandas not available)
- `mock_context` -- fake Lambda context
- `mock_boto3_env` -- patches `os.environ` with QA env vars

Also adds `sys.path` entries for all 4 lambda dirs.

---

## Step 4: Tests per Lambda

### 4A. `tests/mmm-get-client/test_index.py` (11 tests)

Source: [mmm-get-client/index.py](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-get-client/index.py)

Functions: `handler` (L385), `validate_table_exists` (L242), `fetch_metadata_active_clients` (L264), `format_client_metadata` (L324), `write_log_to_s3` (L212)

- `test_handler_returns_active_clients_success` -- mock DynamoDB scan returns auraqa1 record, assert 200 + client list
- `test_handler_returns_no_active_clients` -- empty scan, assert empty clients
- `test_handler_dynamodb_error_raises` -- scan raises ClientError, assert RuntimeError
- `test_validate_table_exists_success` -- mock Table(), assert returns table
- `test_validate_table_exists_missing_table` -- ResourceNotFoundException
- `test_fetch_active_clients_filters_inactive` -- mix active/inactive, only active returned
- `test_fetch_active_clients_paginates` -- two pages via LastEvaluatedKey
- `test_format_client_metadata_parses_brand_retailer_key` -- `"auraqa1-US#walmart"` -> brand_id + retailer_id
- `test_format_client_metadata_all_fields` -- all fixture fields in output
- `test_write_log_to_s3_success` -- put_object called with correct bucket
- `test_write_log_to_s3_error_swallowed` -- ClientError does not propagate

### 4B. `tests/mmm-stale-data-check/test_index.py` (10 tests)

Source: [mmm-stale-data-check/index.py](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-stale-data-check/index.py)

Functions: `handler` (L172), `get_stale_clients` (L72)

- `test_handler_returns_stale_clients` -- old last_data_updated, assert stale list
- `test_handler_empty_when_all_fresh` -- today's date, assert empty
- `test_handler_custom_threshold_from_event` -- event overrides threshold
- `test_handler_default_threshold_from_env` -- env var used
- `test_get_stale_clients_filters_inactive` -- only is_active=1
- `test_get_stale_clients_skips_no_date` -- missing last_data_updated skipped
- `test_get_stale_clients_days_stale_correct` -- delta matches expected
- `test_get_stale_clients_enriches_brand_retailer` -- parses brand_retailer_key
- `test_handler_critical_absence` -- beyond error threshold
- `test_handler_dynamodb_error_returns_dict` -- error key, no raise

### 4C. `tests/mmm-data-ingestion-slack/test_index.py` (39 tests)

Source: [mmm-data-ingestion-slack/index.py](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-data-ingestion-slack/index.py)

Groups:

- **Webhook** (2): resolve from Secrets Manager, fallback to env
- **Handler dispatch** (3): parametrized alert_type routing, default completion path, 500 on exception
- **send_slack_notification** (3): POST to webhook, retry on failure, false after max retries
- **format_slack_message** (3): success/partial colors, cap at 10 clients
- **extract_results_from_event** (4): SUCCESS/PARTIAL/FAILED/NO_ACTIVE_CLIENTS
- **Per-alert format+handle** (24): 2 tests each for all 12 alert types (format returns payload, handle calls send)

### 4D. `tests/mmm-data-transfer/test_index.py` (50 tests)

Source: [mmm-data-transfer/index.py](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-data-transfer/index.py)

Groups:

- **Handler** (4): missing client_id, missing retailer_id, valid event, no files found
- **normalize_brand_name** (2): lowercase+underscores, already normalized
- **is_valid_client_file** (4): match, wrong client, non-csv, exclude pattern
- **validate_file_content** (4): valid csv, empty, header-only, binary
- **validate_sales_data** (4): valid, NaN, empty string, zero ok
- **validate_channel_data** (3): all present, all null, partial null
- **detect_duplicate_dates** (2): no dupes, with dupes
- **detect_date_gaps** (2): no gaps, with gap
- **validate_row_count** (3): no drop, yellow, red
- **find_tracer_files** (4): match, small files, non-csv, exclude pattern
- **validate_file_freshness** (2): newer, older
- **transform_data** (1): standardizes columns
- **upload_to_client_bucket** (2): correct bucket+key, metadata
- **get_vip_bucket_name** (1): auraqa1 bucket
- **Alert dispatch** (7): each send_*_alert invokes slack lambda
- **write_transfer_log / audit** (2): DynamoDB + S3
- **process_file** (1): happy path
- **handler e2e** (2): full flow, exception -> 500

---

## Step 5: Run locally

```bash
cd lambda/data-ingestion-pipeline
pip install -r requirements-dev.txt
pip install -r mmm-data-transfer/requirements.txt
pip install -r mmm-get-client/requirements.txt
pip install -r mmm-data-ingestion-slack/requirements.txt
pytest tests/ -v
```

CI auto-runs this on PR via existing [pr-status-checks-lambda.yaml](MikGorilla.AI/.github/workflows/pr-status-checks-lambda.yaml) -- no changes needed.