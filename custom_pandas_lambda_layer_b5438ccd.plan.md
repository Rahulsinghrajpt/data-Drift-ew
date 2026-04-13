---
name: Custom Pandas Lambda Layer
overview: Build a custom slim pandas+numpy Lambda layer (~60-80 MB) to replace the 258 MB AWSSDKPandas layer, allowing the data-transfer function to keep both Datadog layers and stay well under the 262 MB limit.
todos:
  - id: layer-dir
    content: Create `lambda/data-ingestion-pipeline/layers/pandas-numpy-python312/` with `requirements.txt` (pandas + numpy) and `build-layer.sh` Docker build script
    status: pending
  - id: layer-workflow
    content: Create `.github/workflows/deploy-pandas-layer.yaml` workflow_dispatch workflow to build and publish the layer to AWS
    status: pending
  - id: update-serverless
    content: Replace AWSSDKPandas layer ARN with custom layer ARN in `serverless.yml` data-transfer function (lines 75-78)
    status: pending
  - id: empty-requirements
    content: Empty `mmm-data-transfer/requirements.txt` with a comment explaining deps come from runtime + layer
    status: pending
isProject: false
---

# Custom Pandas Lambda Layer for data-transfer

## Problem

The `data-transfer` function currently uses three layers totaling ~298 MB, exceeding Lambda's 262 MB limit:

- AWSSDKPandas-Python312:1 = ~258 MB (includes pyarrow, awswrangler, full AWS SDK -- mostly unused)
- Datadog-Extension:65 = ~25 MB
- Datadog-Python312:1 = ~15 MB

The function only needs **pandas** and **numpy** from that 258 MB layer.

## Solution

Create a custom slim layer containing only `pandas` + `numpy` (~60-80 MB), bringing the total to ~100-120 MB -- well within the 262 MB limit with room to grow.

## Size Budget After Fix


| Component                 | Size             |
| ------------------------- | ---------------- |
| Custom pandas+numpy layer | ~60-80 MB        |
| Datadog-Extension:65      | ~25 MB           |
| Datadog-Python312:1       | ~15 MB           |
| Function code             | ~0.5 MB          |
| **Total**                 | **~100-120 MB**  |
| **Headroom**              | **~140 MB free** |


## Files to Create/Modify

### 1. New directory: `lambda/data-ingestion-pipeline/layers/pandas-numpy-python312/`

The layer build files live inside the data-ingestion-pipeline folder, co-located with the functions that use it. Structure:

```
lambda/data-ingestion-pipeline/
  layers/
    pandas-numpy-python312/
      requirements.txt        # pandas + numpy pins
      build-layer.sh          # Docker-based build script
  mmm-data-transfer/
  mmm-get-client/
  mmm-stale-data-check/
  mmm-data-ingestion-slack/
  serverless.yml
```

`**requirements.txt**` -- pin only what the code uses:

```
pandas>=2.1,<3
numpy>=1.26,<2
```

`**build-layer.sh**` -- builds the layer zip using a Docker container matching the Lambda Python 3.12 runtime, installs into the `python/` prefix (required by Lambda layer conventions), and strips unnecessary files (tests, docs, pyc) to minimize size.

### 2. New workflow: `.github/workflows/deploy-pandas-layer.yaml`

A manually-triggered (`workflow_dispatch`) GitHub Actions workflow that:

- Runs on `ubuntu-latest`
- Uses Docker with `public.ecr.aws/lambda/python:3.12` to `pip install` into `python/` directory
- Strips tests/docs/pyc to shrink the zip
- Publishes the layer via `aws lambda publish-layer-version` to `eu-west-1`
- Outputs the new layer ARN + version

This only needs to run when upgrading pandas/numpy versions -- not on every deploy.

### 3. Modify: `[serverless.yml](MikGorilla.AI/lambda/data-ingestion-pipeline/serverless.yml)` (lines 75-78)

Replace the AWSSDKPandas layer ARN with the custom layer ARN on the `data-transfer` function:

```yaml
# Before
layers:
  - arn:aws:lambda:eu-west-1:464622532012:layer:Datadog-Extension:65
  - arn:aws:lambda:eu-west-1:464622532012:layer:Datadog-Python312:1
  - arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python312:1

# After
layers:
  - arn:aws:lambda:eu-west-1:464622532012:layer:Datadog-Extension:65
  - arn:aws:lambda:eu-west-1:464622532012:layer:Datadog-Python312:1
  - arn:aws:lambda:eu-west-1:${aws:accountId}:layer:pandas-numpy-python312:<version>
```

### 4. Modify: `[mmm-data-transfer/requirements.txt](MikGorilla.AI/lambda/data-ingestion-pipeline/mmm-data-transfer/requirements.txt)`

Empty this file (add a comment explaining why). All three current dependencies (`boto3`, `botocore`, `python-dateutil`) are provided by the Lambda runtime:

```
# All dependencies provided by Lambda runtime + layers:
#   boto3, botocore  -> Lambda Python 3.12 runtime
#   pandas, numpy    -> pandas-numpy-python312 custom layer
#   python-dateutil  -> transitive dep of boto3 and pandas
```

## Why This Works

The data-transfer function code only imports:

- `pandas` (index.py line 60, dynamic_drift_profile.py line 34)
- `numpy` (dynamic_drift_profile.py line 41)
- `boto3` / `botocore` (index.py lines 42, 51 -- provided by Lambda runtime)
- stdlib modules (`json`, `os`, `io`, `re`, `math`, `time`, `uuid`, `datetime`, `typing`, `sys`, `warnings`)

It does **not** use `pyarrow`, `awswrangler`, or anything else from the AWSSDKPandas layer.

## Deployment Order

1. Run the layer build workflow first to publish the custom layer and get the ARN
2. Update `serverless.yml` with the new layer ARN
3. Empty `mmm-data-transfer/requirements.txt`
4. Deploy the stack via normal CI/CD

