# RCA: data-transfer Lambda Deployment Failure — Layer Size Exceeded

**Date:** 2026-04-12
**Service:** `sls-data-ingestion-pipeline`
**Function:** `data-transfer` (`sls-data-ingestion-pipeline-mmm-data-transfer-{stage}`)
**Environment:** QA (affects all stages)
**Severity:** P1 — Deployment blocked

---

## 1. Incident Summary

Deployment of the `sls-data-ingestion-pipeline` stack to QA failed with:

```
Resource handler returned message: "Function code combined with layers exceeds the
maximum allowed size of 262144000 bytes. The actual size is 266363718 bytes."
```

The `data-transfer` Lambda function's unzipped deployment package (code + layers) exceeded
AWS Lambda's **250 MiB (262,144,000 bytes)** hard limit by **~4.2 MB**.

---

## 2. Root Cause Analysis

### 2.1 Three layers stacked on one function

The `data-transfer` function in `serverless.yml` had three Lambda layers attached:

| Layer | ARN | Approx. Unzipped Size |
|-------|-----|-----------------------|
| Datadog Extension | `464622532012:layer:Datadog-Extension:65` | ~25 MB |
| Datadog Python 3.12 Tracer | `464622532012:layer:Datadog-Python312:1` | ~15 MB |
| AWS SDK for pandas (Python 3.12) | `336392948345:layer:AWSSDKPandas-Python312:1` | ~225-260 MB |
| **Total layers** | | **~265-300 MB** |

The `AWSSDKPandas-Python312:1` layer is extremely large because it bundles pandas, numpy,
pyarrow, the full AWS SDK (boto3 + botocore), and transitive dependencies. Adding the two
Datadog layers on top pushed the total well past the 250 MiB limit — even before counting
the function code itself.

### 2.2 CI/CD pipeline installs redundant pip dependencies into the package

The GitHub Actions workflow (`deploy-sls-lambda.yaml`, lines 86-89) runs:

```bash
pip install -r "${subdir}/requirements.txt" -t "${subdir}" --quiet
```

For `mmm-data-transfer/`, this reads `requirements.txt`:

```
boto3>=1.26.0,<2.0.0
botocore>=1.29.0,<2.0.0
python-dateutil>=2.8.2
```

This installs **boto3 (~1.5 MB)**, **botocore (~80-90 MB)**, and **python-dateutil (~300 KB)**
directly into the `mmm-data-transfer/` directory. The Serverless Framework packaging pattern:

```yaml
package:
  patterns:
    - '!**'
    - mmm-data-transfer/**
```

...bundles **everything** under `mmm-data-transfer/`, including the pip-installed packages.

**These dependencies are triply redundant:**
1. `boto3` and `botocore` are pre-installed in the Lambda Python 3.12 runtime
2. `boto3` and `botocore` are also included in the `AWSSDKPandas` layer
3. `python-dateutil` is a transitive dependency of both boto3 and pandas (already present)

This redundant installation adds **~80-90 MB** to the function code that serves no purpose.

### 2.3 Size breakdown (estimated)

| Component | Size |
|-----------|------|
| AWSSDKPandas-Python312:1 layer | ~258 MB |
| Datadog-Extension:65 layer | ~25 MB |
| Datadog-Python312:1 layer | ~15 MB |
| pip-installed botocore (in function code) | ~80 MB |
| pip-installed boto3 (in function code) | ~1.5 MB |
| Python source code (index.py + utils/) | ~0.5 MB |
| **Total** | **~380 MB** |
| **AWS Lambda limit** | **262 MB** |

### 2.4 Contributing factors

1. **No `.gitignore` or package exclusion** for pip-installed vendor packages inside the
   function directory. The CI pipeline installs packages into the source tree and Serverless
   Framework packages them without filtering.

2. **The `AWSSDKPandas` managed layer is disproportionately large** for this use case.
   The function only uses `pandas` and `boto3` — it does not use `pyarrow` or `awswrangler`,
   which account for a significant portion of the layer's size.

3. **Datadog layers were added without size impact assessment.** The two Datadog layers
   (~40 MB combined) were added alongside the already near-limit AWSSDKPandas layer.

4. **The existing `pandas-layer:3`** (used by the `get-client` function from account
   `931493483974`) was built for an older Python runtime and is incompatible with
   Python 3.12, preventing a simple layer swap.

---

## 3. Impact

- **Deployment of the entire `sls-data-ingestion-pipeline` CloudFormation stack is blocked.**
  All four functions in the stack (`data-ingestion-slack`, `data-transfer`, `get-client`,
  `stale-data-check`) cannot be updated because CloudFormation deploys the stack atomically.
- **No data ingestion pipeline changes can be deployed** to any environment.
- **Existing deployed functions continue to run** (the failure is only on deployment, not runtime).

---

## 4. Immediate Fix (Unblock Deployment)

### Step 1: Remove redundant pip dependencies from requirements.txt

The `mmm-data-transfer/requirements.txt` should not list packages that are already in
the Lambda runtime. Change to:

```txt
# boto3, botocore, and python-dateutil are provided by the Lambda runtime.
# Do not list them here — pip install -t bundles them into the deployment zip,
# causing the package to exceed the 250 MiB Lambda layer+code limit.
```

Or if a requirements.txt must exist (for local development), add a separate
`requirements-deploy.txt` with only non-runtime dependencies and update the CI workflow
to use it.

### Step 2: Remove Datadog layers from data-transfer

Remove the two Datadog layers from the `data-transfer` function since they cannot coexist
with the large AWSSDKPandas layer:

```yaml
layers:
  - arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python312:1
```

The `stale-data-check` function retains its Datadog layers (no AWSSDKPandas layer).

---

## 5. Long-Term Remediation

| Action | Owner | Priority | Description |
|--------|-------|----------|-------------|
| Build custom slim pandas layer for Python 3.12 | Platform | High | Create a layer with only `pandas` + `numpy` (~60-80 MB). This frees ~180 MB for Datadog layers and future growth. |
| Update CI/CD to skip runtime-provided packages | Platform | High | Modify `deploy-sls-lambda.yaml` to exclude `boto3`, `botocore`, and other Lambda-runtime packages from `pip install -t`. |
| Add package size gate to CI | Platform | Medium | Add a CI step that checks the unzipped package size before deployment and fails early with a clear message. |
| Evaluate Datadog Forwarder for data-transfer | Observability | Medium | Use log-based Datadog Forwarder (subscribes to CloudWatch Logs) instead of layers for functions that cannot accommodate layer size. |
| Audit all Lambda layer usage | Platform | Low | Review all functions for layer compatibility, redundancy, and size impact. Document per-function layer budgets. |

---

## 6. Functions Affected in This Stack

| Function | Runtime | Layers | Estimated Total Size | Status |
|----------|---------|--------|----------------------|--------|
| `data-ingestion-slack` | python3.13 | None | ~0.1 MB | OK |
| `data-transfer` | python3.12 | AWSSDKPandas + Datadog x2 | **~380 MB** | **BLOCKED** |
| `get-client` | python3.12 | pandas-layer:3 | ~65 MB | OK |
| `stale-data-check` | python3.12 | Datadog x2 | ~41 MB | OK |

---

## 7. Timeline

| Time | Event |
|------|-------|
| 2026-04-12 | Deployment to QA triggered via CI/CD |
| 2026-04-12 | CloudFormation reports `DataDashtransferLambdaFunction` size exceeded 262,144,000 bytes |
| 2026-04-12 | Stack rollback — no functions updated |
| 2026-04-12 | RCA initiated |

---

## 8. Lessons Learned

1. **Lambda layers share the 250 MiB limit with function code.** Layer selection must
   account for the combined unzipped size of all layers plus the function code.

2. **`pip install -t` in CI creates hidden bloat.** Installing runtime-provided packages
   (boto3, botocore) into the function directory silently adds ~80-90 MB that serves
   no purpose in Lambda.

3. **Managed AWS layers are convenient but opaque in size.** The `AWSSDKPandas` layer
   includes far more than pandas — it bundles the entire AWS SDK, pyarrow, and other
   libraries. A custom slim layer should be used when only a subset is needed.

4. **Layer additions need size impact review.** Adding Datadog layers (~40 MB) to a
   function already near the limit should have been caught by a pre-deployment size check.
