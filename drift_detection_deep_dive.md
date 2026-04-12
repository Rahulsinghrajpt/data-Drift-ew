# Drift Detection: Spend Regime Shift (ME-5401) and KPI Behavior Break (ME-5402)

This document covers both drift-detection features in the data ingestion pipeline:
how they are configured, how each algorithm works step-by-step, what triggers
alerts, what side-effects they produce, and where every piece of code lives.

---

## Table of Contents

1. [Shared Infrastructure](#1-shared-infrastructure)
   - 1.1 [Drift Profile (`profile.json`)](#11-drift-profile-profilejson)
   - 1.2 [Profile Loading and Caching](#12-profile-loading-and-caching)
   - 1.3 [Threshold Defaults](#13-threshold-defaults)
   - 1.4 [Pipeline Integration Point](#14-pipeline-integration-point)
   - 1.5 [DynamoDB Persistence (`update_drift_metrics`)](#15-dynamodb-persistence-update_drift_metrics)
2. [ME-5401: Spend Regime Shift](#2-me-5401-spend-regime-shift)
   - 2.1 [Purpose](#21-purpose)
   - 2.2 [Detection Algorithm](#22-detection-algorithm)
   - 2.3 [Severity Classification](#23-severity-classification)
   - 2.4 [Return Payload](#24-return-payload)
   - 2.5 [Action Chain](#25-action-chain)
   - 2.6 [Slack Alert Format](#26-slack-alert-format)
3. [ME-5402: KPI Behavior Break](#3-me-5402-kpi-behavior-break)
   - 3.1 [Purpose](#31-purpose)
   - 3.2 [Detection Algorithm](#32-detection-algorithm)
   - 3.3 [Residual Prediction Model](#33-residual-prediction-model)
   - 3.4 [Opposite Direction Logic](#34-opposite-direction-logic)
   - 3.5 [Event Transition Suppression](#35-event-transition-suppression)
   - 3.6 [Severity Classification](#36-severity-classification)
   - 3.7 [Return Payload](#37-return-payload)
   - 3.8 [Action Chain](#38-action-chain)
   - 3.9 [Slack Alert Format](#39-slack-alert-format)
4. [Configuration Reference](#4-configuration-reference)
5. [End-to-End Data Flow Diagram](#5-end-to-end-data-flow-diagram)

---

## 1. Shared Infrastructure

### 1.1 Drift Profile (`profile.json`)

Both detectors require a **drift profile** JSON file shipped alongside the Lambda
deployment package at `lambdas/mmm_dev_data_transfer/profile.json`.

The profile is produced offline (from historical training data) and contains five
top-level sections:

| Section            | Used by       | Purpose |
|--------------------|---------------|---------|
| `schema`           | Both          | Column names: `kpi_col`, `spend_cols`, `control_cols`, `event_cols` |
| `spend_stats`      | ME-5401 + 5402 | Per-channel historical statistics: `mean`, `std`, `p99`, `p995`, `active_weeks` |
| `control_stats`    | ME-5402       | Control variable stats (e.g. `GQV`, `Seasonality`): `mean`, `std` |
| `residual_profile` | ME-5402       | Regression model for KPI prediction: `feature_names`, `beta`, `resid_mean`, `resid_std` |
| `thresholds`       | Both          | Named thresholds: `Z_OUTLIER_YELLOW`, `Z_OUTLIER_RED`, `RESID_Z_YELLOW`, `RESID_Z_RED`, etc. |

Example `spend_stats` entry (one per spend channel):

```json
"Google_spend": {
  "mean": 29996.787,
  "std": 12520.477,
  "p99": 69967.634,
  "p995": 71089.539,
  "active_weeks": 157
}
```

### 1.2 Profile Loading and Caching

**File:** `lambdas/mmm_dev_data_transfer/lambda_function.py`

`load_drift_profile()` resolves the file path by checking:

1. `DRIFT_PROFILE_PATH` environment variable (if set)
2. `profile.json` in same directory as the Lambda
3. Parent and grandparent directories

The profile is loaded **once** per Lambda cold start and cached in
`_DRIFT_PROFILE_CACHE`. Subsequent invocations within the same container reuse
the cached result. If the file is missing or JSON parsing fails, the cache is
set to `None` and both detectors are silently skipped for that run.

### 1.3 Threshold Defaults

When a threshold is missing from `profile.json`, hard-coded defaults apply:

| Constant | Default | Used by |
|----------|---------|---------|
| `SPEND_REGIME_Z_YELLOW_DEFAULT` | 3.0 | ME-5401 |
| `SPEND_REGIME_Z_RED_DEFAULT` | 4.0 | ME-5401 |
| `KPI_BEHAVIOR_RESID_Z_YELLOW_DEFAULT` | 3.0 | ME-5402 |
| `KPI_BEHAVIOR_RESID_Z_RED_DEFAULT` | 4.0 | ME-5402 |
| `KPI_BEHAVIOR_OPPOSITE_MIN_RESID_Z_DEFAULT` | 1.5 | ME-5402 |
| `KPI_BEHAVIOR_OPPOSITE_MIN_SPEND_DELTA_PCT_DEFAULT` | 0.15 | ME-5402 |
| `KPI_BEHAVIOR_EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z_DEFAULT` | 1.0 | ME-5402 |

### 1.4 Pipeline Integration Point

Both detectors run inside the per-file processing loop, **after** the retailer
CSV has been uploaded to the VIP bucket (so detection never blocks data delivery):

```
process_file (per retailer CSV)
  -> upload to S3
  -> df = pd.read_csv(retailer_data)
  -> ME-5401: detect_spend_regime_shift(df, drift_profile)
       -> if detected: handle_spend_regime_shift_actions(...)
  -> ME-5402: detect_kpi_behavior_break(df, drift_profile)
       -> if detected: handle_kpi_behavior_break_actions(...)
```

Each detector is wrapped in its own `try/except`; a failure in one does not
affect the other or the main upload path.

### 1.5 DynamoDB Persistence (`update_drift_metrics`)

**File:** `src/utils/pipeline_info_helper.py`

When either detector fires, `handle_*_actions` calls
`PipelineInfoHelper.update_drift_metrics(...)`, which:

1. Reads the current pipeline-info record for `client_id / brand_name / retailer_id`.
2. Copies the existing `drift_metric_current` into `drift_metric_previous`.
3. Writes the new `drift_metric_current` (the `max |z|` or `|residual z|` value).
4. Sets `retraining_required = True`.

This DynamoDB record can then be read by the training pipeline to decide whether
a model retrain is needed.

---

## 2. ME-5401: Spend Regime Shift

### 2.1 Purpose

Detect when a media spend channel's **latest weekly value** is statistically
abnormal compared to historical baselines. This catches scenarios like a
sudden 5x budget increase, a channel being turned off, or data entry errors.

### 2.2 Detection Algorithm

**Function:** `detect_spend_regime_shift(retailer_df, profile)`

Step-by-step:

1. **Extract inputs:**
   - `spend_stats` from profile (per-channel `mean`, `std`, `p99`, `p995`)
   - `thresholds.Z_OUTLIER_YELLOW` and `thresholds.Z_OUTLIER_RED`
   - `latest_row = retailer_df.iloc[-1]` (last row of the DataFrame)

2. **For each channel** in `spend_stats`:
   - Skip if the channel column does not exist in `retailer_df`.
   - Read `current_value` from `latest_row[channel]`.
   - Skip if `mean`, `std`, or `current_value` is missing or `std <= 0`.
   - Compute: **z_score = (current_value - mean) / std**
   - Check **absolute z-score** against thresholds.
   - Check **percentile breach**: `current_value > p99` and `current_value > p995`.

3. **Trigger logic** (per channel):
   - **YELLOW** if `|z| >= Z_OUTLIER_YELLOW` **OR** `value > p99`.
   - **RED** if `|z| >= Z_OUTLIER_RED` **OR** `value > p995`.

4. **Overall detection:**
   - `detected = True` if any channel triggered at YELLOW or above.
   - `drift_metric_current = max(|z|)` across all evaluated channels.

### 2.3 Severity Classification

| Condition | Severity |
|-----------|----------|
| No anomalies | `NONE` |
| At least one YELLOW, no RED | `YELLOW` |
| At least one RED | `RED` |

### 2.4 Return Payload

```python
{
    'detected': bool,
    'severity': 'NONE' | 'YELLOW' | 'RED',
    'anomalies': [
        {
            'channel': str,          # e.g. 'Google_spend'
            'value': float,          # current week value
            'mean': float,           # historical mean
            'std': float,            # historical std
            'z_score': float,        # signed z-score
            'p99': float,
            'p995': float,
            'threshold_breaches': {
                'z_yellow': bool,
                'z_red': bool,
                'p99': bool,
                'p995': bool
            },
            'severity': 'YELLOW' | 'RED'
        }
    ],
    'drift_metric_current': float,   # max |z| across channels
    'evaluated_channels': int        # how many channels had valid stats
}
```

### 2.5 Action Chain

**Function:** `handle_spend_regime_shift_actions(...)`

When `detected == True`:

1. **Slack alert** via `send_spend_regime_shift_alert(...)`:
   - Builds a payload with `alert_type: 'SPEND_REGIME_SHIFT'`.
   - Invokes the Slack Lambda (`mmm_{env}_data_ingestion_slack`) asynchronously
     (`InvocationType='Event'`).
2. **DynamoDB update** via `PipelineInfoHelper.update_drift_metrics(...)`:
   - `drift_metric_current = max |z|`
   - `retraining_required = True`

### 2.6 Slack Alert Format

The Slack Lambda (`data_ingestion_slack/lambda_function.py`) handles
`alert_type == 'SPEND_REGIME_SHIFT'` by calling `format_spend_regime_shift_alert`,
which produces a message with:

- Red or yellow sidebar (based on severity)
- Header: "SPEND REGIME SHIFT DETECTED"
- `@here` mention with retraining note
- Client / brand / retailer / environment / file / severity / max |z|
- Per-channel anomaly lines: `channel`, `value`, `z_score`, `p99`

---

## 3. ME-5402: KPI Behavior Break

### 3.1 Purpose

Detect when the KPI (typically Sales) **diverges from what the model predicts**
given the current media spend levels. Unlike ME-5401 (which checks raw spend
magnitudes), ME-5402 uses a **residual model** to catch situations where spend
looks normal but sales behave unexpectedly, or where spend and sales move in
**opposite directions**.

### 3.2 Detection Algorithm

**Function:** `detect_kpi_behavior_break(retailer_df, profile)`

**Preconditions:**
- Requires at least 2 rows in `retailer_df` (latest + previous).
- Requires a `kpi_col` from `profile.schema` (falls back to `'Sales'`).

Step-by-step:

1. **Extract the last two rows:**
   - `latest_row = retailer_df.iloc[-1]`
   - `previous_row = retailer_df.iloc[-2]`
   - `latest_kpi`, `previous_kpi` from `kpi_col`

2. **Predict expected log-KPI** using the residual model:
   - `pred_log_kpi = _predict_log_kpi_from_profile(latest_row, profile)`
   - If prediction unavailable, falls back to `log1p(previous_kpi)`.

3. **Compute residual z-score:**
   - `actual_log_kpi = log1p(latest_kpi)`
   - `residual = actual_log_kpi - pred_log_kpi`
   - `resid_z = (residual - resid_mean) / resid_std`

4. **Compute spend and KPI deltas:**
   - Sum all spend columns for latest and previous rows.
   - `spend_delta = latest_total_spend - previous_total_spend`
   - `kpi_delta = latest_kpi - previous_kpi`

5. **Evaluate triggers:**
   - **Residual trigger:** `|resid_z| >= RESID_Z_YELLOW`
   - **Opposite direction:** see 3.4 below
   - **Final:** `detected = residual_trigger OR opposite_direction`

### 3.3 Residual Prediction Model

**Function:** `_predict_log_kpi_from_profile(latest_row, profile)`

The profile contains a linear model with named features and coefficients (`beta`).
Supported feature types:

| Feature format | Transformation |
|----------------|----------------|
| `intercept` | 1.0 (constant term) |
| `log1p(X)` | `math.log1p(latest_row[X])` |
| `z(X)` | `(latest_row[X] - control_stats[X].mean) / control_stats[X].std` |
| `<column_name>` | Raw value from `latest_row` (e.g. event flags like `Christmas`) |

Prediction: `sum(feature[i] * beta[i])` for all features.

Example features from `profile.json`:
- `log1p(Google_spend)`, `log1p(Meta_spend)`, ..., `log1p(Target_spend)`
- `z(GQV)`, `z(Seasonality)`
- `BackToSchool`, `Christmas`, `CyberMonday`, `Easter`, `NewYear`, `SuperBowl`,
  `Thanksgiving`, `Valentine`
- `intercept`

### 3.4 Opposite Direction Logic

"KPI went up while spend went down (or vice versa)" is suspicious. The check:

```
raw_opposite = (|spend_delta| > epsilon) AND (|kpi_delta| > epsilon)
               AND (spend_delta * kpi_delta < 0)
```

However, to avoid false positives from noise, **three** additional guards must pass:

1. `|resid_z| >= OPPOSITE_MIN_RESID_Z` (default 1.5) -- the residual must be
   at least somewhat unusual.
2. `spend_delta_pct >= OPPOSITE_MIN_SPEND_DELTA_PCT` (default 0.15) -- the spend
   change must be at least 15% of the historical mean total spend.
3. **Event transition suppression** is not active (see 3.5).

Only when all guards pass is `opposite_direction = True`.

### 3.5 Event Transition Suppression

**Function:** `_is_event_transition_ended(previous_row, latest_row, event_cols)`

When a calendar event (e.g. Christmas, SuperBowl) ends -- the event flag goes
from 1 to 0 between previous and latest rows -- a KPI drop is **expected** because
event-driven lift disappears. To prevent false alarms:

- If any event flag transitioned from active (>= 0.5) to inactive (< 0.5),
  `event_transition_ended = True`.
- If `event_transition_ended` AND `|resid_z| <= EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z`
  (default 1.0), then `opposite_direction` is forced to `False`.

This means a mild KPI-vs-spend divergence right after an event ends is **not**
flagged.

### 3.6 Severity Classification

| Condition | Severity |
|-----------|----------|
| Not detected | `NONE` |
| Detected, `|resid_z| < RESID_Z_RED` | `YELLOW` |
| Detected, `|resid_z| >= RESID_Z_RED` | `RED` |

### 3.7 Return Payload

```python
{
    'detected': bool,
    'severity': 'NONE' | 'YELLOW' | 'RED',
    'anomalies': [
        {
            'kpi_column': str,
            'latest_kpi': float,
            'previous_kpi': float,
            'kpi_delta': float,
            'latest_total_spend': float,
            'previous_total_spend': float,
            'spend_delta': float,
            'spend_delta_pct': float,
            'residual': float,
            'residual_z': float,
            'threshold_breaches': {
                'residual_yellow': bool,
                'residual_red': bool,
                'raw_opposite_direction': bool,
                'opposite_direction': bool,
                'event_transition_ended': bool,
            },
            'severity': 'YELLOW' | 'RED',
        }
    ],
    'drift_metric_current': float,       # |resid_z|
    'opposite_direction': bool,
    'event_transition_ended': bool,
}
```

### 3.8 Action Chain

**Function:** `handle_kpi_behavior_break_actions(...)`

When `detected == True`:

1. **Slack alert** via `send_kpi_behavior_break_alert(...)`:
   - Payload validated by `_validate_kpi_alert_payload` (checks required fields,
     severity in `{YELLOW, RED}`, anomalies non-empty).
   - Invokes Slack Lambda asynchronously with **retry logic**: up to
     `KPI_ALERT_MAX_RETRIES` (default 3) attempts with **exponential backoff**
     (`KPI_ALERT_RETRY_BASE_SECONDS * 2^attempt`). Each invocation gets a
     `correlation_id` for tracing.
2. **DynamoDB update** via `PipelineInfoHelper.update_drift_metrics(...)`:
   - `drift_metric_current = |resid_z|`
   - `retraining_required = True`

### 3.9 Slack Alert Format

The Slack Lambda handles `alert_type == 'KPI_BEHAVIOR_BREAK'` by calling
`format_kpi_behavior_break_alert`, which produces a message with:

- Red or yellow sidebar
- Header: "KPI BEHAVIOR BREAK DETECTED"
- `@here` with ME-5402 retraining note
- Client / brand / retailer / environment / file / severity / `|residual z|`
- Per-anomaly lines: `residual_z`, `kpi_delta`, `spend_delta`, `opposite_direction`
- `correlation_id` in the footer for log tracing

---

## 4. Configuration Reference

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DRIFT_PROFILE_PATH` | `""` (auto-resolve) | Override path to `profile.json` |
| `KPI_ALERT_MAX_RETRIES` | `3` | Retry attempts for ME-5402 Slack invocation |
| `KPI_ALERT_RETRY_BASE_SECONDS` | `1.0` | Base backoff for retries (doubles each attempt) |

### `profile.json` Thresholds

| Key | Default | ME | Meaning |
|-----|---------|----|---------|
| `Z_OUTLIER_YELLOW` | 3.0 | 5401 | Spend z-score for YELLOW |
| `Z_OUTLIER_RED` | 4.0 | 5401 | Spend z-score for RED |
| `RESID_Z_YELLOW` | 3.0 | 5402 | Residual z-score for YELLOW |
| `RESID_Z_RED` | 4.0 | 5402 | Residual z-score for RED |
| `OPPOSITE_MIN_RESID_Z` | 1.5 | 5402 | Min residual z for opposite-direction check |
| `OPPOSITE_MIN_SPEND_DELTA_PCT` | 0.15 | 5402 | Min spend change (% of mean) for opposite check |
| `EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z` | 1.0 | 5402 | Max residual z that gets suppressed after event ends |

---

## 5. End-to-End Data Flow Diagram

```
retailer CSV uploaded to VIP S3
         |
         v
   df = pd.read_csv(retailer_data)
         |
         v
   drift_profile = load_drift_profile()  [cached per cold start]
         |
    +----+----+
    |         |
    v         v
 ME-5401   ME-5402
    |         |
    v         v
 detect_     detect_
 spend_      kpi_
 regime_     behavior_
 shift()     break()
    |         |
    v         v
 detected?   detected?
    |         |
  [yes]     [yes]
    |         |
    +----+----+
         |
         v
   handle_*_actions()
         |
    +----+----+
    |         |
    v         v
 send_*_   update_drift_
 alert()   metrics()
    |         |
    v         v
 Slack     DynamoDB
 Lambda    pipeline-infos
 (async)   (retraining_required=True)
```

---

## Source File Reference

| Component | File |
|-----------|------|
| Detection + alerting functions | `lambdas/mmm_dev_data_transfer/lambda_function.py` (lines ~275-945) |
| Profile data | `lambdas/mmm_dev_data_transfer/profile.json` |
| Pipeline integration (invocation) | `lambdas/mmm_dev_data_transfer/lambda_function.py` (lines ~4111-4184) |
| Slack formatting + delivery | `lambdas/data_ingestion_slack/lambda_function.py` (lines ~1353-1589) |
| DynamoDB persistence | `src/utils/pipeline_info_helper.py` (`update_drift_metrics`, line ~1691) |

---

## 6. ME-5806 Structural Detection (JS and Activation/Deactivation)

In addition to traditional statistical checks, two other checks examine the structural integrity of the media channels:

### 6.1 ME-5806a: Channel Activation/Deactivation
This module counts the number of non-zero weeks for columns defined as spend metrics historically.
If a column reaches `0` when `active_weeks >= MIN_ACTIVE_WEEKS_FOR_STABILITY`, we trigger a `RED` slack alert.
If a column activates prematurely (`> 0` but `active_weeks < MIN_ACTIVE_WEEKS_FOR_STABILITY`), it triggers `RED`.

### 6.2 ME-5806b: Spend Mix Reallocation
Calculates the relative contribution of each channel and compares the vector against the historical baseline mapping using purely the Jensen-Shannon divergence across elements.
If JS divergence is `>= JS_YELLOW` or max absolute delta `>= MIX_SHIFT_YELLOW`, we trigger a `YELLOW` alert.
Similarly, `RED` thresholds apply for larger gaps.
