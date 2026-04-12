# ME-5806: Channel Activation/Deactivation & Spend Mix Reallocation -- Drift Detection

## Table of Contents

1. [Overview](#1-overview)
2. [Why Do We Need These Detectors?](#2-why-do-we-need-these-detectors)
3. [Architecture & File Map](#3-architecture--file-map)
4. [Constants & Thresholds](#4-constants--thresholds)
5. [How the Drift Profile is Built (Dynamic Profile)](#5-how-the-drift-profile-is-built-dynamic-profile)
6. [ME-5806a: Channel Activation/Deactivation -- Logic Deep Dive](#6-me-5806a-channel-activationdeactivation----logic-deep-dive)
7. [ME-5806b: Spend Mix Reallocation -- Logic Deep Dive](#7-me-5806b-spend-mix-reallocation----logic-deep-dive)
8. [Jensen-Shannon Divergence Helper](#8-jensen-shannon-divergence-helper)
9. [Action Pipeline: Alert + Persist + Retrain Flag](#9-action-pipeline-alert--persist--retrain-flag)
10. [Slack Lambda: Formatting & Routing](#10-slack-lambda-formatting--routing)
11. [End-to-End Flow with Example Dataset](#11-end-to-end-flow-with-example-dataset)
12. [Datadog Metrics Emitted](#12-datadog-metrics-emitted)
13. [Unit Tests](#13-unit-tests)
14. [FAQ / Troubleshooting](#14-faq--troubleshooting)

---

## 1. Overview

ME-5806 introduces **two new drift detectors** that run inside the `mmm_dev_data_transfer` Lambda every time a retailer CSV is processed. They sit alongside the existing ME-5401 (Spend Regime Shift) and ME-5402 (KPI Behavior Break) detectors.

| Detector   | Ticket   | What it catches |
|------------|----------|-----------------|
| **Channel Activation/Deactivation** | ME-5806a | A media channel suddenly starts spending when it has very little history, or stops spending entirely when it was historically active. |
| **Spend Mix Reallocation** | ME-5806b | The **proportion** of budget across channels shifts significantly, even though the total spend amount may be unchanged. |

Both detectors always fire severity **RED** (channel activation/deactivation) or **YELLOW/RED** (spend mix) and mark `retraining_required = true` in DynamoDB when triggered.

---

## 2. Why Do We Need These Detectors?

The existing ME-5401 detector catches **absolute magnitude shocks** (e.g., TV spend jumps from $10k to $100k). But it misses two important patterns:

**Pattern 1 -- New/Dropped Channels (ME-5806a)**
If a brand has never spent on TikTok and suddenly starts, or if they ran Google Search for 50+ weeks and suddenly stop, the MMM model is being asked to work with a fundamentally different media structure. The model was trained on one set of channels and is now seeing another. This requires retraining.

**Pattern 2 -- Budget Reallocation (ME-5806b)**
Suppose total spend stays at $100k, but the split changes from 50/50 (TV/Search) to 90/10. ME-5401 would not detect this because neither channel exceeds its individual z-score threshold. But the model's coefficient attribution is now being applied in a very different operating region.

---

## 3. Architecture & File Map

```
data_ingestion_pipeline/
├── lambdas/
│   ├── mmm_dev_data_transfer/
│   │   ├── lambda_function.py          # Detection logic + process_file wiring
│   │   └── dynamic_drift_profile.py    # Builds the reference profile at runtime
│   └── data_ingestion_slack/
│       └── lambda_function.py          # Slack alert formatting + routing
└── tests/layers/compute/unit/
    ├── test_me5806a_channel_activation_deactivation.py
    └── test_me5806b_spend_mix_reallocation.py
```

**Data flow:**
```
S3 CSV → data_transfer Lambda (process_file)
           │
           ├─ prepare_drift_data()     → split into historical + latest row
           ├─ infer_schema()           → identify spend/kpi/event columns
           ├─ build_reference_profile()→ compute baseline stats from history
           │
           ├─ ME-5401: detect_spend_regime_shift()
           ├─ ME-5402: detect_kpi_behavior_break()
           ├─ ME-5806a: detect_channel_activation_deactivation()  ← NEW
           └─ ME-5806b: detect_spend_mix_reallocation()           ← NEW
                  │
                  ▼  (if detected)
           handle_*_actions()
           ├── Invoke Slack Lambda (async)
           ├── Update DynamoDB (retraining_required = true)
           └── Emit Datadog metric
                  │
                  ▼
           Slack Lambda (data_ingestion_slack)
           ├── format_*_alert()  → build Slack Block Kit message
           └── POST to webhook
```

---

## 4. Constants & Thresholds

Defined in `lambda_function.py` (data transfer):

```python
# ME-5806a: Channel Activation/Deactivation thresholds
MIN_ACTIVE_WEEKS_DEFAULT = 8       # Channel must have >=8 weeks of non-zero spend to be "stable"
CONSEC_ZERO_WEEKS_RED_DEFAULT = 4  # (reserved for future multi-row deactivation logic)

# ME-5806b: Spend Mix Reallocation thresholds
MIX_SHIFT_YELLOW_DEFAULT = 0.20    # If any single channel's share changes by >=20%, severity=YELLOW
MIX_SHIFT_RED_DEFAULT = 0.30       # If any single channel's share changes by >=30%, severity=RED
JS_YELLOW_DEFAULT = 0.06           # Jensen-Shannon divergence >=0.06 → YELLOW
JS_RED_DEFAULT = 0.10              # Jensen-Shannon divergence >=0.10 → RED
```

The dynamic profile (`dynamic_drift_profile.py`) also stores these thresholds inside the profile dict under `thresholds.MIN_ACTIVE_WEEKS_FOR_STABILITY`, `thresholds.MIX_SHIFT_YELLOW`, etc. The detector reads from the profile first, falling back to the module-level defaults if not present.

---

## 5. How the Drift Profile is Built (Dynamic Profile)

Before any detector runs, `process_file` builds a **dynamic reference profile** from the retailer's own historical data. Here is the step-by-step:

### Step 1: prepare_drift_data(df)

```
Input CSV (sorted by Date):
  Date       | tv_spend | search_spend | Sales
  2025-01-06 |   5000   |    3000      |  800
  2025-01-13 |   5200   |    2900      |  810
  ...        |   ...    |    ...       |  ...
  2025-06-30 |   5100   |    3100      |  820    ← 25 rows total
  2025-07-07 |   4900   |    3200      |  830    ← LATEST ROW
```

- Sorts by Date.
- **historical_df** = rows 1-25 (everything except the last row). This is the "baseline."
- **latest_df** = row 26 (the newest week). This is what we check for drift.
- **is_eligible** = `len(historical_df) >= 20` (MIN_ROWS_FOR_DRIFT). With 25 rows, we pass.

### Step 2: infer_schema(df)

Scans column names to classify them:
- Columns ending in `_spend` → **spend_cols** (e.g., `tv_spend`, `search_spend`)
- Columns ending in `_sales` → **kpi_col** (e.g., `Sales`)
- Known names like `GQV`, `Seasonality` → **control_cols**
- Known event names (Christmas, Easter...) or binary 0/1 columns → **event_cols**

### Step 3: build_reference_profile(historical_df, schema)

Builds four sections from the historical rows only:

#### a) `spend_stats` (used by ME-5806a)

For each spend column, compute:
```python
spend_stats["tv_spend"] = {
    "mean": 5100.0,         # average historical spend
    "std": 150.0,           # standard deviation
    "p99": 5480.0,          # 99th percentile
    "p995": 5490.0,         # 99.5th percentile
    "active_weeks": 25      # how many weeks had spend > 0
}
```

The **`active_weeks`** field is the key input for ME-5806a. It tells us how established a channel is.

#### b) `mix_profile` (used by ME-5806b)

```python
mix_profile = {
    "spend_cols": ["tv_spend", "search_spend"],
    "avg_share": [0.6296, 0.3704]   # average % of total spend per channel across history
}
```

How `avg_share` is calculated:
1. For each historical row, compute the total spend across all channels.
2. Divide each channel's spend by that row's total → per-row share vector.
3. Average across all rows → `avg_share`.

Example: if TV averages 62.96% of total spend and Search averages 37.04%, `avg_share = [0.6296, 0.3704]`.

#### c) `thresholds`

```python
thresholds = {
    "MIN_ACTIVE_WEEKS_FOR_STABILITY": 8,
    "MIX_SHIFT_YELLOW": 0.20,
    "MIX_SHIFT_RED": 0.30,
    "JS_YELLOW": 0.06,
    "JS_RED": 0.10,
    ...
}
```

#### d) `residual_profile` (used by ME-5402, not ME-5806)

Ridge regression coefficients for KPI prediction. Not relevant to the new detectors.

---

## 6. ME-5806a: Channel Activation/Deactivation -- Logic Deep Dive

**File:** `data_ingestion_pipeline/lambdas/mmm_dev_data_transfer/lambda_function.py`, function `detect_channel_activation_deactivation`

### What it does in plain English

For every media channel in the profile, look at the **latest row** and ask:
- Is the channel spending money **this week** but historically has **very little track record**? → **ACTIVATION** (new channel turned on)
- Is the channel spending **nothing this week** but historically has been **consistently active**? → **DEACTIVATION** (established channel turned off)

### Line-by-line walkthrough

```python
def detect_channel_activation_deactivation(
    retailer_df: Any,       # Full retailer DataFrame (all rows including latest)
    profile: Dict[str, Any] # The dynamic drift profile built from historical data
) -> Dict[str, Any]:
```

**Guard clause -- empty data:**
```python
    if retailer_df is None or len(retailer_df) == 0:
        return {'detected': False, 'severity': 'NONE', 'anomalies': [], 'drift_metric_current': 0.0}
```
If there's no data at all, return "no drift found."

**Extract profile sections:**
```python
    spend_stats = profile.get('spend_stats', {}) if profile else {}
    thresholds = profile.get('thresholds', {}) if profile else {}
```
Pull the per-channel spend statistics and the threshold configuration from the profile.

**Read the stability threshold:**
```python
    min_active_weeks = int(_to_float(thresholds.get('MIN_ACTIVE_WEEKS_FOR_STABILITY')) or MIN_ACTIVE_WEEKS_DEFAULT)
```
This is the minimum number of weeks a channel must have been active (spend > 0) to be considered "established." Default is **8 weeks**.

**Get the latest row (the newest week of data):**
```python
    latest_row = retailer_df.iloc[-1]
    anomalies = []
```

**Loop through every channel in the profile:**
```python
    for channel_name, stats in spend_stats.items():
        if channel_name not in retailer_df.columns:
            continue
```
Skip channels that exist in the profile but not in the DataFrame (shouldn't happen normally).

**Read this week's spend and the channel's history:**
```python
        current_value = _to_float(latest_row.get(channel_name))
        active_weeks = int(_to_float(stats.get('active_weeks', stats.get('count', 0))))
```
- `current_value`: how much this channel spent in the latest row.
- `active_weeks`: how many weeks (historically) this channel had non-zero spend.

**ACTIVATION check:**
```python
        if current_value is not None and current_value > 1e-9:   # channel IS spending this week
            if active_weeks < min_active_weeks:                   # but has < 8 weeks of history
                anomalies.append({
                    'channel': channel_name,
                    'type': 'ACTIVATION',
                    'current_value': round(current_value, 4),
                    'active_weeks': active_weeks,
                    'threshold': min_active_weeks,
                    'severity': 'RED'
                })
```
Logic: "This channel is active NOW, but it has very little historical track record. The model doesn't know how to attribute credit to this channel → retrain."

**DEACTIVATION check:**
```python
        elif current_value is not None and current_value <= 1e-9: # channel is NOT spending this week
            if active_weeks >= min_active_weeks:                   # but WAS established (>=8 weeks)
                anomalies.append({
                    'channel': channel_name,
                    'type': 'DEACTIVATION',
                    'current_value': 0.0,
                    'active_weeks': active_weeks,
                    'threshold': min_active_weeks,
                    'severity': 'RED'
                })
```
Logic: "This channel was a reliable part of the media mix and suddenly stopped. The model still expects it → retrain."

**Return result:**
```python
    detected = len(anomalies) > 0
    return {
        'detected': detected,
        'severity': 'RED' if detected else 'NONE',
        'anomalies': anomalies,
        'drift_metric_current': float(len(anomalies))  # number of affected channels
    }
```
Severity is always RED when triggered (channel structure changes are serious). The `drift_metric_current` is simply the **count of affected channels**.

### Example

```
Profile spend_stats:
  tv_spend:      active_weeks = 50   (well established)
  search_spend:  active_weeks = 50   (well established)
  tiktok_spend:  active_weeks = 2    (brand new)

Latest row:
  tv_spend = 0           ← was active 50 weeks, now zero  → DEACTIVATION
  search_spend = 3000    ← active 50 weeks, still active   → OK (no anomaly)
  tiktok_spend = 500     ← only 2 weeks history, spending  → ACTIVATION

Result:
  detected = True
  severity = "RED"
  anomalies = [
      {channel: "tv_spend", type: "DEACTIVATION", active_weeks: 50, threshold: 8},
      {channel: "tiktok_spend", type: "ACTIVATION", active_weeks: 2, threshold: 8},
  ]
  drift_metric_current = 2.0  (two channels affected)
```

---

## 7. ME-5806b: Spend Mix Reallocation -- Logic Deep Dive

**File:** `data_ingestion_pipeline/lambdas/mmm_dev_data_transfer/lambda_function.py`, function `detect_spend_mix_reallocation`

### What it does in plain English

Compare the **percentage distribution** of spend across channels in the latest week against the historical average distribution. Use two measures:
1. **Max absolute share delta** -- the single biggest channel-level percentage shift.
2. **Jensen-Shannon divergence** -- a statistical measure of how different the two distributions are overall.

If either exceeds the YELLOW or RED threshold, fire an alert.

### Line-by-line walkthrough

**Guard clause:**
```python
    if retailer_df is None or len(retailer_df) == 0:
        return {'detected': False, ...}
```

**Extract the baseline mix from the profile:**
```python
    mix_profile = profile.get('mix_profile', {}) if profile else {}
    spend_cols_mix = mix_profile.get('spend_cols', [])
    avg_share_raw = mix_profile.get('avg_share', [])
```
The profile's `mix_profile` contains:
- `spend_cols`: ordered list of channel names (e.g., `["tv_spend", "search_spend"]`)
- `avg_share`: historical average share per channel (e.g., `[0.63, 0.37]` -- a list from the dynamic profile builder)

**Normalize avg_share into a dict:**
```python
    avg_share_map: Dict[str, float] = {}
    if isinstance(avg_share_raw, dict):
        # Test data passes a dict like {"tv": 0.5, "search": 0.5}
        for k, v in avg_share_raw.items():
            avg_share_map[str(k)] = _to_float(v) or 0.0
        spend_cols = list(spend_cols_mix) if spend_cols_mix else sorted(avg_share_map.keys())
    elif isinstance(avg_share_raw, list) and spend_cols_mix:
        # Production profile passes a list [0.63, 0.37] aligned with spend_cols
        for i, ch in enumerate(spend_cols_mix):
            val = _to_float(avg_share_raw[i]) if i < len(avg_share_raw) else 0.0
            avg_share_map[ch] = val or 0.0
        spend_cols = list(spend_cols_mix)
```
This handles both the **list format** (from `build_reference_profile`) and the **dict format** (from test fixtures).

**Read thresholds:**
```python
    mix_shift_yellow = _to_float(thresholds.get('MIX_SHIFT_YELLOW')) or MIX_SHIFT_YELLOW_DEFAULT  # 0.20
    mix_shift_red = _to_float(thresholds.get('MIX_SHIFT_RED')) or MIX_SHIFT_RED_DEFAULT            # 0.30
    js_yellow = _to_float(thresholds.get('JS_YELLOW')) or JS_YELLOW_DEFAULT                        # 0.06
    js_red = _to_float(thresholds.get('JS_RED')) or JS_RED_DEFAULT                                  # 0.10
```

**Build the current spend vector from the latest row:**
```python
    latest_row = retailer_df.iloc[-1]
    current_spend_vec = []
    baseline_share_vec = []
    total_spend = 0.0

    for channel in spend_cols:
        spend_val = _to_float(latest_row.get(channel)) or 0.0
        current_spend_vec.append(spend_val)
        total_spend += spend_val
        baseline_share_vec.append(avg_share_map.get(channel, 0.0))
```

**Skip if total spend is zero:**
```python
    if total_spend <= 1e-9:
        return {'detected': False, ...}
```
If nobody is spending anything, we can't compute shares.

**Compute current shares and JS divergence:**
```python
    current_share_vec = [v / total_spend for v in current_spend_vec]
    js_div = _jensen_shannon_divergence(current_share_vec, baseline_share_vec)
```
Convert raw spend into percentages, then compare distributions using JS divergence.

**Compute per-channel delta:**
```python
    max_delta = 0.0
    channel_deltas = []
    for idx, channel in enumerate(spend_cols):
        c_share = current_share_vec[idx]
        b_share = baseline_share_vec[idx]
        delta = abs(c_share - b_share)
        max_delta = max(max_delta, delta)
        channel_deltas.append({
            'channel': channel,
            'current_share': round(c_share, 4),
            'baseline_share': round(b_share, 4),
            'delta': round(delta, 4)
        })
```

**Threshold check:**
```python
    yellow_trigger = max_delta >= mix_shift_yellow or js_div >= js_yellow
    red_trigger = max_delta >= mix_shift_red or js_div >= js_red
```
EITHER measure exceeding EITHER threshold triggers the alert. This is an **OR** logic:
- `max_delta >= 0.20` OR `js_div >= 0.06` → YELLOW
- `max_delta >= 0.30` OR `js_div >= 0.10` → RED

**Compute normalized drift metric:**
```python
    normalized_js = js_div / js_yellow if js_yellow > 0 else 0
    normalized_delta = max_delta / mix_shift_yellow if mix_shift_yellow > 0 else 0
    drift_metric_current = float(max(normalized_js, normalized_delta))
```
This gives a single number representing "how far past the threshold are we?" A value of 1.0 = exactly at the YELLOW threshold. A value of 2.0 = twice the threshold.

### Example

```
Historical average mix (from profile):
  tv_spend:     avg_share = 0.50  (50%)
  search_spend: avg_share = 0.50  (50%)

Latest row:
  tv_spend = 750
  search_spend = 250
  total = 1000

Current shares:
  tv_spend:     750/1000 = 0.75 (75%)
  search_spend: 250/1000 = 0.25 (25%)

Per-channel deltas:
  tv_spend:     |0.75 - 0.50| = 0.25   ← max_delta
  search_spend: |0.25 - 0.50| = 0.25

max_delta = 0.25
js_div ≈ 0.0424  (computed via Jensen-Shannon formula)

Threshold check:
  max_delta (0.25) >= MIX_SHIFT_YELLOW (0.20) → True  ← YELLOW triggered
  max_delta (0.25) >= MIX_SHIFT_RED (0.30)    → False
  js_div (0.0424)  >= JS_YELLOW (0.06)        → False
  js_div (0.0424)  >= JS_RED (0.10)           → False

  yellow_trigger = True, red_trigger = False → severity = YELLOW

Drift metric:
  normalized_delta = 0.25 / 0.20 = 1.25
  normalized_js    = 0.0424 / 0.06 = 0.707
  drift_metric_current = max(1.25, 0.707) = 1.25
```

---

## 8. Jensen-Shannon Divergence Helper

```python
def _jensen_shannon_divergence(p, q, eps=1e-12) -> float:
```

**What is it?** A symmetric measure of how different two probability distributions are. It is the smoothed version of KL (Kullback-Leibler) divergence.

**The formula:**

```
M = 0.5 * (P + Q)
JS(P, Q) = 0.5 * KL(P || M) + 0.5 * KL(Q || M)

where KL(P || M) = Σ P(i) * log(P(i) / M(i))
```

**In simple terms:**
- P = current week's spend distribution (e.g., [0.75, 0.25])
- Q = historical average distribution (e.g., [0.50, 0.50])
- M = the midpoint of P and Q (e.g., [0.625, 0.375])
- JS = "how surprised would you be" seeing P when you expected Q, and vice versa

**Properties:**
- JS = 0 when distributions are identical
- JS increases as distributions diverge
- JS is always between 0 and ln(2) ≈ 0.693
- It is symmetric: JS(P, Q) = JS(Q, P)

**Why use JS instead of just max_delta?** Max delta catches single-channel shifts but misses subtle multi-channel reshuffling. JS captures the overall "shape" change across all channels simultaneously.

---

## 9. Action Pipeline: Alert + Persist + Retrain Flag

When a detector returns `detected: True`, the action handler does three things:

### ME-5806a: `handle_channel_activation_deactivation_actions`

```
1. Send async Slack alert → invoke Slack Lambda with alert_type='CHANNEL_ACTIVATION_DEACTIVATION'
2. Persist drift metric to DynamoDB → PipelineInfoHelper.update_drift_metrics(
       drift_metric_current = number of affected channels,
       retraining_required = True
   )
3. Log the event via _transfer_info
```

### ME-5806b: `handle_spend_mix_reallocation_actions`

```
1. Send async Slack alert → invoke Slack Lambda with alert_type='SPEND_MIX_REALLOCATION'
2. Persist drift metric to DynamoDB → PipelineInfoHelper.update_drift_metrics(
       drift_metric_current = normalized max(JS, delta),
       retraining_required = True
   )
3. Log the event via _transfer_info
```

Both use `lambda_client.invoke(InvocationType='Event')` for **asynchronous** Slack notification -- the data transfer Lambda does not wait for Slack to respond.

---

## 10. Slack Lambda: Formatting & Routing

### Routing (lambda_handler)

The Slack Lambda receives the alert payload and routes based on `alert_type`:

```
alert_type == 'CHANNEL_ACTIVATION_DEACTIVATION'
  → handle_channel_activation_deactivation_alert()

alert_type == 'SPEND_MIX_REALLOCATION'
  → handle_spend_mix_reallocation_alert()
```

These are placed after the `KPI_BEHAVIOR_BREAK` branch and before `DUPLICATE_DATES` in the handler chain.

### format_channel_activation_deactivation_alert

Builds a Slack Block Kit message:
- Header: "CHANNEL ACTIVATION/DEACTIVATION DETECTED"
- @here mention
- Fields: Client, Brand/Retailer, Environment, File, Severity, Affected Channels count
- Signals section: up to 6 anomalies with channel name, type (ACTIVATION/DEACTIVATION), current spend, history weeks, and threshold
- Context footer: timestamp and correlation_id

### format_spend_mix_reallocation_alert

Builds a Slack Block Kit message:
- Header: "SPEND MIX REALLOCATION DETECTED"
- @here mention
- Fields: Client, Brand/Retailer, Environment, File, Severity, Drift Metric (Normalized)
- Distribution Shift section: JS Divergence value, Max Abs Delta, top 4 channels sorted by largest delta showing `baseline → current` shares
- Context footer: timestamp and correlation_id

---

## 11. End-to-End Flow with Example Dataset

### The Dataset

A retailer CSV arrives with 26 rows (25 historical weeks + 1 new week):

```
Date        | tv_spend | search_spend | tiktok_spend | Sales
------------|----------|--------------|--------------|------
2025-01-06  |   5000   |    3000      |      0       |  800
2025-01-13  |   5200   |    2900      |      0       |  810
2025-01-20  |   4800   |    3100      |      0       |  790
... (22 more weeks, tv/search always >0, tiktok always 0)
2025-06-30  |   5100   |    3100      |      0       |  820
2025-07-07  |      0   |    2000      |    6000      |  850   ← LATEST WEEK
```

### Step 1: prepare_drift_data

- historical_df = rows 1-25 (Jan 6 to Jun 30)
- latest_df = row 26 (Jul 7)
- is_eligible = True (25 >= 20)

### Step 2: infer_schema

```
spend_cols = ["tv_spend", "search_spend", "tiktok_spend"]
kpi_col = "Sales"
```

### Step 3: build_reference_profile

**spend_stats:**
```
tv_spend:      mean=5040, std=150, active_weeks=25
search_spend:  mean=3020, std=100, active_weeks=25
tiktok_spend:  mean=0,    std=0,   active_weeks=0
```

**mix_profile:**
```
avg_share = [0.625, 0.375, 0.0]   # tv=62.5%, search=37.5%, tiktok=0%
spend_cols = ["tv_spend", "search_spend", "tiktok_spend"]
```

### Step 4: ME-5401 + ME-5402 (existing detectors run first)

These run on the same `drift_profile`. Not detailed here.

### Step 5: ME-5806a -- Channel Activation/Deactivation

```
Loop through spend_stats:

1. tv_spend:
   current_value = 0 (latest week)
   active_weeks = 25
   Check: current <= 0 AND active_weeks (25) >= min_active_weeks (8)
   → DEACTIVATION flagged! TV was reliably active and just stopped.

2. search_spend:
   current_value = 2000 (latest week)
   active_weeks = 25
   Check: current > 0 AND active_weeks (25) >= 8 → No anomaly (stable channel still active)

3. tiktok_spend:
   current_value = 6000 (latest week)
   active_weeks = 0
   Check: current > 0 AND active_weeks (0) < 8
   → ACTIVATION flagged! Never spent before, now spending $6000.

Result:
  detected = True
  severity = "RED"
  anomalies = [
    {channel: "tv_spend", type: "DEACTIVATION", current_value: 0, active_weeks: 25, threshold: 8},
    {channel: "tiktok_spend", type: "ACTIVATION", current_value: 6000, active_weeks: 0, threshold: 8},
  ]
  drift_metric_current = 2.0
```

### Step 6: ME-5806b -- Spend Mix Reallocation

```
Baseline avg_share = [0.625, 0.375, 0.0]  # tv, search, tiktok

Latest week spend: tv=0, search=2000, tiktok=6000 → total=8000

Current shares:
  tv:      0/8000    = 0.0000
  search:  2000/8000 = 0.2500
  tiktok:  6000/8000 = 0.7500

Per-channel deltas:
  tv:      |0.0000 - 0.625| = 0.625
  search:  |0.2500 - 0.375| = 0.125
  tiktok:  |0.7500 - 0.000| = 0.750   ← max_delta

max_delta = 0.750
js_div ≈ 0.45 (very high -- distributions are completely different)

Threshold checks:
  0.750 >= 0.20 (YELLOW) → True
  0.750 >= 0.30 (RED)    → True    ← RED triggered
  0.45  >= 0.06 (YELLOW) → True
  0.45  >= 0.10 (RED)    → True    ← RED triggered

severity = "RED"

Drift metric:
  normalized_delta = 0.750 / 0.20 = 3.75
  normalized_js    = 0.45 / 0.06 = 7.5
  drift_metric_current = max(3.75, 7.5) = 7.5
```

### Step 7: Actions triggered

**For ME-5806a:**
1. Slack Lambda invoked with `alert_type='CHANNEL_ACTIVATION_DEACTIVATION'`, 2 anomalies.
2. DynamoDB updated: `retraining_required = True`, `drift_metric_current = 2.0`.
3. Datadog counter emitted: `data_ingestion.drift.channel_activation_deactivation` (severity:RED).

**For ME-5806b:**
1. Slack Lambda invoked with `alert_type='SPEND_MIX_REALLOCATION'`, severity=RED.
2. DynamoDB updated: `retraining_required = True`, `drift_metric_current = 7.5`.
3. Datadog counter emitted: `data_ingestion.drift.spend_mix_reallocation` (severity:RED).

### Step 8: Slack messages posted

**Channel Activation/Deactivation alert:**
```
🚨 CHANNEL ACTIVATION/DEACTIVATION DETECTED
@here ME-5806a anomaly detected; retraining_required has been set to true.

Client:        acme_corp
Brand/Retailer: AcmeBrand / Target
Severity:      RED
Affected:      2

Signals:
1. `tv_spend` [DEACTIVATION] | spend=0, history=25wks (needs 8wks minimum)
2. `tiktok_spend` [ACTIVATION] | spend=6000, history=0wks (needs 8wks minimum)
```

**Spend Mix Reallocation alert:**
```
🚨 SPEND MIX REALLOCATION DETECTED
@here ME-5806b anomaly detected; retraining_required has been set to true.

Client:        acme_corp
Brand/Retailer: AcmeBrand / Target
Severity:      RED
Drift Metric:  7.5

Distribution Shift:
• JS Divergence: 0.45
• Max Abs Delta: 0.75
    1. `tiktok_spend`: 0.000 ➔ 0.750
    2. `tv_spend`:     0.625 ➔ 0.000
    3. `search_spend`: 0.375 ➔ 0.250
```

### Step 9: drift_checked_result tag

After all four detectors run, the `process_file` function emits a summary Datadog metric with a `result` tag. Priority order:
1. `spend_shift` (ME-5401 found something)
2. `kpi_break` (ME-5402 found something)
3. `channel_activation` (ME-5806a found something)
4. `spend_mix` (ME-5806b found something)
5. `clean` (nothing detected)

In our example, all detectors may fire, so the tag would be `result:spend_shift` (ME-5401 takes priority).

---

## 12. Datadog Metrics Emitted

| Metric | Type | When | Tags |
|--------|------|------|------|
| `data_ingestion.drift.channel_activation_count` | gauge | Always (value = count of anomalies) | client_id, brand_name, retailer_id |
| `data_ingestion.drift.channel_activation_deactivation` | counter | Only when ME-5806a detects drift | + severity |
| `data_ingestion.drift.spend_mix_normalized` | gauge | Always (value = normalized drift metric) | client_id, brand_name, retailer_id |
| `data_ingestion.drift.spend_mix_reallocation` | counter | Only when ME-5806b detects drift | + severity |
| `data_ingestion.drift.checked` | counter | Always | + result (clean/channel_activation/spend_mix/etc.) |

---

## 13. Unit Tests

### test_me5806a_channel_activation_deactivation.py

| Test | What it validates |
|------|-------------------|
| `test_detect_channel_activation_deactivation_no_drift` | All channels have 10+ active weeks and are spending → no anomalies |
| `test_detect_channel_activation_deactivation_activation_drift` | A channel with only 3 active weeks starts spending → ACTIVATION RED |
| `test_detect_channel_activation_deactivation_deactivation_drift` | A channel with 50 active weeks stops spending → DEACTIVATION RED |
| `test_detect_channel_activation_deactivation_empty_df` | Empty DataFrame → no crash, detected=False |

### test_me5806b_spend_mix_reallocation.py

| Test | What it validates |
|------|-------------------|
| `test_jensen_shannon_divergence_identical` | JS([0.5, 0.5], [0.5, 0.5]) ≈ 0 |
| `test_jensen_shannon_divergence_different` | JS([1, 0], [0, 1]) > 0 |
| `test_detect_spend_mix_reallocation_no_drift` | 50/50 current matches 50/50 baseline → no drift |
| `test_detect_spend_mix_reallocation_max_delta_drift_yellow` | 75/25 vs 50/50 → delta=0.25 > 0.20 → YELLOW |
| `test_detect_spend_mix_reallocation_drift_red` | 90/10 vs 50/50 → delta=0.40 > 0.30 → RED |

Run them:
```bash
cd data_ingestion_pipeline
python -m pytest tests/layers/compute/unit/test_me5806a_channel_activation_deactivation.py tests/layers/compute/unit/test_me5806b_spend_mix_reallocation.py -v
```

---

## 14. FAQ / Troubleshooting

**Q: Why is ME-5806a always RED, never YELLOW?**
A: Channel structure changes (a channel appearing or disappearing) fundamentally alter the model's feature space. There is no "mild" version of this -- if the channel set changes, the model must retrain.

**Q: Can ME-5806a and ME-5806b both fire on the same file?**
A: Yes. They are independent checks. In the example above, both fired because TikTok activated (ME-5806a) AND the spend distribution shifted massively (ME-5806b).

**Q: What if a channel has exactly 8 active weeks and spends $0 this week?**
A: `active_weeks (8) >= min_active_weeks (8)` → True → DEACTIVATION flagged. The threshold is inclusive.

**Q: What if avg_share is a list from the dynamic profile but a dict in tests?**
A: The detector handles both formats. In production, `build_reference_profile` outputs `avg_share` as a list aligned with `spend_cols`. In test fixtures, it may be passed as a dict. The normalization logic at the top of `detect_spend_mix_reallocation` handles both.

**Q: Does ME-5806b fire even when ME-5401 also fires?**
A: Yes. All four detectors run independently. The `drift_checked_result` tag uses a priority order for Datadog labeling, but every detector that fires will send its own Slack alert and DynamoDB update.

**Q: What happens if the Slack Lambda is down?**
A: The `send_*_alert` functions use `InvocationType='Event'` (async fire-and-forget). If the Lambda invocation itself fails, the error is logged as a warning but does **not** fail the data transfer. The DynamoDB `retraining_required` flag is still set.

---

## 15. Real-World Example: `national.csv` Dataset

This section walks through both ME-5806 detectors using the **actual** `national.csv` file (75 rows, 2024-01-08 to 2025-06-02). Two scenarios are covered: one using the real latest row, and one using a mid-dataset row to demonstrate the activation path.

### 15.1 The Dataset

| Column | Description |
|--------|-------------|
| `Date` | Weekly date (Monday) |
| `Sales` | KPI column (revenue) |
| `GQV` | Control variable (Google Query Volume) |
| `Amazon_spend` | Media spend: Amazon |
| `Bestbuy_spend` | Media spend: Best Buy |
| `Walmart_spend` | Media spend: Walmart |
| `OOH_spend` | Media spend: Out-of-Home |
| `PaidSocial_spend` | Media spend: Paid Social |
| `TV_spend` | Media spend: TV |

75 data rows. The six `*_spend` columns are the spend channels the detectors evaluate.

### 15.2 Channel Activity History

Not every channel was active for the entire dataset. Here is how each channel's spending history looks:

```
Week Index:  1 ........... 21  22 ........... 41  42 ... 47  48 ... 50  51 ... 55  56 ..... 68  69 ... 71  72  73 74  75
             Jan'24            May'24             Oct'14      Nov'25      Dec'16      Jan'20       Apr'21       May  Jun'02

Amazon       ░░░░░░░░░░░░░░░░░ ████████████████████████████████████████████████████████████████████ ░░░░░░░░░░░░░ ░
Bestbuy      ████████████████████████████████████████████████████████████████████████████████████████████████████░░░ ░
Walmart      ░░░░ ███████████████████████████████████████████████████████████████████████████████ ░░░ █ ░░░░░░░░░░░ ░
OOH          ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ █████████ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ░
PaidSocial   ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ████████ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ░
TV           ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ██████████ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ░

█ = spending (>0)    ░ = zero spend    Row 75 = latest week (2025-06-02)
```

**Active weeks counted from historical rows 1-74 (used to build the profile):**

| Channel | First Active | Last Active | Active Weeks (of 74) | Pattern |
|---------|-------------|-------------|---------------------|---------|
| Amazon_spend | Week 22 (2024-05-27) | Week 68 (2025-04-14) | **47** | Active mid-dataset, then stopped |
| Bestbuy_spend | Week 1 (2024-01-08) | Week 71 (2025-05-05) | **71** | Almost always on, recently stopped |
| Walmart_spend | Week 5 (2024-02-05) | Week 72 (2025-05-12) | **65** | Active most of the time, some gaps |
| OOH_spend | Week 42 (2024-10-14) | Week 50 (2024-12-09) | **9** | Short holiday burst only |
| PaidSocial_spend | Week 48 (2024-11-25) | Week 55 (2025-01-13) | **8** | Very short holiday + Jan burst |
| TV_spend | Week 42 (2024-10-14) | Week 51 (2024-12-16) | **10** | Holiday season only |

### 15.3 Scenario A: Actual Latest Row (2025-06-02) -- Mass Deactivation

The latest week (row 75) has:

```
Date = 2025-06-02
Sales = 4,656,268.26
Amazon_spend    =  0.0
Bestbuy_spend   =  0.0
Walmart_spend   =  0.0
OOH_spend       =  0.0
PaidSocial_spend=  0.0
TV_spend        =  0.0
```

**Every single channel has zero spend.** The brand completely stopped all media spending.

#### Step 1: prepare_drift_data

- historical_df = rows 1-74 (2024-01-08 to 2025-05-26)
- latest_df = row 75 (2025-06-02)
- is_eligible = True (74 >= 20)

#### Step 2: infer_schema

```
spend_cols   = ["Amazon_spend", "Bestbuy_spend", "OOH_spend", "PaidSocial_spend", "TV_spend", "Walmart_spend"]
kpi_col      = "Sales"
control_cols = ["GQV"]
event_cols   = []   (no binary event columns in this dataset)
```

#### Step 3: build_reference_profile

The profile's `spend_stats` section captures each channel's history:

```
spend_stats = {
    "Amazon_spend":      { "mean": ~713, "std": ~337, "active_weeks": 47 },
    "Bestbuy_spend":     { "mean": ~18,451, "std": ~17,744, "active_weeks": 71 },
    "Walmart_spend":     { "mean": ~31,180, "std": ~21,349, "active_weeks": 65 },
    "OOH_spend":         { "mean": ~4,577, "std": ~12,430, "active_weeks": 9 },
    "PaidSocial_spend":  { "mean": ~538, "std": ~2,016, "active_weeks": 8 },
    "TV_spend":          { "mean": ~47,508, "std": ~137,791, "active_weeks": 10 },
}
```

The `mix_profile` would capture the historical spend distribution (shares averaged across all weeks with nonzero total spend).

#### Step 4: ME-5806a -- Channel Activation/Deactivation

The detector loops through each channel:

| Channel | current_value | active_weeks | vs threshold (8) | Decision |
|---------|--------------|-------------|-------------------|----------|
| Amazon_spend | **0.0** | 47 | 47 >= 8 → established | **DEACTIVATION** |
| Bestbuy_spend | **0.0** | 71 | 71 >= 8 → established | **DEACTIVATION** |
| Walmart_spend | **0.0** | 65 | 65 >= 8 → established | **DEACTIVATION** |
| OOH_spend | **0.0** | 9 | 9 >= 8 → established | **DEACTIVATION** |
| PaidSocial_spend | **0.0** | 8 | 8 >= 8 → established (exactly at threshold!) | **DEACTIVATION** |
| TV_spend | **0.0** | 10 | 10 >= 8 → established | **DEACTIVATION** |

**Result:**
```python
{
    'detected': True,
    'severity': 'RED',
    'drift_metric_current': 6.0,   # 6 channels affected
    'anomalies': [
        {'channel': 'Amazon_spend',      'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks': 47, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'Bestbuy_spend',     'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks': 71, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'Walmart_spend',     'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks': 65, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'OOH_spend',         'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks':  9, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'PaidSocial_spend',  'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks':  8, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'TV_spend',          'type': 'DEACTIVATION', 'current_value': 0.0, 'active_weeks': 10, 'threshold': 8, 'severity': 'RED'},
    ]
}
```

**Why it matters:** The MMM model was trained with all six media channels active. Now none are spending. The model cannot attribute any Sales movement to media -- it is operating completely outside its training domain. A full retrain is required.

**Note on PaidSocial:** It had **exactly 8** active weeks, which equals the `MIN_ACTIVE_WEEKS_FOR_STABILITY` threshold. Because the check is `active_weeks >= min_active_weeks` (inclusive), it qualifies as "established" and its deactivation is flagged. If it had 7 active weeks, it would be ignored (not enough history to be considered established).

#### Step 5: ME-5806b -- Spend Mix Reallocation

```
total_spend in latest row = 0 + 0 + 0 + 0 + 0 + 0 = 0.0
```

The detector hits this guard clause:
```python
if total_spend <= 1e-9:
    return {'detected': False, 'severity': 'NONE', 'anomalies': [], 'drift_metric_current': 0.0}
```

**Result: ME-5806b does NOT fire.**

**Why?** You cannot compute a percentage distribution when the total is zero -- dividing by zero is undefined. This is a deliberate safety check. ME-5806a already caught the problem (all channels deactivated), so ME-5806b is not needed here.

#### Step 6: Slack alert sent for ME-5806a

```
🚨 CHANNEL ACTIVATION/DEACTIVATION DETECTED
@here ME-5806a anomaly detected; retraining_required has been set to true.

Client:        <client_id>
Brand/Retailer: <brand> / national
Severity:      RED
Affected:      6

Signals:
1. `Amazon_spend` [DEACTIVATION] | spend=0, history=47wks (needs 8wks minimum)
2. `Bestbuy_spend` [DEACTIVATION] | spend=0, history=71wks (needs 8wks minimum)
3. `Walmart_spend` [DEACTIVATION] | spend=0, history=65wks (needs 8wks minimum)
4. `OOH_spend` [DEACTIVATION] | spend=0, history=9wks (needs 8wks minimum)
5. `PaidSocial_spend` [DEACTIVATION] | spend=0, history=8wks (needs 8wks minimum)
6. `TV_spend` [DEACTIVATION] | spend=0, history=10wks (needs 8wks minimum)
```

### 15.4 Scenario B: Hypothetical Latest Row at Week 42 (2024-10-14) -- New Channels Activate

For a more interesting ME-5806b demonstration, consider what would happen if the dataset contained only weeks 1-42 and week 42 was the latest row.

**Historical** = rows 1-41 (2024-01-08 to 2024-10-07)
**Latest row** = row 42 (2024-10-14):

```
Amazon_spend      =    808.00
Bestbuy_spend     =  5,035.15
Walmart_spend     = 36,310.80
OOH_spend         = 40,088.55    ← FIRST TIME EVER (was always 0 before)
PaidSocial_spend  =      0.00
TV_spend          = 285,730.99   ← FIRST TIME EVER (was always 0 before)
```

**Active weeks in historical rows 1-41:**

| Channel | Active Weeks (of 41) |
|---------|---------------------|
| Amazon_spend | 20 (weeks 22-41) |
| Bestbuy_spend | 41 (every week) |
| Walmart_spend | 37 (weeks 5-41) |
| OOH_spend | **0** (never active before) |
| PaidSocial_spend | 0 (never active before) |
| TV_spend | **0** (never active before) |

#### ME-5806a on this scenario

| Channel | current_value | active_weeks | Decision |
|---------|--------------|-------------|----------|
| Amazon_spend | 808.00 | 20 | 20 >= 8 → OK (stable, still active) |
| Bestbuy_spend | 5,035.15 | 41 | 41 >= 8 → OK |
| Walmart_spend | 36,310.80 | 37 | 37 >= 8 → OK |
| OOH_spend | **40,088.55** | **0** | 0 < 8 → **ACTIVATION** |
| PaidSocial_spend | 0.00 | 0 | current=0, active=0 → neither check triggers (not established AND not active) |
| TV_spend | **285,730.99** | **0** | 0 < 8 → **ACTIVATION** |

**Result:**
```python
{
    'detected': True,
    'severity': 'RED',
    'drift_metric_current': 2.0,
    'anomalies': [
        {'channel': 'OOH_spend', 'type': 'ACTIVATION', 'current_value': 40088.55, 'active_weeks': 0, 'threshold': 8, 'severity': 'RED'},
        {'channel': 'TV_spend',  'type': 'ACTIVATION', 'current_value': 285730.99, 'active_weeks': 0, 'threshold': 8, 'severity': 'RED'},
    ]
}
```

**Why it matters:** The brand has launched TV and OOH for the first time ever. The model was trained without these channels. It has no coefficients for them and cannot properly attribute their impact. A retrain is mandatory.

**Note on PaidSocial:** It has 0 active weeks AND current spend is 0. Neither the ACTIVATION check (current > 0 but history < 8) nor the DEACTIVATION check (current = 0 but history >= 8) applies. It's simply a channel that has never been used and still isn't -- no anomaly.

#### ME-5806b on this scenario

**Baseline avg_share (from historical rows 1-41):**

In the first 41 weeks, only Amazon, Bestbuy, and Walmart had any spend. OOH, PaidSocial, and TV were always zero.

```
Approximate historical avg_share:
  Amazon_spend       ≈ 0.014  (very small spend, ~$700/week)
  Bestbuy_spend      ≈ 0.293  (moderate, ~$16k/week)
  Walmart_spend      ≈ 0.693  (largest, ~$38k/week)
  OOH_spend          = 0.000  (never active)
  PaidSocial_spend   = 0.000  (never active)
  TV_spend           = 0.000  (never active)
```

**Current shares from latest row (week 42):**

```
Total spend = 808 + 5,035.15 + 36,310.80 + 40,088.55 + 0 + 285,730.99 = 367,973.49

Amazon_spend:       808.00 / 367,973.49 = 0.0022  (0.2%)
Bestbuy_spend:    5,035.15 / 367,973.49 = 0.0137  (1.4%)
Walmart_spend:   36,310.80 / 367,973.49 = 0.0987  (9.9%)
OOH_spend:       40,088.55 / 367,973.49 = 0.1089  (10.9%)
PaidSocial_spend:     0.00 / 367,973.49 = 0.0000  (0.0%)
TV_spend:       285,730.99 / 367,973.49 = 0.7764  (77.6%)
```

**Per-channel deltas (|current - baseline|):**

| Channel | Baseline Share | Current Share | Delta |
|---------|---------------|---------------|-------|
| Amazon_spend | 0.014 | 0.002 | 0.012 |
| Bestbuy_spend | 0.293 | 0.014 | **0.279** |
| Walmart_spend | 0.693 | 0.099 | **0.594** |
| OOH_spend | 0.000 | 0.109 | **0.109** |
| PaidSocial_spend | 0.000 | 0.000 | 0.000 |
| TV_spend | 0.000 | 0.776 | **0.776** ← max_delta |

```
max_delta = 0.776  (TV went from 0% to 77.6% of all spend)
js_div ≈ 0.52     (very high -- distributions are radically different)
```

**Threshold checks:**

| Check | Value | Threshold | Result |
|-------|-------|-----------|--------|
| max_delta >= MIX_SHIFT_YELLOW | 0.776 >= 0.20 | YELLOW | Triggered |
| max_delta >= MIX_SHIFT_RED | 0.776 >= 0.30 | RED | **Triggered** |
| js_div >= JS_YELLOW | 0.52 >= 0.06 | YELLOW | Triggered |
| js_div >= JS_RED | 0.52 >= 0.10 | RED | **Triggered** |

**Severity = RED** (both RED thresholds exceeded).

**Normalized drift metric:**
```
normalized_delta = 0.776 / 0.20 = 3.88
normalized_js    = 0.52 / 0.06  = 8.67
drift_metric_current = max(3.88, 8.67) = 8.67
```

A drift_metric_current of **8.67** means the JS divergence is nearly **9x** the YELLOW threshold -- an extreme reallocation.

**Slack alert for ME-5806b:**

```
🚨 SPEND MIX REALLOCATION DETECTED
@here ME-5806b anomaly detected; retraining_required has been set to true.

Client:        <client_id>
Brand/Retailer: <brand> / national
Severity:      RED
Drift Metric:  8.67

Distribution Shift:
• JS Divergence: 0.52
• Max Abs Delta: 0.776
    1. `TV_spend`:       0.000 ➔ 0.776
    2. `Walmart_spend`:  0.693 ➔ 0.099
    3. `Bestbuy_spend`:  0.293 ➔ 0.014
    4. `OOH_spend`:      0.000 ➔ 0.109
```

### 15.5 Key Takeaways from the National Dataset

| Insight | Scenario A (Week 75) | Scenario B (Week 42) |
|---------|---------------------|---------------------|
| **ME-5806a fires?** | Yes -- 6 DEACTIVATION anomalies | Yes -- 2 ACTIVATION anomalies (OOH, TV) |
| **ME-5806b fires?** | **No** -- total spend = $0, cannot compute shares | Yes -- RED, drift metric = 8.67 |
| **Why is ME-5806b silent in Scenario A?** | Dividing by zero is undefined. When nobody spends anything, there is no "mix" to compare. ME-5806a already flagged the problem. | N/A |
| **Edge case: PaidSocial** | 8 active weeks = exactly the threshold → flagged as DEACTIVATION | 0 active weeks + $0 current spend → not flagged (neither activated nor established) |
| **In reality, what happens?** | Brand stopped all advertising. The model has no media inputs to attribute Sales to. Retrain is critical. | Brand launched TV ($285k) and OOH ($40k) for the holiday season. The model has never seen these channels. Retrain is critical. |
