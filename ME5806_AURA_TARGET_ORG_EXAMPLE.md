# ME-5806 Drift Detection: Aura_Target_Org Dataset Walkthrough

This document walks through both ME-5806 detectors using the **real** `Aura_Target_Org.csv` dataset, step by step.

For full theory, thresholds, and code explanations, see [ME5806_CHANNEL_AND_SPEND_MIX_DRIFT_DETECTION.md](ME5806_CHANNEL_AND_SPEND_MIX_DRIFT_DETECTION.md).

---

## 1. The Dataset

**File:** `data_ingestion_pipeline/Aura_Target_Org.csv`

| Property | Value |
|----------|-------|
| Rows | 158 (weekly, 02-Jan-2023 to 29-Dec-2025) |
| KPI Column | `Target_sales` |
| Control Columns | `GQV`, `Seasonality` |
| Event Columns | `Valentine`, `Easter`, `Thanksgiving`, `Christmas`, `SuperBowl`, `CyberMonday`, `NewYear`, `BackToSchool` |

**Spend Channels (6):**

| Channel | Typical Range ($/week) | Role |
|---------|----------------------|------|
| `Target_spend` | $15k -- $85k | Retailer media (Target.com) |
| `Paid_Social_spend` | $4k -- $82k | Social media advertising |
| `Google_spend` | $8k -- $70k | Search advertising |
| `Meta_spend` | $7k -- $86k | Meta platform ads |
| `TV_spend` | $63k -- $252k | Television (largest channel) |
| `OOH_spend` | $30k -- $133k | Out-of-Home (billboards, transit) |

---

## 2. Channel Activity History

Unlike the `national.csv` dataset where channels turned on and off, the Aura Target dataset shows **all 6 channels active in every single week** across the entire 158-row history.

```
Week:  1 .......................... 78 .......................... 157  158
       Jan'23                       Jun'24                      Dec'25  Latest

Target_spend       ██████████████████████████████████████████████████████████████
Paid_Social_spend  ██████████████████████████████████████████████████████████████
Google_spend       ██████████████████████████████████████████████████████████████
Meta_spend         ██████████████████████████████████████████████████████████████
TV_spend           ██████████████████████████████████████████████████████████████
OOH_spend          ██████████████████████████████████████████████████████████████

█ = spending (>0) in every row — no gaps anywhere
```

**Active weeks for each channel (historical rows 1-157):**

| Channel | Active Weeks (of 157) | Min Spend | Max Spend |
|---------|----------------------|-----------|-----------|
| Target_spend | **157** | $13,526.92 | $84,826.21 |
| Paid_Social_spend | **157** | $4,296.81 | $82,287.05 |
| Google_spend | **157** | $8,514.57 | $70,535.02 |
| Meta_spend | **157** | $7,494.57 | $86,500.64 |
| TV_spend | **157** | $62,084.15 | $251,612.45 |
| OOH_spend | **157** | $29,920.61 | $132,653.88 |

Every channel has been active **every week** for over 3 years. All channels are firmly "established" (157 >> 8 weeks threshold).

---

## 3. Step-by-Step: prepare_drift_data

```
Input: 158 rows sorted by Date (02-Jan-2023 to 29-Dec-2025)

historical_df = rows 1-157 (02-Jan-2023 to 22-Dec-2025)  → 157 rows
latest_df     = row 158    (29-Dec-2025)                  → 1 row
is_eligible   = True       (157 >= 20 = MIN_ROWS_FOR_DRIFT)
```

---

## 4. Step-by-Step: infer_schema

The schema inference scans column names:

```python
schema = {
    "date_col":     "Date",
    "kpi_col":      "Target_sales",           # auto-detected: ends with '_sales'
    "spend_cols":   [                          # auto-detected: ends with '_spend'
        "Google_spend",
        "Meta_spend",
        "OOH_spend",
        "Paid_Social_spend",
        "Target_spend",
        "TV_spend",
    ],
    "control_cols": ["GQV", "Seasonality"],    # known control names
    "event_cols":   [                          # known event names + binary columns
        "BackToSchool", "Christmas", "CyberMonday",
        "Easter", "NewYear", "SuperBowl",
        "Thanksgiving", "Valentine",
    ],
}
```

This dataset has a rich schema: 6 spend channels, 2 controls, and 8 event indicators -- exactly the kind of data the dynamic drift profile is designed for.

---

## 5. Step-by-Step: build_reference_profile

The profile is built from the **157 historical rows** (excluding the latest row).

### 5a. spend_stats (used by ME-5806a)

For each spend column, the profile builder computes mean, std, p99, p995, and **active_weeks**:

```
spend_stats = {
    "Target_spend": {
        "mean":          ~36,400,
        "std":           ~13,800,
        "p99":           ~76,000,
        "p995":          ~80,000,
        "active_weeks":  157          ← every week had Target spend > 0
    },
    "Paid_Social_spend": {
        "mean":          ~24,200,
        "std":           ~11,700,
        "active_weeks":  157
    },
    "Google_spend": {
        "mean":          ~29,500,
        "std":           ~12,100,
        "active_weeks":  157
    },
    "Meta_spend": {
        "mean":          ~30,100,
        "std":           ~15,600,
        "active_weeks":  157
    },
    "TV_spend": {
        "mean":          ~120,300,
        "std":           ~34,200,
        "active_weeks":  157          ← TV is the dominant channel
    },
    "OOH_spend": {
        "mean":          ~72,100,
        "std":           ~22,400,
        "active_weeks":  157
    },
}
```

**Key observation:** Every channel has `active_weeks = 157`. This means all channels are deeply established and well above the `MIN_ACTIVE_WEEKS_FOR_STABILITY = 8` threshold.

### 5b. mix_profile (used by ME-5806b)

The profile builder computes the average spend share across all historical rows. For each row it divides each channel's spend by that row's total, then averages across all 157 rows.

**How it's calculated (showing 3 sample rows):**

```
Row 1 (02-Jan-2023):
  Total spend = 23,087.56 + 14,618.34 + 25,488.19 + 26,667.61 + 89,325.82 + 87,920.42 = 267,107.94
  Shares: Target=0.0865, PaidSocial=0.0547, Google=0.0954, Meta=0.0999, TV=0.3345, OOH=0.3292

Row 78 (17-Jun-2024):
  Total spend = 36,699.29 + 32,696.67 + 23,103.21 + 19,852.27 + 160,837.13 + 46,027.57 = 319,216.14
  Shares: Target=0.1150, PaidSocial=0.1024, Google=0.0724, Meta=0.0622, TV=0.5037, OOH=0.1442

Row 157 (22-Dec-2025):
  Total spend = 43,392.48 + 18,696.98 + 23,405.91 + 63,267.50 + 198,235.88 + 116,897.91 = 463,896.66
  Shares: Target=0.0935, PaidSocial=0.0403, Google=0.0504, Meta=0.1363, TV=0.4273, OOH=0.2520
```

**Average across all 157 rows → avg_share:**

```python
mix_profile = {
    "spend_cols": ["Google_spend", "Meta_spend", "OOH_spend",
                   "Paid_Social_spend", "Target_spend", "TV_spend"],
    "avg_share":  [0.096, 0.097, 0.232, 0.078, 0.117, 0.381]
    #              Google  Meta   OOH    Social  Target  TV
}
```

Approximate historical baseline distribution:

```
TV_spend           ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  38.1%   ← dominant channel
OOH_spend          ▓▓▓▓▓▓▓▓▓▓▓▓▓▓           23.2%
Target_spend       ▓▓▓▓▓▓▓                   11.7%
Meta_spend         ▓▓▓▓▓▓                     9.7%
Google_spend       ▓▓▓▓▓▓                     9.6%
Paid_Social_spend  ▓▓▓▓▓                      7.8%
```

### 5c. thresholds (from dynamic_drift_profile.py constants)

```python
thresholds = {
    "MIN_ACTIVE_WEEKS_FOR_STABILITY": 8,
    "MIX_SHIFT_YELLOW": 0.20,
    "MIX_SHIFT_RED": 0.30,
    "JS_YELLOW": 0.06,
    "JS_RED": 0.10,
    "CONSEC_ZERO_WEEKS_RED": 4,
    ...
}
```

---

## 6. ME-5806a: Channel Activation/Deactivation

### Latest row (29-Dec-2025):

```
Target_spend       = 50,713.44
Paid_Social_spend  = 16,267.04
Google_spend       = 21,977.93
Meta_spend         = 11,622.57
TV_spend           = 100,706.08
OOH_spend          = 128,549.22
```

### Detector logic for each channel:

The detector loops through `spend_stats` and for each channel checks two conditions:

| Channel | current_value | > 0 ? | active_weeks | vs threshold (8) | Decision |
|---------|--------------|-------|-------------|-------------------|----------|
| Target_spend | 50,713.44 | Yes | 157 | 157 >= 8 → established | **No anomaly** (established channel, still active) |
| Paid_Social_spend | 16,267.04 | Yes | 157 | 157 >= 8 → established | **No anomaly** |
| Google_spend | 21,977.93 | Yes | 157 | 157 >= 8 → established | **No anomaly** |
| Meta_spend | 11,622.57 | Yes | 157 | 157 >= 8 → established | **No anomaly** |
| TV_spend | 100,706.08 | Yes | 157 | 157 >= 8 → established | **No anomaly** |
| OOH_spend | 128,549.22 | Yes | 157 | 157 >= 8 → established | **No anomaly** |

### Why no anomaly is flagged:

For **ACTIVATION** to trigger: `current_value > 0 AND active_weeks < 8`
- Every channel has `active_weeks = 157`, which is far above 8. No channel is "new."

For **DEACTIVATION** to trigger: `current_value <= 0 AND active_weeks >= 8`
- Every channel has `current_value > 0` -- all channels are still spending.

Neither condition applies to any channel.

### Result:

```python
{
    'detected': False,
    'severity': 'NONE',
    'anomalies': [],
    'drift_metric_current': 0.0
}
```

**ME-5806a does NOT fire.** The channel structure is perfectly stable -- the same 6 channels that have always been active are still active. No Slack alert, no DynamoDB update, no retrain flag.

---

## 7. ME-5806b: Spend Mix Reallocation

### Current shares from latest row (29-Dec-2025):

```
Total spend = 50,713.44 + 16,267.04 + 21,977.93 + 11,622.57 + 100,706.08 + 128,549.22
            = 329,836.28
```

| Channel | Latest Spend ($) | Current Share | Calculation |
|---------|-----------------|---------------|-------------|
| Target_spend | 50,713.44 | **0.1538** (15.4%) | 50,713.44 / 329,836.28 |
| Paid_Social_spend | 16,267.04 | **0.0493** (4.9%) | 16,267.04 / 329,836.28 |
| Google_spend | 21,977.93 | **0.0666** (6.7%) | 21,977.93 / 329,836.28 |
| Meta_spend | 11,622.57 | **0.0352** (3.5%) | 11,622.57 / 329,836.28 |
| TV_spend | 100,706.08 | **0.3053** (30.5%) | 100,706.08 / 329,836.28 |
| OOH_spend | 128,549.22 | **0.3897** (39.0%) | 128,549.22 / 329,836.28 |

### Per-channel delta (|current_share - baseline_share|):

| Channel | Baseline Share | Current Share | Delta | Direction |
|---------|---------------|---------------|-------|-----------|
| Target_spend | 0.117 (11.7%) | 0.154 (15.4%) | **0.037** | ↑ +3.7pp |
| Paid_Social_spend | 0.078 (7.8%) | 0.049 (4.9%) | **0.029** | ↓ -2.9pp |
| Google_spend | 0.096 (9.6%) | 0.067 (6.7%) | **0.029** | ↓ -2.9pp |
| Meta_spend | 0.097 (9.7%) | 0.035 (3.5%) | **0.062** | ↓ -6.2pp |
| TV_spend | 0.381 (38.1%) | 0.305 (30.5%) | **0.076** | ↓ -7.6pp |
| OOH_spend | 0.232 (23.2%) | 0.390 (39.0%) | **0.158** | ↑ +15.8pp ← max_delta |

```
max_delta = 0.158  (OOH went from ~23.2% to ~39.0% of total spend)
```

### Visual: baseline vs. current distribution

```
Channel          Baseline              Current               Delta
─────────────────────────────────────────────────────────────────────
TV_spend         ████████████████ 38.1% ████████████  30.5%   -7.6pp
OOH_spend        █████████  23.2%      ████████████████ 39.0% +15.8pp ← biggest shift
Target_spend     █████  11.7%          ██████  15.4%          +3.7pp
Meta_spend       ████  9.7%            ██  3.5%               -6.2pp
Google_spend     ████  9.6%            ███  6.7%              -2.9pp
Paid_Social      ███  7.8%             ██  4.9%               -2.9pp
```

### Jensen-Shannon divergence:

```
P (current)  = [0.0666, 0.0352, 0.3897, 0.0493, 0.1538, 0.3053]
Q (baseline) = [0.096,  0.097,  0.232,  0.078,  0.117,  0.381 ]

Step 1: M = 0.5 * (P + Q)
  M = [0.0813, 0.0661, 0.3109, 0.0637, 0.1354, 0.3432]

Step 2: KL(P || M)
  = Σ P(i) * log(P(i) / M(i))
  = 0.0666*log(0.0666/0.0813) + 0.0352*log(0.0352/0.0661) + 0.3897*log(0.3897/0.3109)
    + 0.0493*log(0.0493/0.0637) + 0.1538*log(0.1538/0.1354) + 0.3053*log(0.3053/0.3432)
  ≈ 0.0158

Step 3: KL(Q || M)
  = Σ Q(i) * log(Q(i) / M(i))
  ≈ 0.0162

Step 4: JS = 0.5 * KL(P||M) + 0.5 * KL(Q||M)
  ≈ 0.5 * 0.0158 + 0.5 * 0.0162
  ≈ 0.016
```

### Threshold check:

| Check | Value | Threshold | Triggered? |
|-------|-------|-----------|------------|
| max_delta >= MIX_SHIFT_YELLOW | 0.158 >= 0.20 | YELLOW | **No** |
| max_delta >= MIX_SHIFT_RED | 0.158 >= 0.30 | RED | **No** |
| js_div >= JS_YELLOW | 0.016 >= 0.06 | YELLOW | **No** |
| js_div >= JS_RED | 0.016 >= 0.10 | RED | **No** |

```
yellow_trigger = False OR False = False
red_trigger    = False OR False = False
```

**Neither threshold is breached.**

### Result:

```python
{
    'detected': False,
    'severity': 'NONE',
    'anomalies': [],
    'drift_metric_current': 0.0
}
```

**ME-5806b does NOT fire.** The spend distribution shifted modestly (OOH up ~16pp, TV down ~8pp) but stayed well within normal operating ranges. No Slack alert, no retrain flag.

---

## 8. Why Neither Detector Fires -- Interpretation

This is the **healthy baseline** case. The Aura Target dataset represents a mature, stable advertiser:

| Property | Aura Target | National (Section 15 of main doc) |
|----------|------------|----------------------------------|
| Channel history | All 6 active for 157 weeks straight | Channels turned on/off throughout |
| Latest week spend | All 6 channels spending | All 6 channels at $0 |
| ME-5806a result | **Clean** (no activation/deactivation) | **6 DEACTIVATION anomalies** |
| ME-5806b result | **Clean** (max_delta=0.158 < 0.20) | N/A (total spend = 0) |
| Retrain required? | **No** | **Yes** |

The Aura Target brand has been running a consistent media mix for 3 years. The model's coefficients were trained on this exact channel structure and spend distribution. The latest week is business-as-usual.

**This is exactly what the detectors are designed to do: distinguish normal operational fluctuation from structural change.**

- A 16-percentage-point shift in OOH's share is notable, but not structural. OOH naturally fluctuates between 23% and 39% over time.
- A channel going from 50 active weeks to $0 spend (as in the national.csv example) IS structural -- it fundamentally changes the model's input space.

---

## 9. What Would Make ME-5806 Fire on This Dataset?

To illustrate the thresholds, here are hypothetical latest rows that WOULD trigger alerts:

### Scenario: ME-5806a would fire if...

**A channel drops to zero:**
```
Hypothetical latest row:
  Target_spend       = 50,000
  Paid_Social_spend  = 16,000
  Google_spend       = 22,000
  Meta_spend         =      0    ← was active 157 weeks, now zero
  TV_spend           = 100,000
  OOH_spend          = 128,000
```

Result: `Meta_spend` → DEACTIVATION (active_weeks=157 >= 8 AND current=0). Severity=RED.

**A brand-new channel appears:**

If the CSV suddenly contained a column `TikTok_spend` with $50,000 in the latest row but only 2 weeks of history in the profile → ACTIVATION.

### Scenario: ME-5806b would fire if...

**TV and OOH swap places (max_delta >= 0.20):**
```
Hypothetical latest row:
  Target_spend       =  35,000
  Paid_Social_spend  =  24,000
  Google_spend       =  29,000
  Meta_spend         =  30,000
  TV_spend           =  70,000   ← normally ~38%, now ~23%
  OOH_spend          = 120,000   ← normally ~23%, now ~39%
  Total              = 308,000

TV share:  70,000/308,000 = 0.227 → delta = |0.227 - 0.381| = 0.154  (still < 0.20)
OOH share: 120,000/308,000 = 0.390 → delta = |0.390 - 0.232| = 0.158 (still < 0.20)
```

Even swapping the two largest channels doesn't quite hit the threshold. You would need:

```
Extreme reallocation:
  TV_spend  =  50,000   (share = 0.163 → delta = |0.163 - 0.381| = 0.218 ≥ 0.20 → YELLOW!)
  OOH_spend = 140,000   (share = 0.455 → delta = |0.455 - 0.232| = 0.223 ≥ 0.20 → YELLOW!)

  max_delta = 0.223 → YELLOW triggered
```

Or if JS divergence crosses 0.06:

```
All budget goes to TV:
  TV_spend  = 308,000    (share = 1.0 → delta from 0.381 = 0.619)
  All others = 0

  JS divergence would be very high (>0.5) → RED
  max_delta = 0.619 → RED
```

---

## 10. Datadog Metrics for This Run

Even though no drift is detected, the `process_file` function still emits **gauge metrics** for monitoring:

| Metric | Value | Meaning |
|--------|-------|---------|
| `data_ingestion.drift.channel_activation_count` | **0.0** | Zero channels activated or deactivated |
| `data_ingestion.drift.spend_mix_normalized` | **0.0** | No mix shift detected (below threshold) |
| `data_ingestion.drift.checked` | 1.0 (tag: `result:clean`) | Drift check completed, all clear |

No counter metrics are emitted for `channel_activation_deactivation` or `spend_mix_reallocation` because nothing was detected.

---

## 11. Summary

The `Aura_Target_Org.csv` dataset is a textbook example of a **healthy, stable media mix**:

- **All 6 channels** have been continuously active for 157 weeks
- **No channel** activates or deactivates in the latest week
- **Spend distribution** shifts only modestly (max delta = 15.8%, below the 20% yellow threshold)
- **Jensen-Shannon divergence** is small (~0.016, well below the 0.06 yellow threshold)
- **Result**: Both ME-5806a and ME-5806b return `detected: False` -- the model is operating within its training domain and does not need retraining

This contrasts sharply with the `national.csv` example (see main documentation) where all 6 channels simultaneously deactivated, triggering 6 RED alerts from ME-5806a.
