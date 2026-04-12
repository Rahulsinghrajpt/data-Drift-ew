"""
Lambda Function: mmm_dev_data_transfer

Purpose:
Download data from Tracer S3 bucket, process with pandas (date transformation,
retailer splitting), upload to VIP client buckets, and log to DynamoDB/S3.
Part of Step Function Map state for each client returned by mmm_dev_get_client.

Data Processing:
- Converts "week" columns to "Date" with YYYY-MM-DD format
- Splits multi-retailer files into per-retailer files
- Uploads to preprocessed/{brand}/{retailer}/{retailer}.csv structure

Integration Points:
- mmm_dev_get_client (Lambda 1): Provides list of active clients from pipeline-infos
- Step Function: Orchestrates parallel processing via Map state
- DynamoDB: mmm-{env}-data-transfer-logs for operation tracking
- S3: mmm-{env}-audit-logs for audit trail
- S3: Tracer bucket (source) → VIP buckets (destination)

Configuration:
- FunctionName: mmm_{env}_data_transfer
- Runtime: python3.12
- Timeout: 300 seconds (5 minutes)
- Memory: 2048 MB (2 GB)
- Layer: AWS SDK for Pandas (pandas, numpy)

Environment Variables:
- ENVIRONMENT: dev, prod, staging
- PIPELINE_INFO_TABLE: mmm-{env}-pipeline-infos
- TRANSFER_LOGS_TABLE: mmm-{env}-data-transfer-logs
- AUDIT_LOGS_BUCKET: mmm-{env}-audit-logs
- TRACER_BUCKET: Source bucket (e.g., prd-mm-vendor-sync or mmm-dev-data-stage)
- VIP_BUCKET_PREFIX: mmm-{env}-data (VIP bucket naming prefix)
- AWS_REGION: eu-west-1

Version: 3.0.0 (Pandas processing, retailer splitting, standardized paths)
Date: 2025-12-07
"""

import json
import boto3
import os
import io
import re
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from botocore.exceptions import ClientError

# Import pandas for data processing (from Lambda layer)
# Try importing with error handling for numpy issues
PANDAS_AVAILABLE = False
try:
    # Suppress numpy import warnings
    import warnings
    warnings.filterwarnings('ignore')
    import pandas as pd
    PANDAS_AVAILABLE = True
except Exception as e:
    # Pandas should be available from Lambda layer
    # If import fails, we'll handle gracefully
    import sys
    print(f"WARNING: Pandas import failed: {e}", file=sys.stderr)

# ============================================================================
# CRITICAL: Add src/ to sys.path BEFORE importing utils modules
# ============================================================================
# Lambda package structure: lambda_function.py is at root, src/ is sibling in ZIP
import sys
lambda_dir = os.path.dirname(os.path.abspath(__file__))
# In Lambda package: src/ is at same level as lambda_function.py
src_path = os.path.join(lambda_dir, 'src')
if not os.path.exists(src_path):
    # Fallback: try parent directory (for local testing)
    src_path = os.path.join(os.path.dirname(lambda_dir), 'src')
if os.path.exists(src_path):
    sys.path.insert(0, src_path)

# Import helper libraries (now that sys.path is set)
# Try importing from utils.pipeline_info_helper (when src/ is in package)
PipelineInfoHelper = None
try:
    # First try: from utils (when src/ is in package)
    from utils.pipeline_info_helper import PipelineInfoHelper
except ImportError:
    try:
        # Second try: direct import (backward compatibility)
        from pipeline_info_helper import PipelineInfoHelper
    except ImportError:
        # Fallback: not available in Lambda runtime
        PipelineInfoHelper = None
        print("WARNING: PipelineInfoHelper not available - pipeline-info table updates will fail", file=sys.stderr)

# Import column standardization module
try:
    from utils.column_standardizer import (
        standardize_csv_data,
        validate_required_columns,
        validate_column_patterns
    )
    STANDARDIZATION_AVAILABLE = True
except ImportError as e:
    # Fallback: standardization not available
    STANDARDIZATION_AVAILABLE = False
    # Logger not initialized yet, use print for now
    print(f"WARNING: Column standardization not available: {e}", file=sys.stderr)

# Import pipeline configuration
try:
    from utils.pipeline_config import PipelineConfiguration, get_config
    CONFIG_AVAILABLE = True
except ImportError as e:
    CONFIG_AVAILABLE = False
    print(f"WARNING: Pipeline configuration not available: {e}", file=sys.stderr)

# Load configuration
if CONFIG_AVAILABLE:
    config = get_config()
    ENVIRONMENT = config.pipeline.environment
    PIPELINE_INFO_TABLE = config.get_pipeline_info_table_name()
    TRANSFER_LOGS_TABLE = config.get_transfer_logs_table_name()
    AUDIT_LOGS_BUCKET = config.s3.audit_logs_bucket
    TRACER_BUCKET = config.s3.tracer_bucket
    VIP_BUCKET_PREFIX = config.s3.vip_bucket_prefix
    AWS_REGION = config.pipeline.aws_region
    VALID_EXTENSIONS = config.file_discovery.valid_extensions
    EXCLUDE_PATTERNS = config.file_discovery.exclude_patterns
    SEARCH_DEPTH_DAYS = config.file_discovery.search_depth_days
    MIN_FILE_SIZE_BYTES = config.file_discovery.min_file_size_bytes
    MIN_DATA_ROWS = config.file_discovery.min_data_rows
    SOURCE_PREFIX = config.s3.source_prefix
else:
    # Fallback to environment variables if config not available
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
    PIPELINE_INFO_TABLE = os.environ.get("PIPELINE_INFO_TABLE", f"mmm-{ENVIRONMENT}-pipeline-infos")
    TRANSFER_LOGS_TABLE = os.environ.get("TRANSFER_LOGS_TABLE") or os.environ.get("LOGS_TABLE") or f"mmm_{ENVIRONMENT}_data_transfer_logs"
    AUDIT_LOGS_BUCKET = os.environ.get("AUDIT_LOGS_BUCKET", f"mmm-{ENVIRONMENT}-audit-logs")
    TRACER_BUCKET = os.environ.get("TRACER_BUCKET", "mmm-dev-data-stage")
    VIP_BUCKET_PREFIX = os.environ.get("VIP_BUCKET_PREFIX", f"mmm-{ENVIRONMENT}-data")
    AWS_REGION = os.environ.get("AWS_REGION", "eu-west-1")
    VALID_EXTENSIONS = {'.csv', '.parquet', '.json'}
    EXCLUDE_PATTERNS = {'test', 'backup', 'archive', 'old', 'temp', 'tmp', 'sample'}
    SEARCH_DEPTH_DAYS = 2
    MIN_FILE_SIZE_BYTES = 0
    MIN_DATA_ROWS = 1
    SOURCE_PREFIX = "tracer"

# ============================================================================
# TEMPORARY VIP BUCKET OVERRIDES
# ============================================================================
# Override map for clients that need non-standard bucket names
# Remove entries when no longer needed and redeploy
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# MikAura observability (pipeline-agnostic)
try:
    from utils.mikaura_observability import (
        MikAuraObservabilityConfig,
        MikAuraStatusLogger,
        MikAuraMetricLogger,
    )
    _MIKAURA_AVAILABLE = True
except ImportError:
    _MIKAURA_AVAILABLE = False

# Pipeline-owned MikAura status vocabulary for transfer + nested mikaura JSON (not in utility defaults)
INGESTION_MIKAURA_ALLOWED_STATUSES = frozenset(
    {"running", "success", "failed", "warning", "info", "no_file"}
)

# Universal SOT column order (validation + reorder). Do not duplicate elsewhere in this module.
UNIVERSAL_SOT_COLS = [
    "Date",
    "Sales",
    "In_stock_Rate",
    "GQV",
    "OOH_impressions",
    "OOH_spend",
    "PaidSocial_impressions",
    "PaidSocial_spend",
    "TV_impressions",
    "TV_spend",
    "Promo_flag",
]

# Datadog DogStatsD fallback when MikAura metrics unavailable (utils under src/ in Lambda package)
try:
    from utils.metrics_utils import get_metrics_utils

    _pipeline_metrics = get_metrics_utils()
except ImportError:
    _pipeline_metrics = None


def _transfer_debug(
    status_logger: Optional[Any], debug_event: str, message: str, **fields: Any
) -> None:
    """Structured DEBUG via MikAura when available; else silent."""
    if status_logger:
        status_logger.log_debug(message, debug_event=debug_event, **fields)


def _transfer_running(
    status_logger: Optional[Any], message: str, **fields: Any
) -> None:
    """Phase 4: operational lifecycle on MikAura when available."""
    if status_logger:
        status_logger.log_running(message, **fields)


def _transfer_info(
    status_logger: Optional[Any], message: str, force: bool = True, **fields: Any
) -> None:
    if status_logger:
        status_logger.log_info(message, force=force, **fields)


def _transfer_warning(
    status_logger: Optional[Any], message: str, **fields: Any
) -> None:
    if status_logger:
        status_logger.log_warning(message, **fields)


def _check_logger_required(message: str) -> None:
    """Write stderr when status_logger is unavailable; optionally hard-fail via env."""
    sys.stderr.write(f"[STATUS_LOGGER_UNAVAILABLE] {message}\n")
    if os.environ.get("FAIL_ON_MISSING_LOGGER", "false").lower() == "true":
        raise RuntimeError(f"Status logger was not available: {message}")


def _transfer_error(
    status_logger: Optional[Any], message: str, reason: Optional[str] = None, **fields: Any
) -> None:
    if status_logger:
        status_logger.log_error(message, reason=reason or message, **fields)
    else:
        _check_logger_required(message)


def _transfer_failed(
    status_logger: Optional[Any], message: str, reason: str, **fields: Any
) -> None:
    if status_logger:
        status_logger.log_failed(message, reason=reason, **fields)
    else:
        _check_logger_required(f"{message}: {reason}")


def _transfer_exception(
    status_logger: Optional[Any], message: str, exc: Exception, **fields: Any
) -> None:
    if status_logger:
        status_logger.log_exception(message, exc, **fields)
    else:
        _check_logger_required(f"{message}: {exc}")


# Import-time diagnostics (no MikAura scope yet; stdout -> CloudWatch in Lambda)
if not PipelineInfoHelper:
    print("CRITICAL: PipelineInfoHelper not available - pipeline-info table updates will fail")

if not STANDARDIZATION_AVAILABLE:
    print("WARNING: Column standardization not available - data validation may be limited")

if not CONFIG_AVAILABLE:
    print("WARNING: Pipeline configuration not available - using environment variable fallbacks")

# AWS Clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
transfer_logs_table = dynamodb.Table(TRANSFER_LOGS_TABLE)
pipeline_info_table = dynamodb.Table(PIPELINE_INFO_TABLE)

# Slack notification Lambda name (for schema drift alerts)
SLACK_LAMBDA_NAME = f"mmm_{ENVIRONMENT}_data_ingestion_slack"

# Drift profile and thresholds for ME-5401 (Spend Regime Shift)
DRIFT_PROFILE_PATH = os.environ.get("DRIFT_PROFILE_PATH", "")
SPEND_REGIME_Z_YELLOW_DEFAULT = 3.0
SPEND_REGIME_Z_RED_DEFAULT = 4.0
KPI_BEHAVIOR_RESID_Z_YELLOW_DEFAULT = 3.0
KPI_BEHAVIOR_RESID_Z_RED_DEFAULT = 4.0
KPI_BEHAVIOR_OPPOSITE_MIN_RESID_Z_DEFAULT = 1.5
KPI_BEHAVIOR_OPPOSITE_MIN_SPEND_DELTA_PCT_DEFAULT = 0.15
KPI_BEHAVIOR_EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z_DEFAULT = 1.0
KPI_ALERT_MAX_RETRIES = int(os.environ.get("KPI_ALERT_MAX_RETRIES", "3"))
KPI_ALERT_RETRY_BASE_SECONDS = float(os.environ.get("KPI_ALERT_RETRY_BASE_SECONDS", "1.0"))
_DRIFT_PROFILE_CACHE: Optional[Dict[str, Any]] = None
_DRIFT_PROFILE_LOADED = False

# ME-5806a: Channel Activation/Deactivation thresholds
MIN_ACTIVE_WEEKS_DEFAULT = 8
CONSEC_ZERO_WEEKS_RED_DEFAULT = 4

# ME-5806b: Spend Mix Reallocation thresholds
MIX_SHIFT_YELLOW_DEFAULT = 0.20
MIX_SHIFT_RED_DEFAULT = 0.30
JS_YELLOW_DEFAULT = 0.06
JS_RED_DEFAULT = 0.10


def get_current_timestamp() -> str:
    """Get current timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _to_float(value: Any) -> Optional[float]:
    """Safely convert values to float; return None when conversion fails."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_profile_path() -> Optional[str]:
    """Resolve drift profile path from env var or common local paths."""
    candidates = []
    if DRIFT_PROFILE_PATH:
        candidates.append(DRIFT_PROFILE_PATH)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    candidates.extend([
        os.path.join(current_dir, 'profile.json'),
        os.path.join(os.path.dirname(current_dir), 'profile.json'),
        os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'profile.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))), 'profile.json'),
    ])
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def load_drift_profile(status_logger: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Load drift profile once per runtime; return None when unavailable.

    When ``status_logger`` is set (e.g. from ``process_file``), load/skip/failure
    lines go through MikAura JSON; otherwise helpers no-op or print minimally.
    """
    global _DRIFT_PROFILE_CACHE
    global _DRIFT_PROFILE_LOADED
    if _DRIFT_PROFILE_LOADED:
        return _DRIFT_PROFILE_CACHE
    _DRIFT_PROFILE_LOADED = True
    profile_path = _resolve_profile_path()
    if not profile_path:
        _transfer_warning(
            status_logger,
            "Spend regime shift profile not found - skipping ME-5401 checks for this run",
            debug_event="drift_profile",
        )
        _DRIFT_PROFILE_CACHE = None
        return None
    try:
        with open(profile_path, 'r', encoding='utf-8') as profile_file:
            _DRIFT_PROFILE_CACHE = json.load(profile_file)
            _transfer_info(
                status_logger,
                f"Loaded spend regime shift profile from {profile_path}",
                profile_path=profile_path,
                debug_event="drift_profile",
            )
            return _DRIFT_PROFILE_CACHE
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to load spend regime shift profile from {profile_path}: {e}",
            profile_path=profile_path,
            error=str(e),
            debug_event="drift_profile",
        )
        _DRIFT_PROFILE_CACHE = None
        return None


def detect_spend_regime_shift(
    retailer_df: Any,
    profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Detect spend magnitude shocks (ME-5401) using z-score and percentile checks.
    Trigger when |z| > threshold or spend > historical p99.
    Severity is RED when |z| >= red threshold or spend > p995.
    """
    if retailer_df is None or len(retailer_df) == 0:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0,
            'evaluated_channels': 0
        }
    spend_stats = profile.get('spend_stats', {}) if profile else {}
    thresholds = profile.get('thresholds', {}) if profile else {}
    z_yellow = _to_float(thresholds.get('Z_OUTLIER_YELLOW')) or SPEND_REGIME_Z_YELLOW_DEFAULT
    z_red = _to_float(thresholds.get('Z_OUTLIER_RED')) or SPEND_REGIME_Z_RED_DEFAULT
    latest_row = retailer_df.iloc[-1]
    anomalies = []
    max_abs_z = 0.0
    evaluated_channels = 0
    for channel_name, stats in spend_stats.items():
        if channel_name not in retailer_df.columns:
            continue
        current_value = _to_float(latest_row.get(channel_name))
        mean_value = _to_float(stats.get('mean'))
        std_value = _to_float(stats.get('std'))
        p99_value = _to_float(stats.get('p99'))
        p995_value = _to_float(stats.get('p995'))
        if current_value is None or mean_value is None or std_value is None or std_value <= 0:
            continue
        evaluated_channels += 1
        z_score = (current_value - mean_value) / std_value
        abs_z_score = abs(z_score)
        max_abs_z = max(max_abs_z, abs_z_score)
        exceeds_p99 = p99_value is not None and current_value > p99_value
        exceeds_p995 = p995_value is not None and current_value > p995_value
        yellow_trigger = abs_z_score >= z_yellow or exceeds_p99
        red_trigger = abs_z_score >= z_red or exceeds_p995
        if yellow_trigger:
            anomalies.append({
                'channel': channel_name,
                'value': round(current_value, 4),
                'mean': round(mean_value, 4),
                'std': round(std_value, 4),
                'z_score': round(z_score, 4),
                'p99': p99_value,
                'p995': p995_value,
                'threshold_breaches': {
                    'z_yellow': abs_z_score >= z_yellow,
                    'z_red': abs_z_score >= z_red,
                    'p99': exceeds_p99,
                    'p995': exceeds_p995
                },
                'severity': 'RED' if red_trigger else 'YELLOW'
            })
    severity = 'NONE'
    if anomalies:
        severity = 'RED' if any(item.get('severity') == 'RED' for item in anomalies) else 'YELLOW'
    return {
        'detected': bool(anomalies),
        'severity': severity,
        'anomalies': anomalies,
        'drift_metric_current': round(max_abs_z, 6),
        'evaluated_channels': evaluated_channels
    }


def send_spend_regime_shift_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send async Slack alert for Spend Regime Shift (ME-5401)."""
    if not detection_result or not detection_result.get('detected'):
        return True
    try:
        payload = {
            'alert_type': 'SPEND_REGIME_SHIFT',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'severity': detection_result.get('severity', 'YELLOW'),
            'drift_metric_current': detection_result.get('drift_metric_current', 0.0),
            'evaluated_channels': detection_result.get('evaluated_channels', 0),
            'anomalies': detection_result.get('anomalies', []),
            'message': 'Spend regime shift detected. Retraining has been marked as required.'
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Spend regime shift alert sent for {client_id}/{brand_name}/{retailer_id}: "
                f"severity={detection_result.get('severity')}",
                alert_type="SPEND_REGIME_SHIFT",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Spend regime shift alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="SPEND_REGIME_SHIFT",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send spend regime shift alert: {e}",
            alert_type="SPEND_REGIME_SHIFT",
        )
        return False


def handle_spend_regime_shift_actions(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> None:
    """Execute ME-5401 actions: Slack alert + retraining_required flip + drift metric update."""
    if not detection_result or not detection_result.get('detected'):
        return
    send_spend_regime_shift_alert(
        client_id=client_id,
        brand_name=brand_name,
        retailer_id=retailer_id,
        filename=filename,
        detection_result=detection_result,
        status_logger=status_logger,
    )
    if not PipelineInfoHelper:
        _transfer_warning(
            status_logger,
            "PipelineInfoHelper unavailable - cannot persist spend regime shift metrics",
            alert_type="SPEND_REGIME_SHIFT",
        )
        return
    try:
        helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)
        drift_metric_current = _to_float(detection_result.get('drift_metric_current')) or 0.0
        helper.update_drift_metrics(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            drift_metric_current=drift_metric_current,
            retraining_required=True
        )
        _transfer_info(
            status_logger,
            f"ME-5401 persisted drift metrics for {client_id}/{brand_name}/{retailer_id}: "
            f"drift_metric_current={drift_metric_current}",
            alert_type="SPEND_REGIME_SHIFT",
        )
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to persist spend regime shift metrics: {e}",
            alert_type="SPEND_REGIME_SHIFT",
        )


def _safe_log1p(value: float) -> float:
    """Numerically safe log1p for potentially noisy values."""
    if value <= -1:
        value = -0.999999
    return math.log1p(value)


def _is_event_transition_ended(previous_row: Any, latest_row: Any, event_cols: List[str]) -> bool:
    """Return True when any configured event flag transitions from active (1) to inactive (0)."""
    for event_col in event_cols or []:
        prev_event = _to_float(previous_row.get(event_col)) or 0.0
        curr_event = _to_float(latest_row.get(event_col)) or 0.0
        if prev_event >= 0.5 and curr_event < 0.5:
            return True
    return False


def _get_total_spend_mean(profile: Dict[str, Any], spend_cols: List[str]) -> float:
    """Compute expected total spend baseline from spend_stats means in profile."""
    spend_stats = profile.get('spend_stats', {}) if profile else {}
    total_mean = 0.0
    for col_name in spend_cols or []:
        mean_value = _to_float((spend_stats.get(col_name) or {}).get('mean'))
        if mean_value is not None and mean_value > 0:
            total_mean += mean_value
    return total_mean


def _validate_kpi_alert_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate required fields before invoking the Slack lambda."""
    required_fields = ['client_id', 'brand_name', 'retailer_id', 'severity', 'anomalies', 'alert_type']
    for field_name in required_fields:
        if not payload.get(field_name):
            return False, f"missing required field '{field_name}'"

    severity = str(payload.get('severity', '')).upper()
    if severity not in {'YELLOW', 'RED'}:
        return False, f"invalid severity '{severity}'"

    anomalies = payload.get('anomalies')
    if not isinstance(anomalies, list) or not anomalies:
        return False, 'anomalies must be a non-empty list'

    return True, ''


def _predict_log_kpi_from_profile(latest_row: Any, profile: Dict[str, Any]) -> Optional[float]:
    """Predict log KPI using residual profile features and coefficients."""
    residual_profile = profile.get('residual_profile', {}) if profile else {}
    feature_names = residual_profile.get('feature_names', [])
    beta = residual_profile.get('beta', [])

    if not feature_names or not beta:
        return None

    control_stats = profile.get('control_stats', {}) if profile else {}
    features = []

    for feature_name in feature_names:
        if feature_name == 'intercept':
            features.append(1.0)
            continue

        if feature_name.startswith('log1p(') and feature_name.endswith(')'):
            col_name = feature_name[6:-1]
            value = _to_float(latest_row.get(col_name))
            features.append(_safe_log1p(value if value is not None else 0.0))
            continue

        if feature_name.startswith('z(') and feature_name.endswith(')'):
            col_name = feature_name[2:-1]
            value = _to_float(latest_row.get(col_name))
            stats = control_stats.get(col_name, {})
            mean_value = _to_float(stats.get('mean'))
            std_value = _to_float(stats.get('std'))

            if value is None or mean_value is None or std_value is None or std_value <= 0:
                features.append(0.0)
            else:
                features.append((value - mean_value) / std_value)
            continue

        generic_value = _to_float(latest_row.get(feature_name))
        features.append(generic_value if generic_value is not None else 0.0)

    usable_len = min(len(features), len(beta))
    if usable_len == 0:
        return None

    prediction = 0.0
    for idx in range(usable_len):
        beta_value = _to_float(beta[idx])
        if beta_value is None:
            continue
        prediction += features[idx] * beta_value

    return prediction


def detect_kpi_behavior_break(
    retailer_df: Any,
    profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Detect KPI Behavior Break (ME-5402).

    Trigger conditions:
    - residual z-score beyond threshold
    - KPI direction opposite to total media spend direction
    """
    if retailer_df is None or len(retailer_df) < 2:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0,
        }

    schema = profile.get('schema', {}) if profile else {}
    thresholds = profile.get('thresholds', {}) if profile else {}
    residual_profile = profile.get('residual_profile', {}) if profile else {}

    kpi_col = schema.get('kpi_col') if schema.get('kpi_col') in retailer_df.columns else None
    if not kpi_col and 'Sales' in retailer_df.columns:
        kpi_col = 'Sales'
    if not kpi_col:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0,
        }

    latest_row = retailer_df.iloc[-1]
    previous_row = retailer_df.iloc[-2]

    latest_kpi = _to_float(latest_row.get(kpi_col))
    previous_kpi = _to_float(previous_row.get(kpi_col))
    if latest_kpi is None or previous_kpi is None:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0,
        }

    pred_log_kpi = _predict_log_kpi_from_profile(latest_row, profile)
    if pred_log_kpi is None:
        pred_log_kpi = _safe_log1p(previous_kpi)

    actual_log_kpi = _safe_log1p(latest_kpi)
    residual_value = actual_log_kpi - pred_log_kpi
    resid_mean = _to_float(residual_profile.get('resid_mean')) or 0.0
    resid_std = _to_float(residual_profile.get('resid_std')) or 0.0
    resid_z = 0.0
    if resid_std > 0:
        resid_z = (residual_value - resid_mean) / resid_std

    spend_cols = schema.get('spend_cols') or list((profile.get('spend_stats') or {}).keys())
    latest_total_spend = 0.0
    previous_total_spend = 0.0
    for col_name in spend_cols:
        latest_total_spend += _to_float(latest_row.get(col_name)) or 0.0
        previous_total_spend += _to_float(previous_row.get(col_name)) or 0.0

    spend_delta = latest_total_spend - previous_total_spend
    kpi_delta = latest_kpi - previous_kpi
    raw_opposite_direction = (
        abs(spend_delta) > 1e-9 and
        abs(kpi_delta) > 1e-9 and
        (spend_delta * kpi_delta) < 0
    )

    resid_yellow = _to_float(thresholds.get('RESID_Z_YELLOW')) or KPI_BEHAVIOR_RESID_Z_YELLOW_DEFAULT
    resid_red = _to_float(thresholds.get('RESID_Z_RED')) or KPI_BEHAVIOR_RESID_Z_RED_DEFAULT
    opposite_min_resid_z = (
        _to_float(thresholds.get('OPPOSITE_MIN_RESID_Z'))
        or KPI_BEHAVIOR_OPPOSITE_MIN_RESID_Z_DEFAULT
    )
    opposite_min_spend_delta_pct = (
        _to_float(thresholds.get('OPPOSITE_MIN_SPEND_DELTA_PCT'))
        or KPI_BEHAVIOR_OPPOSITE_MIN_SPEND_DELTA_PCT_DEFAULT
    )
    event_transition_suppress_max_resid_z = (
        _to_float(thresholds.get('EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z'))
        or KPI_BEHAVIOR_EVENT_TRANSITION_SUPPRESS_MAX_RESID_Z_DEFAULT
    )

    total_spend_mean = _get_total_spend_mean(profile, spend_cols)
    spend_delta_pct = abs(spend_delta) / total_spend_mean if total_spend_mean > 0 else 0.0
    event_cols = schema.get('event_cols') or []
    event_transition_ended = _is_event_transition_ended(previous_row, latest_row, event_cols)

    residual_trigger = abs(resid_z) >= resid_yellow
    opposite_direction = (
        raw_opposite_direction
        and abs(resid_z) >= opposite_min_resid_z
        and spend_delta_pct >= opposite_min_spend_delta_pct
    )

    if event_transition_ended and abs(resid_z) <= event_transition_suppress_max_resid_z:
        opposite_direction = False

    detected = residual_trigger or opposite_direction

    severity = 'NONE'
    if detected:
        severity = 'RED' if abs(resid_z) >= resid_red else 'YELLOW'

    anomalies = []
    if detected:
        anomalies.append({
            'kpi_column': kpi_col,
            'latest_kpi': round(latest_kpi, 4),
            'previous_kpi': round(previous_kpi, 4),
            'kpi_delta': round(kpi_delta, 4),
            'latest_total_spend': round(latest_total_spend, 4),
            'previous_total_spend': round(previous_total_spend, 4),
            'spend_delta': round(spend_delta, 4),
            'spend_delta_pct': round(spend_delta_pct, 6),
            'residual': round(residual_value, 6),
            'residual_z': round(resid_z, 6),
            'threshold_breaches': {
                'residual_yellow': abs(resid_z) >= resid_yellow,
                'residual_red': abs(resid_z) >= resid_red,
                'raw_opposite_direction': raw_opposite_direction,
                'opposite_direction': opposite_direction,
                'event_transition_ended': event_transition_ended,
            },
            'severity': severity,
        })

    return {
        'detected': detected,
        'severity': severity,
        'anomalies': anomalies,
        'drift_metric_current': round(abs(resid_z), 6),
        'opposite_direction': opposite_direction,
        'event_transition_ended': event_transition_ended,
    }


def send_kpi_behavior_break_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send async Slack alert for KPI Behavior Break (ME-5402)."""
    if not detection_result or not detection_result.get('detected'):
        return True

    correlation_id = f"kpi-break-{uuid.uuid4().hex[:12]}"
    helper = None

    try:
        if PipelineInfoHelper:
            helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)

        anomalies = detection_result.get('anomalies', []) or []
        severity = str(detection_result.get('severity', 'YELLOW')).upper()
        drift_metric_current = detection_result.get('drift_metric_current', 0.0)

        payload = {
            'alert_type': 'KPI_BEHAVIOR_BREAK',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'severity': severity,
            'drift_metric_current': drift_metric_current,
            'anomalies': anomalies,
            'correlation_id': correlation_id,
            'message': 'KPI behavior break detected. Retraining has been marked as required.'
        }

        is_valid, validation_error = _validate_kpi_alert_payload(payload)
        if not is_valid:
            _transfer_warning(
                status_logger,
                f"Skipping KPI behavior break alert due to invalid payload: {validation_error}",
                alert_type="KPI_BEHAVIOR_BREAK",
            )
            return False

        max_retries = max(1, KPI_ALERT_MAX_RETRIES)
        for attempt in range(1, max_retries + 1):
            try:
                response = lambda_client.invoke(
                    FunctionName=SLACK_LAMBDA_NAME,
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
            except Exception as invoke_error:
                if attempt >= max_retries:
                    raise invoke_error

                sleep_seconds = KPI_ALERT_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                _transfer_warning(
                    status_logger,
                    f"KPI behavior break alert invoke failed (attempt {attempt}/{max_retries}, "
                    f"correlation_id={correlation_id}): {invoke_error}. Retrying in {sleep_seconds:.1f}s",
                    alert_type="KPI_BEHAVIOR_BREAK",
                )
                time.sleep(sleep_seconds)
                continue

            if response.get('StatusCode') == 202:
                _transfer_info(
                    status_logger,
                    f"KPI behavior break alert queued for {client_id}/{brand_name}/{retailer_id}: "
                    f"severity={severity}, correlation_id={correlation_id}, attempt={attempt}",
                    alert_type="KPI_BEHAVIOR_BREAK",
                )
                return True

            if attempt < max_retries:
                sleep_seconds = KPI_ALERT_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                _transfer_warning(
                    status_logger,
                    f"KPI behavior break alert returned StatusCode={response.get('StatusCode')} "
                    f"(attempt {attempt}/{max_retries}, correlation_id={correlation_id}). "
                    f"Retrying in {sleep_seconds:.1f}s",
                    alert_type="KPI_BEHAVIOR_BREAK",
                )
                time.sleep(sleep_seconds)
                continue

            _transfer_warning(
                status_logger,
                f"KPI behavior break alert failed after retries: StatusCode={response.get('StatusCode')}, "
                f"correlation_id={correlation_id}",
                alert_type="KPI_BEHAVIOR_BREAK",
            )
            return False

        _transfer_warning(
            status_logger,
            f"KPI behavior break alert exhausted retries without success: "
            f"{client_id}/{brand_name}/{retailer_id}, correlation_id={correlation_id}",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send KPI behavior break alert (correlation_id={correlation_id}): {e}",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
        return False


def handle_kpi_behavior_break_actions(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> None:
    """Execute ME-5402 actions: Slack alert + retraining_required flip + drift metric update."""
    if not detection_result or not detection_result.get('detected'):
        return

    send_kpi_behavior_break_alert(
        client_id=client_id,
        brand_name=brand_name,
        retailer_id=retailer_id,
        filename=filename,
        detection_result=detection_result,
        status_logger=status_logger,
    )

    if not PipelineInfoHelper:
        _transfer_warning(
            status_logger,
            "PipelineInfoHelper unavailable - cannot persist KPI behavior break metrics",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
        return

    try:
        helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)
        drift_metric_current = _to_float(detection_result.get('drift_metric_current')) or 0.0
        helper.update_drift_metrics(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            drift_metric_current=drift_metric_current,
            retraining_required=True
        )
        _transfer_info(
            status_logger,
            f"ME-5402 persisted drift metrics for {client_id}/{brand_name}/{retailer_id}: "
            f"drift_metric_current={drift_metric_current}",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to persist KPI behavior break metrics: {e}",
            alert_type="KPI_BEHAVIOR_BREAK",
        )


def detect_channel_activation_deactivation(
    retailer_df: Any,
    profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Detect Channel Activation/Deactivation (ME-5806a).
    Trigger when:
    - active history < threshold AND latest spend > 0 (activation)
    - active history >= threshold AND latest spend == 0 (deactivation)
    """
    if retailer_df is None or len(retailer_df) == 0:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0
        }

    spend_stats = profile.get('spend_stats', {}) if profile else {}
    thresholds = profile.get('thresholds', {}) if profile else {}
    
    min_active_weeks = int(_to_float(thresholds.get('MIN_ACTIVE_WEEKS_FOR_STABILITY')) or MIN_ACTIVE_WEEKS_DEFAULT)
    
    latest_row = retailer_df.iloc[-1]
    anomalies = []
    
    for channel_name, stats in spend_stats.items():
        if channel_name not in retailer_df.columns:
            continue
            
        current_value = _to_float(latest_row.get(channel_name))
        
        # We assume active_weeks is pre-calculated in the profile, or we approximate 
        # based on N (where N in spend_stats signifies total obs). The EDA says "historically strong".
        # Standard implementation of ME-5401 profile format provides 'count' for non-zeros.
        # If 'active_weeks' is explicitly present, we use it. Otherwise we fallback to 'count'.
        active_weeks = int(_to_float(stats.get('active_weeks', stats.get('count', 0))))
        
        if current_value is not None and current_value > 1e-9:
            # Channel is active this week
            if active_weeks < min_active_weeks:
                anomalies.append({
                    'channel': channel_name,
                    'type': 'ACTIVATION',
                    'current_value': round(current_value, 4),
                    'active_weeks': active_weeks,
                    'threshold': min_active_weeks,
                    'severity': 'RED'
                })
        elif current_value is not None and current_value <= 1e-9:
            # Channel is inactive this week
            if active_weeks >= min_active_weeks:
                # Based on user direction, we stick to single-row triggering for Deactivation.
                anomalies.append({
                    'channel': channel_name,
                    'type': 'DEACTIVATION',
                    'current_value': 0.0,
                    'active_weeks': active_weeks,
                    'threshold': min_active_weeks,
                    'severity': 'RED'
                })
                
    detected = len(anomalies) > 0
    return {
        'detected': detected,
        'severity': 'RED' if detected else 'NONE',
        'anomalies': anomalies,
        'drift_metric_current': float(len(anomalies))
    }


def send_channel_activation_deactivation_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send async Slack alert for Channel Activation/Deactivation (ME-5806a)."""
    if not detection_result or not detection_result.get('detected'):
        return True
    try:
        payload = {
            'alert_type': 'CHANNEL_ACTIVATION_DEACTIVATION',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'severity': detection_result.get('severity', 'RED'),
            'drift_metric_current': detection_result.get('drift_metric_current', 0.0),
            'anomalies': detection_result.get('anomalies', []),
            'message': 'Channel activation/deactivation detected. Full retrain required.'
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Channel activation/deactivation alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="CHANNEL_ACTIVATION_DEACTIVATION",
            )
            return True
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send channel activation/deactivation alert: {e}",
            alert_type="CHANNEL_ACTIVATION_DEACTIVATION",
        )
        return False


def handle_channel_activation_deactivation_actions(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    detection_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> None:
    """Execute ME-5806a actions: Slack alert + retraining_required flip + drift metric update."""
    if not detection_result or not detection_result.get('detected'):
        return
    send_channel_activation_deactivation_alert(
        client_id=client_id,
        brand_name=brand_name,
        retailer_id=retailer_id,
        filename=filename,
        detection_result=detection_result,
        status_logger=status_logger,
    )
    if not PipelineInfoHelper:
        return
    try:
        helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)
        helper.update_drift_metrics(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            drift_metric_current=_to_float(detection_result.get('drift_metric_current')) or 0.0,
            retraining_required=True
        )
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to persist channel activation/deactivation metrics: {e}",
            alert_type="CHANNEL_ACTIVATION_DEACTIVATION",
        )


def _jensen_shannon_divergence(p: List[float], q: List[float], eps: float = 1e-12) -> float:
    """Compute Jensen-Shannon divergence between two distributions."""
    if not PANDAS_AVAILABLE:
        return 0.0
    import numpy as np
    
    p = np.asarray(p, dtype=float)
    q = np.asarray(q, dtype=float)
    
    p_sum, q_sum = p.sum(), q.sum()
    if p_sum <= 0 or q_sum <= 0:
        return 0.0
        
    p = p / p_sum
    q = q / q_sum
    m = 0.5 * (p + q)
    
    kl_p = np.sum(p * np.log((p + eps) / (m + eps)))
    kl_q = np.sum(q * np.log((q + eps) / (m + eps)))
    
    return float(0.5 * kl_p + 0.5 * kl_q)


def detect_spend_mix_reallocation(
    retailer_df: Any,
    profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Detect Spend Mix Reallocation (ME-5806b).
    Trigger when JS divergence or max absolute share delta exceed thresholds.
    """
    if retailer_df is None or len(retailer_df) == 0:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0
        }

    mix_profile = profile.get('mix_profile', {}) if profile else {}
    avg_share = mix_profile.get('avg_share', {})
    spend_cols = mix_profile.get('spend_cols', list(avg_share.keys()))
    
    if not avg_share or not spend_cols:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0
        }

    thresholds = profile.get('thresholds', {}) if profile else {}
    mix_shift_yellow = _to_float(thresholds.get('MIX_SHIFT_YELLOW')) or MIX_SHIFT_YELLOW_DEFAULT
    mix_shift_red = _to_float(thresholds.get('MIX_SHIFT_RED')) or MIX_SHIFT_RED_DEFAULT
    js_yellow = _to_float(thresholds.get('JS_YELLOW')) or JS_YELLOW_DEFAULT
    js_red = _to_float(thresholds.get('JS_RED')) or JS_RED_DEFAULT
    
    latest_row = retailer_df.iloc[-1]
    
    current_spend_vec = []
    baseline_share_vec = []
    total_spend = 0.0
    
    for channel in spend_cols:
        if channel in retailer_df.columns:
            spend_val = _to_float(latest_row.get(channel)) or 0.0
        else:
            spend_val = 0.0
        current_spend_vec.append(spend_val)
        total_spend += spend_val
        baseline_share_vec.append(_to_float(avg_share.get(channel, 0.0)))
        
    if total_spend <= 1e-9:
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0
        }
        
    current_share_vec = [v / total_spend for v in current_spend_vec]
    js_div = _jensen_shannon_divergence(current_share_vec, baseline_share_vec)
    
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
        
    yellow_trigger = max_delta >= mix_shift_yellow or js_div >= js_yellow
    red_trigger = max_delta >= mix_shift_red or js_div >= js_red
    
    if not (yellow_trigger or red_trigger):
        return {
            'detected': False,
            'severity': 'NONE',
            'anomalies': [],
            'drift_metric_current': 0.0
        }
        
    severity = 'RED' if red_trigger else 'YELLOW'
    
    anomalies = [{
        'total_spend': round(total_spend, 4),
        'max_share_delta': round(max_delta, 4),
        'js_divergence': round(js_div, 4),
        'channel_deltas': channel_deltas,
        'threshold_breaches': {
            'max_delta_yellow': max_delta >= mix_shift_yellow,
            'max_delta_red': max_delta >= mix_shift_red,
            'js_yellow': js_div >= js_yellow,
            'js_red': js_div >= js_red,
        },
        'severity': severity
    }]
    
    # We take the max of JS / JS_threshold vs Delta / Delta_threshold to report normalized highest pressure
    normalized_js = js_div / js_yellow if js_yellow > 0 else 0
    normalized_delta = max_delta / mix_shift_yellow if mix_shift_yellow > 0 else 0
    drift_metric_current = float(max(normalized_js, normalized_delta))
    
    return {
        'detected': True,
        'severity': severity,
        'anomalies': anomalies,
        'drift_metric_current': round(drift_metric_current, 6)
    }


def send_schema_drift_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    missing_columns: List[str],
    extra_columns: List[str],
    filename: str,
    missing_in_source: List[str] = None,
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Send a Slack alert when schema drift is detected (source differs from SOT).

    Invokes the Slack notification Lambda asynchronously to notify the team
    and ask if the schema difference is expected.

    Args:
        client_id: Client identifier
        brand_name: Brand name
        retailer_id: Retailer identifier
        missing_columns: SOT columns missing from source (same as missing_in_source when provided)
        extra_columns: Columns in output not allowed by SOT / suffix rules
        filename: Source filename
        missing_in_source: SOT columns never present in source (optional; defaults to missing_columns)

    Returns:
        True if alert was sent successfully, False otherwise
    """
    if not missing_columns and not extra_columns:
        return True  # No drift to report

    if missing_in_source is None:
        missing_in_source = list(missing_columns)

    try:
        payload = {
            'alert_type': 'SCHEMA_DRIFT',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'missing_columns': missing_columns,
            'missing_in_source': missing_in_source,
            'extra_columns': extra_columns,
            'filename': filename
        }
        
        # Invoke Slack Lambda asynchronously (don't wait for response)
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload)
        )
        
        if response.get('StatusCode') == 202:  # 202 = Accepted for async
            _transfer_info(
                status_logger,
                f"Schema drift alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="SCHEMA_DRIFT",
            )
            return True
        else:
            _transfer_warning(
                status_logger,
                f"Schema drift alert may have failed: StatusCode={response.get('StatusCode')}",
                alert_type="SCHEMA_DRIFT",
            )
            return False
            
    except Exception as e:
        # Don't fail the pipeline for alert failures
        _transfer_warning(
            status_logger,
            f"Failed to send schema drift alert: {e}",
            alert_type="SCHEMA_DRIFT",
        )
        return False


def send_column_drop_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    dropped_columns: List[Dict[str, str]],
    total_source_columns: int,
    total_output_columns: int,
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Send Slack alert when columns are dropped during processing.
    
    STEP 9: Provides visibility into data loss by alerting on dropped columns with reasons.
    
    Args:
        client_id: Client identifier
        brand_name: Brand name
        retailer_id: Retailer identifier
        filename: Source filename
        dropped_columns: List of dicts with 'column' and 'reason' keys
        total_source_columns: Total columns in source file
        total_output_columns: Total columns in output file
    
    Returns:
        True if alert sent successfully, False otherwise
    """
    if not dropped_columns:
        return True  # No drops to report
    
    try:
        payload = {
            'alert_type': 'COLUMN_DROP',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'dropped_columns': dropped_columns,
            'total_source_columns': total_source_columns,
            'total_output_columns': total_output_columns,
            'columns_dropped_count': len(dropped_columns)
        }
        
        # Invoke Slack Lambda asynchronously (don't wait for response)
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload)
        )
        
        if response.get('StatusCode') == 202:  # 202 = Accepted for async
            _transfer_info(
                status_logger,
                f"Column drop alert sent for {client_id}/{brand_name}/{retailer_id}: {len(dropped_columns)} columns dropped",
                alert_type="COLUMN_DROP",
            )
            return True
        else:
            _transfer_warning(
                status_logger,
                f"Column drop alert may have failed: StatusCode={response.get('StatusCode')}",
                alert_type="COLUMN_DROP",
            )
            return False
            
    except Exception as e:
        # Don't fail the pipeline for alert failures
        _transfer_warning(
            status_logger,
            f"Failed to send column drop alert: {e}",
            alert_type="COLUMN_DROP",
        )
        return False


def send_invalid_sales_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    invalid_columns: List[Dict[str, Any]],
    total_rows: int,
    invalid_row_count: int,
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Send Slack alert when invalid/missing sales data is detected.
    Invokes the Slack notification Lambda asynchronously.
    """
    if not invalid_columns:
        return True
    try:
        def convert_to_native_types(obj):
            if isinstance(obj, dict):
                return {k: convert_to_native_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native_types(item) for item in obj]
            elif hasattr(obj, 'item'):
                return obj.item()
            elif isinstance(obj, (int, float, str, bool)) or obj is None:
                return obj
            else:
                try:
                    if hasattr(obj, 'dtype'):
                        if 'int' in str(obj.dtype):
                            return int(obj)
                        elif 'float' in str(obj.dtype):
                            return float(obj)
                    return int(obj)
                except (ValueError, TypeError, AttributeError):
                    try:
                        return str(obj)
                    except Exception:
                        return repr(obj)
        serializable_invalid_columns = convert_to_native_types(invalid_columns)
        payload = {
            'alert_type': 'INVALID_SALES_DATA',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'invalid_columns': serializable_invalid_columns,
            'total_rows': int(total_rows),
            'invalid_row_count': int(invalid_row_count),
            'message': f'File ingestion skipped: Invalid or missing sales data detected. {int(invalid_row_count)} row(s) with null/NaN/empty sales values in {len(invalid_columns)} column(s).'
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Invalid sales data alert sent for {client_id}/{brand_name}/{retailer_id}: {filename}",
                alert_type="INVALID_SALES_DATA",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Invalid sales data alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="INVALID_SALES_DATA",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send invalid sales data alert: {e}",
            alert_type="INVALID_SALES_DATA",
        )
        return False


def _get_date_column_name(df: Any) -> Optional[str]:
    """Return the name of the Date column in df (normalized 'date'), or None if not found."""
    for col in df.columns:
        if normalize_header_for_matching(str(col)) == 'date':
            return col
    return None


def detect_duplicate_dates(df: Any, status_logger: Optional[Any] = None) -> Dict[str, Any]:
    """
    Detect duplicate dates in the Date column. Same date appearing multiple times corrupts training.
    Returns {'has_duplicates': True, 'duplicate_count', 'duplicates', 'action': 'BLOCK'} or {'has_duplicates': False}.
    """
    date_col = _get_date_column_name(df)
    if not date_col or date_col not in df.columns:
        return {'has_duplicates': False}
    try:
        series = df[date_col].astype(str).str.strip()
        parsed = pd.to_datetime(series, errors='coerce')
        normalized = parsed.dt.strftime('%Y-%m-%d').fillna(series)
        # Only check valid (non-empty) dates for duplicates; ignore empty rows
        valid_mask = (normalized != '') & (normalized != 'nan')
        valid_dates = normalized[valid_mask]
        if len(valid_dates) == 0:
            return {'has_duplicates': False}
        counts = valid_dates.value_counts()
        dupes = counts[counts > 1]
        if len(dupes) == 0:
            return {'has_duplicates': False}
        duplicates = []
        for date_val, count in dupes.items():
            indices = valid_dates[valid_dates == date_val].index.tolist()
            duplicates.append({
                'date': str(date_val),
                'occurrences': int(count),
                'row_indices': indices,
            })
        return {
            'has_duplicates': True,
            'duplicate_count': len(duplicates),
            'duplicates': duplicates,
            'action': 'BLOCK',
        }
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Duplicate date detection failed: {e}",
            debug_event="duplicate_date_detection",
        )
        return {'has_duplicates': False}


def detect_date_gaps(df: Any, status_logger: Optional[Any] = None) -> Dict[str, Any]:
    """
    Detect gaps (missing weeks) in the Date column. Expects weekly data; gaps break time series.
    Returns {'has_gaps': True, 'gap_count', 'gaps', 'action': 'BLOCK'} or {'has_gaps': False}.
    Only runs gap logic when every row has a valid date; if any row has empty/invalid date,
    returns no gaps to avoid false positives from dropped rows (see RCA_BESTBUY_DATE_GAPS_FALSE_POSITIVE).
    """ 
    date_col = _get_date_column_name(df)
    if not date_col or date_col not in df.columns:
        return {'has_gaps': False}
    try:
        parsed = pd.to_datetime(df[date_col], errors='coerce')
        # Skip gap check when any row has missing/empty date to avoid false gaps
        if parsed.isna().any():
            return {'has_gaps': False}
        valid = parsed.dropna()
        if len(valid) < 2:
            return {'has_gaps': False}
        sorted_dates = valid.sort_values().unique()
        gaps = []
        for i in range(1, len(sorted_dates)):
            prev = pd.Timestamp(sorted_dates[i - 1])
            actual = pd.Timestamp(sorted_dates[i])
            expected_next = prev + timedelta(days=7)
            if actual != expected_next:
                missing_weeks = (actual - prev).days // 7 - 1
                if missing_weeks > 0:
                    gaps.append({
                        'after_date': prev.strftime('%Y-%m-%d'),
                        'before_date': actual.strftime('%Y-%m-%d'),
                        'missing_weeks': int(missing_weeks),
                    })
        if not gaps:
            return {'has_gaps': False}
        return {
            'has_gaps': True,
            'gap_count': len(gaps),
            'gaps': gaps,
            'action': 'BLOCK',
        }
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Date gap detection failed: {e}",
            debug_event="date_gap_detection",
        )
        return {'has_gaps': False}


def send_duplicate_dates_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    duplicate_details: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send Slack alert when duplicate dates are detected. Invokes Slack Lambda asynchronously."""
    try:
        payload = {
            'alert_type': 'DUPLICATE_DATES',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'duplicates': duplicate_details.get('duplicates', []),
            'duplicate_count': duplicate_details.get('duplicate_count', 0),
            'message': f"File ingestion blocked: Duplicate dates detected. {duplicate_details.get('duplicate_count', 0)} date(s) appear more than once.",
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload, default=str),
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Duplicate dates alert sent for {client_id}/{brand_name}/{retailer_id}: {filename}",
                alert_type="DUPLICATE_DATES",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Duplicate dates alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="DUPLICATE_DATES",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send duplicate dates alert: {e}",
            alert_type="DUPLICATE_DATES",
        )
        return False


def send_date_gaps_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    gap_details: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send Slack alert when date gaps (missing weeks) are detected. Invokes Slack Lambda asynchronously."""
    try:
        payload = {
            'alert_type': 'DATE_GAPS',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'gaps': gap_details.get('gaps', []),
            'gap_count': gap_details.get('gap_count', 0),
            'message': f"File ingestion blocked: Date gaps (missing weeks) detected. {gap_details.get('gap_count', 0)} gap(s) found.",
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload, default=str),
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Date gaps alert sent for {client_id}/{brand_name}/{retailer_id}: {filename}",
                alert_type="DATE_GAPS",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Date gaps alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="DATE_GAPS",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send date gaps alert: {e}",
            alert_type="DATE_GAPS",
        )
        return False


ROW_COUNT_DROP_YELLOW_PCT = 50.0
ROW_COUNT_DROP_RED_PCT = 80.0
ROW_COUNT_MIN_HISTORICAL = 10


def validate_row_count(
    df: Any,
    helper: Any,
    client_id: str,
    brand_name: str,
    retailer_id: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Validate row count vs historical average. Sudden large drop suggests truncation/corruption.
    Returns valid=True/False, severity NONE/YELLOW/RED, action BLOCK/WARN, and metrics.

    Lookup uses pipeline_info (table mmm-{env}-pipeline-infos) with key:
    partition_key=client_id, sort_key=build_sort_key(brand_name, retailer_id) e.g. {normalized_brand}#{retailer_id}.
    Ensure your pipeline_infos record uses the same key (same client_id and brand_retailer_key format).
    """
    current_rows = len(df) if df is not None else 0
    if helper is None:
        _transfer_info(
            status_logger,
            "Row count check skipped: PipelineInfoHelper not available",
            debug_event="row_count_check",
        )
        return {'valid': True, 'reason': 'insufficient_history'}
    try:
        # Log lookup key so it can be verified against pipeline_infos (client_id + brand_retailer_key)
        try:
            sort_key = type(helper).build_sort_key(brand_name, retailer_id)
            _transfer_info(
                status_logger,
                f"Row count lookup: pipeline_info key client_id={client_id!r}, "
                f"brand_retailer_key={sort_key!r} (from brand_name={brand_name!r}, retailer_id={retailer_id!r})",
                debug_event="row_count_check",
            )
        except Exception as e:
            _transfer_warning(
                status_logger,
                f"Row count lookup: could not build sort_key for brand_name={brand_name!r}, retailer_id={retailer_id!r}: {e}",
                debug_event="row_count_check",
            )
        info = helper.get_pipeline_info(client_id, brand_name, retailer_id) or {}
        historical_raw = info.get('last_transfer_row_count')
        historical_avg = _to_float(historical_raw) if historical_raw is not None else None
        if historical_avg is None or historical_avg < ROW_COUNT_MIN_HISTORICAL:
            _transfer_info(
                status_logger,
                f"Row count: no usable history (last_transfer_row_count={historical_raw!r}, "
                f"min_required={ROW_COUNT_MIN_HISTORICAL}). Passing without alert.",
                debug_event="row_count_check",
            )
            return {'valid': True, 'reason': 'insufficient_history'}
        drop_percentage = (historical_avg - current_rows) / historical_avg * 100.0
        if drop_percentage >= ROW_COUNT_DROP_RED_PCT:
            return {
                'valid': False,
                'severity': 'RED',
                'current_rows': current_rows,
                'historical_avg': int(historical_avg),
                'drop_percentage': round(drop_percentage, 2),
                'action': 'BLOCK',
            }
        if drop_percentage >= ROW_COUNT_DROP_YELLOW_PCT:
            return {
                'valid': True,
                'severity': 'YELLOW',
                'current_rows': current_rows,
                'historical_avg': int(historical_avg),
                'drop_percentage': round(drop_percentage, 2),
                'action': 'WARN',
            }
        _transfer_info(
            status_logger,
            f"Row count: drop {round(drop_percentage, 1)}% below YELLOW threshold ({ROW_COUNT_DROP_YELLOW_PCT}%). "
            f"Current={current_rows}, historical={int(historical_avg)}. No alert.",
            debug_event="row_count_check",
        )
        return {'valid': True, 'severity': 'NONE'}
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Row count validation failed: {e}",
            debug_event="row_count_check",
        )
        return {'valid': True, 'reason': 'validation_error'}


def send_row_count_drop_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    validation_result: Dict[str, Any],
    status_logger: Optional[Any] = None,
) -> bool:
    """Send Slack alert for row count drop (RED block or YELLOW warn)."""
    try:
        payload = {
            'alert_type': 'ROW_COUNT_DROP',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'filename': filename,
            'severity': validation_result.get('severity', 'NONE'),
            'current_rows': validation_result.get('current_rows', 0),
            'historical_avg': validation_result.get('historical_avg', 0),
            'drop_percentage': validation_result.get('drop_percentage', 0),
            'message': (
                f"Row count drop {validation_result.get('severity', '')}: "
                f"current={validation_result.get('current_rows', 0)} vs historical avg={validation_result.get('historical_avg', 0)} "
                f"({validation_result.get('drop_percentage', 0)}% drop)."
            ),
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload, default=str),
        )
        if response.get('StatusCode') == 202:
            _transfer_info(
                status_logger,
                f"Row count drop alert sent for {client_id}/{brand_name}/{retailer_id}: {filename}",
                alert_type="ROW_COUNT_DROP",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Row count drop alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="ROW_COUNT_DROP",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send row count drop alert: {e}",
            alert_type="ROW_COUNT_DROP",
        )
        return False


def normalize_brand_name(brand_name: str) -> str:
    """
    Normalize brand name for consistent use in S3 paths and DynamoDB keys.
    
    Converts hyphens to underscores and lowercases: 'bella-US' -> 'bella_us'
    This matches Tracer bucket filename conventions and ensures consistency
    across DynamoDB operations and S3 path creation.
    
    Args:
        brand_name: Brand name (e.g., 'bella-US', 'cleaning-CA')
    
    Returns:
        Normalized brand name (e.g., 'bella_us', 'cleaning_ca')
    
    Example:
        >>> normalize_brand_name('bella-US')
        'bella_us'
        >>> normalize_brand_name('cleaning-CA')
        'cleaning_ca'
        >>> normalize_brand_name('bella_us')
        'bella_us'
    """
    if not brand_name:
        return brand_name
    return brand_name.lower().replace('-', '_')


def is_valid_client_file(
    filename: str,
    client_id: str,
    brand_id: str,
    retailer_id: str,
    expected_suffix: str = None,
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Token-based matching - validates file contains required identifiers.
    
    This approach is robust against:
    - Date prefixes: 2025-01-...
    - Different separators: _ vs -
    - Case variations: bella-US vs bella_us
    - Extra tokens in filename
    - Order variations (within reason)
    
    The file is valid if it:
    1. Contains the client_id
    2. Contains the brand_id (normalized)
    3. Contains the retailer_id
    4. Has .csv extension
    5. Optionally matches expected_suffix
    
    Args:
        filename: Filename to check
        client_id: Client identifier (e.g., 'madebygather')
        brand_id: Brand identifier (e.g., 'bella-US' or 'bella_us') - used for file matching
        retailer_id: Retailer identifier (e.g., 'amazon')
        expected_suffix: Optional expected number suffix (e.g., '331') for exact match
    
    Returns:
        True if filename contains all required tokens, False otherwise
    """
    # Step 1: Basic validation
    if not filename:
        _transfer_debug(
            status_logger,
            "file_validation",
            "is_valid_client_file: Empty filename",
            filename="",
        )
        return False

    # Step 2: Extension validation
    if not filename.lower().endswith('.csv'):
        _transfer_debug(
            status_logger,
            "file_validation",
            f"is_valid_client_file: Invalid extension for '{filename}'",
            filename=filename,
        )
        return False
    
    # Step 3: Normalize filename
    name_lower = filename.lower()
    name_without_ext = name_lower[:-4]  # Remove .csv
    
    # Step 4: Normalize identifiers
    client_lower = client_id.lower()
    # Normalize brand: bella-US -> bella_us (Tracer uses underscores in filenames)
    brand_normalized = brand_id.lower().replace('-', '_')
    retailer_lower = retailer_id.lower()
    
    # Step 5: Contains-based validation
    if client_lower not in name_lower:
        _transfer_debug(
            status_logger,
            "file_validation",
            f"is_valid_client_file: Client '{client_id}' not found in '{filename}'",
            filename=filename,
            client_id=client_id,
        )
        return False

    if retailer_lower not in name_lower:
        _transfer_debug(
            status_logger,
            "file_validation",
            f"is_valid_client_file: Retailer '{retailer_id}' not found in '{filename}'",
            filename=filename,
            retailer_id=retailer_id,
        )
        return False

    # Step 6: Brand matching (with normalization)
    # Normalize filename for brand comparison (handle both - and _)
    name_normalized = name_without_ext.replace('-', '_')
    if brand_normalized not in name_normalized:
        _transfer_debug(
            status_logger,
            "file_validation",
            f"is_valid_client_file: Brand '{brand_id}' (normalized: '{brand_normalized}') not found in '{filename}'",
            filename=filename,
            brand_id=brand_id,
            brand_normalized=brand_normalized,
        )
        return False

    # Step 7: Optional suffix validation
    if expected_suffix:
        suffix_lower = str(expected_suffix).lower()
        valid_endings = [f"_{suffix_lower}.csv", f"-{suffix_lower}.csv"]
        if not any(name_lower.endswith(ending) for ending in valid_endings):
            _transfer_debug(
                status_logger,
                "file_validation",
                f"is_valid_client_file: Expected suffix '{expected_suffix}' not found in '{filename}'",
                filename=filename,
                expected_suffix=str(expected_suffix),
            )
            return False

    _transfer_debug(
        status_logger,
        "file_validation",
        f"is_valid_client_file: ✓ Valid file '{filename}' (client={client_id}, brand={brand_id}, retailer={retailer_id})",
        filename=filename,
        client_id=client_id,
        brand_id=brand_id,
        retailer_id=retailer_id,
    )
    return True


def extract_number_suffix(key: str) -> int:
    """
    Extract number suffix from filename for sorting (e.g., '331' from 'madebygather_bella_us_amazon_331.csv').
    Returns 0 if no number found.
    """
    filename = key.split('/')[-1]
    name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
    parts = name_without_ext.split('_')
    if parts:
        last_part = parts[-1]
        if last_part.isdigit():
            return int(last_part)
    return 0


def find_tracer_files(
    client_id: str,
    brand_id: str,
    retailer_id: str,
    expected_filename: str = None,
    min_last_modified: Optional[datetime] = None,
    status_logger: Optional[Any] = None,
) -> List[str]:
    """
    Token-based file discovery - matches files containing required identifiers and selects most recent.
    
    Uses S3 LastModified timestamp (not filename suffix) to select most recent file
    when multiple files match. This handles date-prefixed files correctly.
    
    Can optionally filter out files older than last_data_updated to prevent reprocessing.
    
    Args:
        client_id: Client identifier
        brand_id: Brand identifier (should be normalized) - used for file matching
        retailer_id: Retailer identifier
        expected_filename: Optional exact filename to match (e.g., 'madebygather_bella_us_amazon_331.csv')
        min_last_modified: Optional minimum LastModified date (filters out older files)
    
    Returns:
        List of matching S3 keys (typically 1 file, or empty if none found)
    """
    # If exact filename provided, match it directly
    if expected_filename:
        key = f"{SOURCE_PREFIX}/{expected_filename}"
        try:
            s3_client.head_object(Bucket=TRACER_BUCKET, Key=key)
            _transfer_info(
                status_logger,
                f"✓ Found exact file: {key}",
                file_key=key,
                data_source_bucket=TRACER_BUCKET,
            )
            return [key]
        except ClientError:
            _transfer_warning(
                status_logger,
                f"Expected file not found: {key}",
                file_key=key,
                data_source_bucket=TRACER_BUCKET,
            )
            return []
    
    # List of tuples: (key, last_modified, size)
    matching_files = []
    
    # Scan flat tracer/ folder (no YYYY/MM subfolders)
    # Structure: s3://prd-mm-vendor-sync/tracer/{filename}.csv
    prefix = f"{SOURCE_PREFIX}/"
    
    try:
        _transfer_info(
            status_logger,
            f"Scanning: s3://{TRACER_BUCKET}/{prefix} for {client_id}/{brand_id}/{retailer_id}",
            data_source_bucket=TRACER_BUCKET,
            prefix=prefix,
            client_id=client_id,
            brand_id=brand_id,
            retailer_id=retailer_id,
        )
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=TRACER_BUCKET, Prefix=prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                size = obj.get('Size', 0)
                last_modified = obj.get('LastModified')  # S3 timestamp
                filename = key.split('/')[-1]
                
                # Layer 1: Empty file check (reject files with zero size only)
                if size == 0:
                    _transfer_debug(
                        status_logger,
                        "file_discovery",
                        f"Skipping empty file: {filename} (0 bytes)",
                        filename=filename,
                        size_bytes=0,
                    )
                    continue

                # Layer 1.5: Filter by last_data_updated correlation (if provided)
                if min_last_modified and last_modified:
                    if last_modified.date() <= min_last_modified.date():
                        _transfer_debug(
                            status_logger,
                            "file_discovery",
                            f"Skipping file older than last processed: {filename} "
                            f"(modified: {last_modified.date()}, "
                            f"last_processed: {min_last_modified.date()})",
                            filename=filename,
                            file_modified=str(last_modified.date()),
                            last_processed=str(min_last_modified.date()),
                        )
                        continue

                # Layer 2: Extension check
                if not any(filename.lower().endswith(ext) for ext in VALID_EXTENSIONS):
                    _transfer_debug(
                        status_logger,
                        "file_discovery",
                        f"Skipping invalid extension: {filename}",
                        filename=filename,
                    )
                    continue

                # Layer 3: Exclude patterns (test, backup, etc.)
                filename_lower = filename.lower()
                if any(pattern in filename_lower for pattern in EXCLUDE_PATTERNS):
                    _transfer_debug(
                        status_logger,
                        "file_discovery",
                        f"Excluded (pattern match): {filename}",
                        filename=filename,
                    )
                    continue

                # Layer 4: Token-based pattern matching
                if is_valid_client_file(
                    filename, client_id, brand_id, retailer_id, status_logger=status_logger
                ):
                    matching_files.append((key, last_modified, size))
                    _transfer_debug(
                        status_logger,
                        "file_discovery",
                        f"✓ Pattern matched: {key} ({size} bytes, modified: {last_modified})",
                        file_key=key,
                        filename=filename,
                        size_bytes=size,
                        last_modified=str(last_modified) if last_modified else "",
                    )
                    
    except ClientError as e:
        _transfer_warning(
            status_logger,
            f"Error scanning {prefix}: {e}",
            prefix=prefix,
            error=str(e),
        )

    # Select most recent file by LastModified timestamp
    if len(matching_files) > 1:
        # Sort by last_modified DESC, then size DESC (larger = more complete)
        # Handle None last_modified by treating as oldest (shouldn't happen with S3, but safe)
        def sort_key(item):
            key, last_modified, size = item
            if last_modified is None:
                return (datetime(1970, 1, 1, tzinfo=timezone.utc), size)
            return (last_modified, size)
        
        matching_files.sort(key=sort_key, reverse=True)
        selected = matching_files[0][0]

        _transfer_info(
            status_logger,
            f"Selected most recent of {len(matching_files)} matching tracer files for {client_id}/{brand_id}/{retailer_id}",
            force=True,
            candidate_count=len(matching_files),
            selected_file_key=selected,
            client_id=client_id,
            brand_id=brand_id,
            retailer_id=retailer_id,
        )
        for idx, (key, modified, size) in enumerate(matching_files[:5], 1):
            marker = "→ SELECTED" if key == selected else ""
            filename_display = key.split('/')[-1]
            modified_str = modified.strftime('%Y-%m-%d %H:%M:%S UTC') if modified else 'N/A'
            _transfer_info(
                status_logger,
                f"  {idx}. {filename_display} | Modified: {modified_str} | Size: {size:,} bytes {marker}",
                file_key=key,
                candidate_index=idx,
                client_id=client_id,
                brand_id=brand_id,
                retailer_id=retailer_id,
            )

        return [selected]
    elif len(matching_files) == 1:
        key, modified, size = matching_files[0]
        modified_str = modified.strftime('%Y-%m-%d %H:%M:%S UTC') if modified else 'N/A'
        _transfer_info(
            status_logger,
            f"Found exact match: {key} (modified: {modified_str}, size: {size:,} bytes)",
            file_key=key,
            size_bytes=size,
            last_modified=modified_str,
            client_id=client_id,
            brand_id=brand_id,
            retailer_id=retailer_id,
        )
        return [key]
    else:
        _transfer_warning(
            status_logger,
            f"No matching files found for {client_id}/{brand_id}/{retailer_id}",
            client_id=client_id,
            brand_id=brand_id,
            retailer_id=retailer_id,
        )
        return []


def validate_file_freshness(
    file_last_modified: datetime,
    last_data_updated: Optional[str],
    client_id: str,
    brand_name: str,
    retailer_id: str,
    status_logger: Optional[Any] = None,
) -> Tuple[bool, str]:
    """
    Validate that source file is newer than last processed data.
    
    Args:
        file_last_modified: S3 LastModified timestamp of the file
        last_data_updated: Last data update date from pipeline_info (YYYY-MM-DD format)
        client_id: Client identifier (for logging)
        brand_name: Brand name (for logging, should be normalized)
        retailer_id: Retailer identifier (for logging)
    
    Returns:
        Tuple of (is_valid, reason)
        - is_valid: True if file should be processed, False if it's stale
        - reason: Human-readable explanation
    """
    if not last_data_updated:
        return True, "No previous data update recorded"
    
    try:
        last_updated_date = datetime.strptime(last_data_updated, '%Y-%m-%d').date()
        file_date = file_last_modified.date()
        
        if file_date <= last_updated_date:
            return False, (
                f"Source file ({file_date}) is not newer than "
                f"last processed ({last_updated_date})"
            )
        
        return True, (
            f"Source file ({file_date}) is newer than "
            f"last processed ({last_updated_date})"
        )
    except ValueError as e:
        _transfer_warning(
            status_logger,
            f"Invalid last_data_updated format for {client_id}/{brand_name}#{retailer_id}: "
            f"'{last_data_updated}' - {e}",
            debug_event="file_freshness",
        )
        return True, "Could not validate (invalid date format)"


def download_from_tracer(file_key: str, status_logger: Optional[Any] = None) -> bytes:
    """Download file from Tracer bucket."""
    try:
        _transfer_debug(
            status_logger,
            "s3_transfer",
            f"Downloading: {file_key}",
            file_key=file_key,
            sdk_operation="get_object",
        )
        response = s3_client.get_object(Bucket=TRACER_BUCKET, Key=file_key)
        data = response['Body'].read()
        _transfer_debug(
            status_logger,
            "s3_transfer",
            f"✓ Downloaded {len(data)} bytes",
            file_key=file_key,
            size_bytes=len(data),
            sdk_operation="get_object",
        )
        return data
    except ClientError as e:
        _transfer_error(
            status_logger,
            f"✗ Error downloading: {str(e)}",
            reason=str(e),
            file_key=file_key,
            data_source_bucket=TRACER_BUCKET,
        )
        raise


def validate_file_content(
    file_data: bytes,
    filename: str,
    retailer_id: str = "",
    status_logger: Optional[Any] = None,
) -> Tuple[bool, List[str]]:
    """
    Validate file has expected structure and required columns.
    Prevents processing corrupted or wrong file types.
    
    Args:
        file_data: File content as bytes
        filename: Name of the file
        retailer_id: Retailer ID for context (optional)
    
    Returns:
        Tuple of (is_valid: bool, missing_columns: List[str])
    """
    try:
        # Check for empty file (only reject 0-byte files, not small files)
        if len(file_data) == 0:
            return False, ["File is empty (0 bytes)"]
        
        # For CSV: check has header row with commas and data rows
        if filename.lower().endswith('.csv'):
            # Decode file content to check structure
            try:
                file_content = file_data.decode('utf-8', errors='ignore')
            except Exception:
                # Try with different encoding
                file_content = file_data.decode('latin-1', errors='ignore')
            
            if not file_content or not file_content.strip():
                return False, ["File contains no data (empty or whitespace only)"]
            
            # Split into lines
            lines = [line.strip() for line in file_content.split('\n') if line.strip()]
            
            if len(lines) == 0:
                return False, ["File contains no data rows"]
            
            # Check header row
            if len(lines) < 1:
                return False, ["Invalid CSV structure (no header)"]
            
            first_line = lines[0]
            if ',' not in first_line:
                return False, ["Invalid CSV structure (no comma in header)"]
            
            # Check for data rows (excluding header)
            if len(lines) < 2:
                return False, [f"File contains only header row, no data rows (minimum {MIN_DATA_ROWS} data row required)"]
            
            # Check required columns if standardization is available
            if STANDARDIZATION_AVAILABLE:
                # Strip whitespace AND quotes (CSV files often have quoted column names)
                columns = [col.strip().strip('"').strip("'") for col in first_line.split(',')]
                is_valid, missing = validate_required_columns(columns)
                if not is_valid:
                    return False, missing
        
        return True, []
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Content validation failed for {filename}: {e}",
            filename=filename,
            debug_event="content_validation",
        )
        return False, [f"Validation error: {str(e)}"]


def transform_data(
    raw_data: bytes,
    file_key: str,
    retailer_id: str,
    status_logger: Optional[Any] = None,
) -> Tuple[bytes, Dict[str, Any]]:
    """
    Transform raw data to standardized format.
    Applies Data Standardization Contract rules.
    
    Args:
        raw_data: Raw file data as bytes
        file_key: S3 key of the source file
        retailer_id: Retailer identifier for channel mapping
    
    Returns:
        Tuple of (transformed_data: bytes, metadata: Dict[str, Any])
    """
    if not raw_data:
        return raw_data, {}
    
    filename = file_key.split('/')[-1]
    
    # Only standardize CSV files
    if not filename.lower().endswith('.csv'):
        _transfer_debug(
            status_logger,
            "data_transformation",
            f"Skipping standardization for non-CSV file: {filename}",
            filename=filename,
            reason="not_csv",
        )
        return raw_data, {'skipped': True, 'reason': 'not_csv'}
    
    # If standardization not available, pass through
    if not STANDARDIZATION_AVAILABLE:
        _transfer_warning(
            status_logger,
            f"Standardization not available, passing through: {filename}",
            filename=filename,
            debug_event="data_transformation",
        )
        return raw_data, {'skipped': True, 'reason': 'standardization_unavailable'}
    
    try:
        standardized_data, metadata = standardize_csv_data(raw_data, retailer_id)
        
        # Log standardization actions
        if metadata.get('warnings'):
            for warning in metadata['warnings']:
                _transfer_warning(
                    status_logger,
                    f"Standardization warning for {filename}: {warning}",
                    filename=filename,
                    debug_event="data_transformation",
                )
        
        if metadata.get('columns_renamed'):
            rename_count = len(metadata['columns_renamed'])
            _transfer_info(
                status_logger,
                f"Renamed {rename_count} columns in {filename}",
                filename=filename,
                rename_count=rename_count,
            )
            _transfer_debug(
                status_logger,
                "column_mapping",
                f"Column renames: {metadata['columns_renamed']}",
                filename=filename,
                columns_renamed=metadata['columns_renamed'],
            )
        
        if metadata.get('errors'):
            for error in metadata['errors']:
                _transfer_error(
                    status_logger,
                    f"Standardization error for {filename}: {error}",
                    reason=str(error),
                    filename=filename,
                )
            # On error, return original data
            if metadata['errors']:
                return raw_data, metadata
        
        return standardized_data, metadata
        
    except Exception as e:
        _transfer_exception(status_logger, f"Standardization failed for {filename}", e, filename=filename)
        # Return original data on error
        return raw_data, {'error': str(e), 'exception_type': type(e).__name__}


def reorder_columns_sot(
    df, retailer_id: str, status_logger: Optional[Any] = None
):
    """
    Reorder DataFrame columns to match SOT (Source of Truth) schema.

    Uses ``UNIVERSAL_SOT_COLS`` order for columns that exist in the frame only.
    Does **not** create or inject missing SOT columns (schema is preserved).

    Output order: present SOT columns (exact order), retailer-specific spend, retailer
    impressions, then remaining extras. ``Promo_flag`` is moved last only when present.

    Uses case-insensitive matching for SOT and media-channel exclusion for retailer detection.
    """
    ordered_cols: List[str] = []

    # Create case-insensitive lookup map: normalized -> actual column name
    df_cols_normalized_map = {normalize_header_for_matching(col): col for col in df.columns}
    df_cols_normalized = set(df_cols_normalized_map.keys())

    # Known media channel prefixes (to exclude from retailer detection)
    media_channels_lower = {
        'ooh_impressions', 'ooh_spend',
        'paidsocial_impressions', 'paidsocial_spend',
        'tv_impressions', 'tv_spend'
    }

    # SOT columns that exist in df only (no injection)
    for sot_col in UNIVERSAL_SOT_COLS:
        sot_col_normalized = normalize_header_for_matching(sot_col)
        if sot_col_normalized in df_cols_normalized:
            actual_col = df_cols_normalized_map[sot_col_normalized]
            ordered_cols.append(actual_col)
        else:
            _transfer_debug(
                status_logger,
                "column_ordering",
                f"reorder_columns_sot: SOT column '{sot_col}' missing from DataFrame (not creating)",
                retailer_id=retailer_id,
                missing_sot_column=sot_col,
            )

    # Add retailer-specific columns (spend before impressions)
    # Find ANY retailer-specific spend/impressions columns
    # Pattern: {Retailer}_spend, {Retailer}_impressions
    # Exclude media channel columns (case-insensitive check)
    retailer_spend_cols = []
    retailer_impressions_cols = []
    
    for col in df.columns:
        col_normalized = normalize_header_for_matching(col)
        if col_normalized.endswith('_spend') and col_normalized not in media_channels_lower:
            retailer_spend_cols.append(col)
        elif (
            col_normalized.endswith('_impressions') or col_normalized.endswith('_impression')
        ) and col_normalized not in media_channels_lower:
            retailer_impressions_cols.append(col)
    
    # FIX #3: LOG: Track retailer column detection
    _transfer_debug(
        status_logger,
        "column_ordering",
        f"reorder_columns_sot: Found {len(retailer_spend_cols)} retailer spend cols: {retailer_spend_cols}",
        retailer_id=retailer_id,
        retailer_spend_cols=retailer_spend_cols,
    )
    _transfer_debug(
        status_logger,
        "column_ordering",
        f"reorder_columns_sot: Found {len(retailer_impressions_cols)} retailer impressions cols: {retailer_impressions_cols}",
        retailer_id=retailer_id,
        retailer_impressions_cols=retailer_impressions_cols,
    )

    # Add retailer-specific columns in order (spend before impressions)
    for col in retailer_spend_cols:
        if col not in ordered_cols:
            ordered_cols.append(col)
            _transfer_debug(
                status_logger,
                "column_ordering",
                f"reorder_columns_sot: Added retailer spend column '{col}'",
                retailer_id=retailer_id,
                column=col,
            )
    for col in retailer_impressions_cols:
        if col not in ordered_cols:
            ordered_cols.append(col)
            _transfer_debug(
                status_logger,
                "column_ordering",
                f"reorder_columns_sot: Added retailer impressions column '{col}'",
                retailer_id=retailer_id,
                column=col,
            )

    # Add any remaining columns not in the schema (preserve extras)
    for col in df.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
            _transfer_debug(
                status_logger,
                "column_ordering",
                f"Added extra column '{col}' to output (not in SOT schema)",
                retailer_id=retailer_id,
                column=col,
            )
    
    # If Promo_flag exists (any casing), keep it as the last column
    promo_flag_norm = normalize_header_for_matching("Promo_flag")
    promo_flag_actual: Optional[str] = None
    for col in df.columns:
        if normalize_header_for_matching(col) == promo_flag_norm:
            promo_flag_actual = col
            break
    if promo_flag_actual and promo_flag_actual in ordered_cols:
        ordered_cols.remove(promo_flag_actual)
        ordered_cols.append(promo_flag_actual)

    return df[ordered_cols]


def process_national_data(
    df: pd.DataFrame,
    available_retailers: List[Dict[str, str]],
    client_id: str,
    brand_name: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Process National retailer data with special handling.
    
    National is an aggregated view across all retailers. It requires:
    1. Keeping ALL columns including retailer-specific columns (Amazon_*, Bestbuy_*, Walmart_*, etc.)
    2. Renaming National_sales → Sales
    3. Renaming Week column → Date (with YYYY-MM-DD formatting)
    4. NOT triggering fan-out to individual retailers
    
    Args:
        df: Source DataFrame with National data
        available_retailers: List of retailer dicts from pipeline-infos
        client_id: Client identifier
        brand_name: Brand identifier
    
    Returns:
        Dict with single entry: {'national': processed_dataframe}
    """
    _transfer_debug(
        status_logger,
        "national_processing",
        "=== Processing NATIONAL data (separate logic) ===",
        client_id=client_id,
        brand_name=brand_name,
    )
    _transfer_debug(
        status_logger,
        "national_processing",
        f"Source columns ({len(df.columns)}): {df.columns.tolist()}",
        client_id=client_id,
        brand_name=brand_name,
    )
    
    # Create copy to avoid modifying original
    national_df = df.copy()
    
    # Step 1: Rename National_sales → Sales (case-insensitive)
    sales_renamed = False
    for col in national_df.columns:
        col_normalized = normalize_header_for_matching(col)
        if col_normalized == 'national_sales':
            national_df = national_df.rename(columns={col: 'Sales'})
            _transfer_debug(
                status_logger,
                "national_processing",
                f"NATIONAL: Renamed '{col}' → 'Sales'",
                client_id=client_id,
                brand_name=brand_name,
            )
            sales_renamed = True
            break
    
    if not sales_renamed:
        _transfer_warning(
            status_logger,
            "NATIONAL: No 'National_sales' column found to rename",
            client_id=client_id,
            brand_name=brand_name,
        )
    
    # Step 2: Handle Week column → Date (if not already done)
    date_col_exists = 'date' in [normalize_header_for_matching(c) for c in national_df.columns]
    if not date_col_exists:
        for col in national_df.columns:
            col_normalized = normalize_header_for_matching(col)
            if 'week' in col_normalized:
                national_df = national_df.rename(columns={col: 'Date'})
                _transfer_debug(
                    status_logger,
                    "national_processing",
                    f"NATIONAL: Renamed '{col}' → 'Date'",
                    client_id=client_id,
                    brand_name=brand_name,
                )
                # Format date values to YYYY-MM-DD if needed
                try:
                    national_df['Date'] = pd.to_datetime(national_df['Date']).dt.strftime('%Y-%m-%d')
                    _transfer_debug(
                        status_logger,
                        "national_processing",
                        "NATIONAL: Formatted Date column to YYYY-MM-DD",
                        client_id=client_id,
                        brand_name=brand_name,
                    )
                except Exception as e:
                    _transfer_warning(
                        status_logger,
                        f"NATIONAL: Could not format Date column: {e}",
                        client_id=client_id,
                        brand_name=brand_name,
                    )
                break
    
    # Step 3: Keep ALL other columns as-is (no dropping of retailer columns)
    # National keeps Amazon_*, Bestbuy_*, Walmart_*, etc.
    
    # Step 4: Log final columns
    _transfer_info(
        status_logger,
        f"NATIONAL: Final columns ({len(national_df.columns)}): {national_df.columns.tolist()}",
        client_id=client_id,
        brand_name=brand_name,
    )
    _transfer_info(
        status_logger,
        f"NATIONAL: Created file with {len(national_df)} rows",
        client_id=client_id,
        brand_name=brand_name,
        row_count=len(national_df),
    )
    
    return {'national': national_df}


def normalize_header_for_matching(name: str) -> str:
    """
    Normalize column header for robust matching.
    
    Removes BOM, strips whitespace, strips quotes, and lowercases.
    Used consistently across all column matching logic to prevent
    dropped columns due to whitespace/BOM/quote variations.
    
    Args:
        name: Original column name (may contain BOM, whitespace, quotes)
    
    Returns:
        Normalized column name (lowercase, trimmed, no BOM/quotes)
    """
    if not name:
        return ""
    
    # Remove BOM if present
    normalized = name.lstrip('\ufeff')
    
    # Strip whitespace
    normalized = normalized.strip()
    
    # Strip surrounding quotes (single or double)
    normalized = normalized.strip('"').strip("'")
    
    # Lowercase for case-insensitive matching
    normalized = normalized.lower()
    
    return normalized


def validate_sales_data(
    df: pd.DataFrame,
    sales_columns: List[str],
    df_columns_normalized: List[str],
    original_columns: List[str],
    status_logger: Optional[Any] = None,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Validate sales columns for null/NaN/missing values.
    Uses robust pandas validation; explicitly allows zero (numeric 0) values.
    Returns (is_valid, invalid_columns_details) with 'column', 'invalid_rows', 'invalid_count' per detail.
    """
    invalid_columns_details = []
    _transfer_info(
        status_logger,
        f"Starting sales data validation: checking {len(sales_columns)} sales column(s)",
        debug_event="sales_validation",
    )
    for sales_col_norm in sales_columns:
        try:
            col_index = df_columns_normalized.index(sales_col_norm)
            original_col_name = original_columns[col_index]
        except (ValueError, IndexError):
            _transfer_warning(
                status_logger,
                f"Could not find original column name for normalized '{sales_col_norm}', skipping validation",
                debug_event="sales_validation",
            )
            continue
        _transfer_info(
            status_logger,
            f"Validating sales column: '{original_col_name}' (normalized: '{sales_col_norm}')",
            debug_event="sales_validation",
        )
        if original_col_name not in df.columns:
            _transfer_warning(
                status_logger,
                f"Column '{original_col_name}' not found in DataFrame, skipping validation",
                debug_event="sales_validation",
            )
            continue
        col_data = df[original_col_name]
        null_mask = col_data.isna() | col_data.isnull()
        empty_mask = col_data.astype(str).str.strip() == ''
        invalid_mask = null_mask | empty_mask
        invalid_rows = df.index[invalid_mask].tolist()
        invalid_count = int(invalid_mask.sum())
        total_rows = len(df)
        valid_count = total_rows - invalid_count
        zero_count = ((col_data == 0) & (~invalid_mask)).sum() if invalid_count < total_rows else 0
        _transfer_info(
            status_logger,
            f"Sales column '{original_col_name}': {valid_count} valid rows, "
            f"{invalid_count} invalid rows (null/NaN/empty), {zero_count} zero values (valid)",
            debug_event="sales_validation",
        )
        if invalid_count > 0:
            sample_rows = invalid_rows[:10]
            _transfer_warning(
                status_logger,
                f"INVALID SALES DATA detected in column '{original_col_name}': "
                f"{invalid_count} row(s) with null/NaN/empty values. "
                f"Sample invalid row indices: {sample_rows}{'...' if len(invalid_rows) > 10 else ''}",
                debug_event="sales_validation",
            )
            invalid_columns_details.append({
                'column': original_col_name,
                'invalid_rows': invalid_rows,
                'invalid_count': invalid_count
            })
    is_valid = len(invalid_columns_details) == 0
    if is_valid:
        _transfer_info(
            status_logger,
            "Sales data validation PASSED: All sales columns contain valid data (no null/NaN/empty values)",
            debug_event="sales_validation",
        )
    else:
        total_invalid_rows = sum(detail['invalid_count'] for detail in invalid_columns_details)
        _transfer_error(
            status_logger,
            f"Sales data validation FAILED: {len(invalid_columns_details)} column(s) with invalid data, "
            f"{total_invalid_rows} total invalid row(s). File ingestion will be skipped.",
            reason="invalid_sales_data",
        )
    return is_valid, invalid_columns_details


def _is_channel_metric_column(normalized_name: str) -> bool:
    """True if column is a media or retailer spend/impressions metric (not sales/date)."""
    if not normalized_name:
        return False
    if normalized_name == "sales" or normalized_name.endswith("_sales"):
        return False
    if (
        normalized_name.endswith("_impressions")
        or normalized_name.endswith("_impression")
        or normalized_name.endswith("_spend")
    ):
        return True
    return False


def _channel_column_pairs(
    df_columns_normalized: List[str], original_columns: List[str]
) -> List[Tuple[str, str]]:
    """Return list of (normalized_name, original_header) for channel metric columns."""
    pairs: List[Tuple[str, str]] = []
    for i, col_norm in enumerate(df_columns_normalized):
        if _is_channel_metric_column(col_norm):
            pairs.append((col_norm, original_columns[i]))
    return pairs


def validate_channel_data(
    df: pd.DataFrame,
    df_columns_normalized: List[str],
    original_columns: List[str],
    status_logger: Optional[Any] = None,
) -> Tuple[bool, bool, Dict[str, Any]]:
    """
    Validate media/retailer channel impressions and spend columns.

    Returns:
        has_quality_issues: True if any present channel cell is null (partial), negative, or non-numeric.
        is_channel_missing: True if there is no usable impression data (no *_impressions or *_impression
            columns, or every such column is entirely null/empty).
        details: dict with missing_channels, quality_issues, impression_columns_checked, etc.
    """
    details: Dict[str, Any] = {
        "missing_channels": [],
        "quality_issues": [],
        "impression_columns": [],
    }
    pairs = _channel_column_pairs(df_columns_normalized, original_columns)
    impression_pairs = [
        (n, o) for n, o in pairs if n.endswith("_impressions") or n.endswith("_impression")
    ]
    spend_pairs = [(n, o) for n, o in pairs if n.endswith("_spend")]

    _transfer_info(
        status_logger,
        f"Starting channel data validation: {len(impression_pairs)} impression col(s), {len(spend_pairs)} spend col(s)",
        debug_event="channel_validation",
    )

    # No impression columns at all -> missing channel data
    if not impression_pairs:
        details["missing_channels"].append(
            {
                "reason": "no_impressions_columns",
                "message": "No *_impressions or *_impression columns in file",
            }
        )
        _transfer_error(
            status_logger,
            "CHANNEL VALIDATION FAILED: No *_impressions or *_impression columns found in file",
            reason="channel_data_missing",
            debug_event="channel_validation",
        )
        return False, True, details

    has_quality_issues = False
    is_channel_missing = False

    def analyze_metric_column(original_col: str, col_norm: str, kind: str) -> None:
        nonlocal has_quality_issues, is_channel_missing
        if original_col not in df.columns:
            return
        col_data = df[original_col]
        n = len(df)
        null_mask = col_data.isna() | col_data.isnull()
        try:
            empty_mask = col_data.astype(str).str.strip() == ""
        except Exception:
            empty_mask = pd.Series([False] * n, index=df.index)
        blank_mask = null_mask | empty_mask
        coerced = pd.to_numeric(col_data, errors="coerce")
        had_value = ~blank_mask
        non_numeric_mask = had_value & coerced.isna()
        negative_mask = coerced.notna() & (coerced < 0)
        # Some rows blank but not all -> quality (partial null)
        partial_null_mask = (
            blank_mask
            if (n > 0 and blank_mask.any() and not bool(blank_mask.all()))
            else pd.Series([False] * n, index=df.index)
        )

        if kind == "impressions" and n > 0 and bool(blank_mask.all()):
            is_channel_missing = True
            details["missing_channels"].append(
                {
                    "column": original_col,
                    "normalized": col_norm,
                    "reason": "all_null_or_empty",
                }
            )
            _transfer_error(
                status_logger,
                f"CHANNEL DATA MISSING: Impressions column '{original_col}' is entirely null/empty",
                reason="channel_impressions_all_null",
                column=original_col,
                debug_event="channel_validation",
            )
            return

        if kind == "spend" and n > 0 and bool(blank_mask.all()):
            has_quality_issues = True
            details["quality_issues"].append(
                {
                    "column": original_col,
                    "normalized": col_norm,
                    "kind": kind,
                    "partial_null_count": 0,
                    "non_numeric_count": 0,
                    "negative_count": 0,
                    "all_null_or_empty": True,
                    "sample_row_indices": [],
                }
            )
            _transfer_warning(
                status_logger,
                f"Channel quality issue: spend column '{original_col}' is entirely null/empty (ingestion continues)",
                debug_event="channel_validation",
                column=original_col,
            )
            return

        if partial_null_mask.any() or non_numeric_mask.any() or negative_mask.any():
            has_quality_issues = True
            bad_idx = df.index[partial_null_mask | non_numeric_mask | negative_mask].tolist()
            issue = {
                "column": original_col,
                "normalized": col_norm,
                "kind": kind,
                "partial_null_count": int(partial_null_mask.sum()),
                "non_numeric_count": int(non_numeric_mask.sum()),
                "negative_count": int(negative_mask.sum()),
                "sample_row_indices": bad_idx[:20],
            }
            details["quality_issues"].append(issue)
            _transfer_warning(
                status_logger,
                f"Channel quality issue in '{original_col}' ({kind}): "
                f"partial_null={issue['partial_null_count']}, non_numeric={issue['non_numeric_count']}, "
                f"negative={issue['negative_count']}",
                debug_event="channel_validation",
                column=original_col,
            )

    for col_norm, original_col in impression_pairs:
        details["impression_columns"].append(original_col)
        analyze_metric_column(original_col, col_norm, "impressions")

    for col_norm, original_col in spend_pairs:
        analyze_metric_column(original_col, col_norm, "spend")

    if not is_channel_missing and not has_quality_issues:
        _transfer_info(
            status_logger,
            "Channel data validation PASSED: impression/spend metrics are present and consistent",
            debug_event="channel_validation",
        )
    elif has_quality_issues and not is_channel_missing:
        _transfer_info(
            status_logger,
            "Channel data validation: quality issues logged (ingestion continues)",
            debug_event="channel_validation",
        )

    return has_quality_issues, is_channel_missing, details


def _emit_validation_metric(metric_name: str, status_logger: Optional[Any] = None) -> None:
    """Emit DogStatsD counter for Datadog (Lambda extension)."""
    try:
        if _pipeline_metrics:
            _pipeline_metrics.increment(metric_name, tags=[f"env:{ENVIRONMENT}"])
    except Exception as exc:
        _transfer_warning(
            status_logger,
            f"Failed to emit metric {metric_name}: {exc}",
            debug_event="metrics",
        )


def send_missing_channel_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    missing_channels: List[Dict[str, Any]],
    quality_issues: List[Dict[str, Any]],
    total_rows: int,
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Send Slack alert when file ingestion is skipped due to missing channel (impression) data.
    """
    if not missing_channels:
        return True
    try:

        def convert_to_native_types(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: convert_to_native_types(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_to_native_types(item) for item in obj]
            if hasattr(obj, "item"):
                return obj.item()
            if isinstance(obj, (int, float, str, bool)) or obj is None:
                return obj
            try:
                if hasattr(obj, "dtype"):
                    if "int" in str(obj.dtype):
                        return int(obj)
                    if "float" in str(obj.dtype):
                        return float(obj)
                    return int(obj)
            except (ValueError, TypeError, AttributeError):
                pass
            try:
                return str(obj)
            except Exception:
                return repr(obj)

        payload = {
            "alert_type": "MISSING_CHANNEL_DATA",
            "client_id": client_id,
            "brand_name": brand_name,
            "retailer_id": retailer_id,
            "filename": filename,
            "missing_channels": convert_to_native_types(missing_channels),
            "quality_issues": convert_to_native_types(quality_issues),
            "total_rows": int(total_rows),
            "message": (
                f"File ingestion skipped: missing channel data. "
                f"{len(missing_channels)} issue(s) reported."
            ),
        }
        response = lambda_client.invoke(
            FunctionName=SLACK_LAMBDA_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload, default=str),
        )
        if response.get("StatusCode") == 202:
            _transfer_info(
                status_logger,
                f"Missing channel data alert sent for {client_id}/{brand_name}/{retailer_id}: {filename}",
                alert_type="MISSING_CHANNEL_DATA",
            )
            return True
        _transfer_warning(
            status_logger,
            f"Missing channel alert may have failed: StatusCode={response.get('StatusCode')}",
            alert_type="MISSING_CHANNEL_DATA",
        )
        return False
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Failed to send missing channel alert: {e}",
            alert_type="MISSING_CHANNEL_DATA",
        )
        return False


def process_with_pandas(
    csv_data: bytes,
    client_id: str,
    brand_name: str,
    available_retailers: List[Dict[str, str]],
    event_retailer_id: str = None,
    filename: str = None,
    status_logger: Optional[Any] = None,
) -> Dict[str, bytes]:
    """
    Process CSV data with pandas:
    1. Load CSV into DataFrame
    2. Preserve original column names (data integrity requirement)
    3. Normalize headers for robust matching (BOM/whitespace/quotes removal)
    4. Find columns containing "week" -> rename to "Date" and reformat to YYYY-MM-DD
    5. Detect retailers from column prefixes using normalized headers
    6. Split into retailer-specific DataFrames preserving original column names
    7. Return dict of {retailer_id: csv_bytes}
    
    Args:
        csv_data: Raw CSV file data as bytes
        client_id: Client identifier
        brand_name: Brand name
        available_retailers: List of dicts with retailer_id and retailer_name from pipeline-infos
        event_retailer_id: Retailer ID from event - only process columns for this retailer (prevents fan-out)
    
    Returns:
        Dict mapping retailer_id to processed CSV bytes (only event_retailer_id if provided)
    """
    if not PANDAS_AVAILABLE:
        _transfer_error(
            status_logger,
            "Pandas not available - cannot process data",
            reason="pandas_unavailable",
        )
        return {}
    
    try:
        # Load CSV into pandas DataFrame
        df = pd.read_csv(io.BytesIO(csv_data))
        
        # Check for empty DataFrame (no data rows)
        if df.empty or len(df) == 0:
            _transfer_warning(
                status_logger,
                "DataFrame is empty after loading CSV - file contains no data rows",
                debug_event="file_parsing",
            )
            return {}
        
        # Additional check: ensure we have at least MIN_DATA_ROWS of actual data
        if len(df) < MIN_DATA_ROWS:
            _transfer_warning(
                status_logger,
                f"DataFrame has only {len(df)} row(s), minimum {MIN_DATA_ROWS} data row required",
                debug_event="file_parsing",
            )
            return {}
        
        # Preserve original column names (data integrity requirement)
        original_columns = df.columns.tolist()
        
        # TASK 4: Apply robust header normalization BEFORE column selection
        # This prevents dropped columns due to BOM/whitespace/quote variations
        df_columns_normalized = [normalize_header_for_matching(col) for col in original_columns]
        
        # ============================================================================
        # SALES DATA VALIDATION: Check for null/NaN/missing values BEFORE processing
        # ============================================================================
        sales_columns = []
        for i, col_norm in enumerate(df_columns_normalized):
            if col_norm.endswith('_sales') or col_norm == 'sales':
                sales_columns.append(col_norm)
                _transfer_debug(
                    status_logger,
                    "file_parsing",
                    f"Detected sales column: '{original_columns[i]}' (normalized: '{col_norm}')",
                    column=original_columns[i],
                    column_normalized=col_norm,
                )
        if sales_columns:
            _transfer_info(
                status_logger,
                f"Validating {len(sales_columns)} sales column(s) for null/NaN/missing values",
                debug_event="sales_validation",
            )
            is_valid, invalid_columns_details = validate_sales_data(
                df, sales_columns, df_columns_normalized, original_columns, status_logger=status_logger
            )
            if not is_valid:
                total_invalid_rows = sum(detail['invalid_count'] for detail in invalid_columns_details)
                _emit_validation_metric(
                    "data_ingestion.validation.sales_missing", status_logger=status_logger
                )
                _transfer_error(
                    status_logger,
                    f"SALES VALIDATION FAILED: Skipping file ingestion due to invalid/missing sales data. "
                    f"Found {len(invalid_columns_details)} column(s) with invalid data: "
                    f"{[detail['column'] for detail in invalid_columns_details]}. "
                    f"Total invalid rows: {total_invalid_rows}/{len(df)}",
                    reason="sales_validation_failed",
                    client_id=client_id,
                    retailer_id=event_retailer_id or "unknown",
                    filename=filename or "",
                    invalid_columns_count=len(invalid_columns_details),
                    invalid_row_count=int(total_invalid_rows),
                )
                if filename:
                    send_invalid_sales_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=event_retailer_id or 'unknown',
                        filename=filename,
                        invalid_columns=invalid_columns_details,
                        total_rows=len(df),
                        invalid_row_count=total_invalid_rows,
                        status_logger=status_logger,
                    )
                else:
                    _transfer_warning(
                        status_logger,
                        "Filename not provided, cannot send Slack alert for invalid sales data",
                        debug_event="sales_validation",
                    )
                return {}
        else:
            _transfer_warning(
                status_logger,
                "No sales columns detected for validation. File may not contain sales data.",
                debug_event="sales_validation",
            )

        # ============================================================================
        # CHANNEL DATA VALIDATION (impressions / spend): quality vs missing
        # ============================================================================
        ch_quality, ch_missing, ch_details = validate_channel_data(
            df, df_columns_normalized, original_columns, status_logger=status_logger
        )
        if ch_missing:
            _emit_validation_metric(
                "data_ingestion.validation.channel_missing", status_logger=status_logger
            )
            _transfer_error(
                status_logger,
                "CHANNEL VALIDATION FAILED: Skipping file ingestion due to missing channel (impression) data",
                reason="channel_validation_missing",
            )
            if filename:
                send_missing_channel_alert(
                    client_id=client_id,
                    brand_name=brand_name,
                    retailer_id=event_retailer_id or "unknown",
                    filename=filename,
                    missing_channels=ch_details.get("missing_channels", []),
                    quality_issues=ch_details.get("quality_issues", []),
                    total_rows=len(df),
                    status_logger=status_logger,
                )
            else:
                _transfer_warning(
                    status_logger,
                    "Filename not provided, cannot send Slack alert for missing channel data",
                    debug_event="channel_validation",
                )
            return {}
        if ch_quality:
            _emit_validation_metric(
                "data_ingestion.validation.channel_quality", status_logger=status_logger
            )
        
        # Create lowercase copy for checking (now normalized)
        df_columns_lower = df_columns_normalized
        
        # ============================================================================
        # PHASE 2: Date Column Existence Check
        # ============================================================================
        # TASK 4: Check if Date column already exists using normalized headers
        has_date_column = 'date' in df_columns_normalized
        
        if has_date_column:
            # EDGE CASE: Both Date and week columns exist
            # TASK 4: Check if week column also exists using normalized headers
            week_col_exists = 'week' in df_columns_normalized
            
            if week_col_exists:
                _transfer_warning(
                    status_logger,
                    "Both 'Date' and 'week' columns exist. "
                    "Keeping 'Date' column as-is and ignoring 'week' column to prevent duplicates.",
                    debug_event="file_parsing",
                )
            else:
                _transfer_info(
                    status_logger,
                    "Date column already exists, skipping week column processing",
                    debug_event="file_parsing",
                )
            
            # Optionally: Validate and standardize existing Date column format
            # (This ensures Date is in YYYY-MM-DD format even if it already exists)
            try:
                # TASK 4: Use normalized headers to find date column index
                date_col_name = original_columns[df_columns_normalized.index('date')]
                original_date_values = df[date_col_name].copy()  # Preserve original for fallback
                
                # Try to parse dates
                parsed_dates = pd.to_datetime(df[date_col_name], errors='coerce', dayfirst=False)
                
                # Check conversion success rate
                valid_count = parsed_dates.notna().sum()
                total_count = len(parsed_dates)
                success_rate = valid_count / total_count if total_count > 0 else 0
                
                if success_rate < 0.5:
                    # Less than 50% parsed successfully - keep original values
                    _transfer_warning(
                        status_logger,
                        f"Date parsing failed for {total_count - valid_count}/{total_count} rows "
                        f"({(1-success_rate)*100:.1f}%). Keeping original date values.",
                        debug_event="file_parsing",
                    )
                    # Don't modify the column - keep original values
                else:
                    # Format successful parses to YYYY-MM-DD
                    df[date_col_name] = parsed_dates.dt.strftime('%Y-%m-%d')
                    # Fill failed parses with original values (not empty strings)
                    failed_mask = parsed_dates.isna()
                    if failed_mask.any():
                        df.loc[failed_mask, date_col_name] = original_date_values[failed_mask].astype(str)
                        _transfer_warning(
                            status_logger,
                            f"Date parsing failed for {failed_mask.sum()}/{total_count} rows. "
                            f"Kept original values for failed rows.",
                            debug_event="file_parsing",
                        )
                    else:
                        _transfer_debug(
                            status_logger,
                            "file_parsing",
                            f"Standardized existing Date column '{date_col_name}' to YYYY-MM-DD format",
                            date_col_name=date_col_name,
                        )
            except Exception as e:
                _transfer_warning(
                    status_logger,
                    f"Could not standardize existing Date column: {e}",
                    debug_event="file_parsing",
                )
        
        else:
            # ============================================================================
            # PHASE 1: Week Column Detection with Pattern Matching
            # ============================================================================
            # Match columns that START with 'week' to handle variations:
            #   - "week" (exact)
            #   - "Week (From Mon)" (common variation with space)
            #   - "Week_(From_Mon)" (variation with underscore)
            # But EXCLUDE columns where 'week' appears at the END (event columns):
            #   - "primeday_week" (NOT a date column)
            #   - "blackfriday_week" (NOT a date column)
            #
            # Regex pattern: ^"?week[\s_]*(\(.*\))?"?$
            #   - ^ = start of string
            #   - "? = optional leading quote
            #   - week = literal 'week'
            #   - [\s_]* = optional: whitespace OR underscores (handles both formats)
            #   - (\(.*\))? = optional: parenthetical text (e.g., "(From Mon)")
            #   - "? = optional trailing quote
            #   - $ = end of string
            week_pattern = re.compile(r'^"?week[\s_]*(\(.*\))?"?$', re.IGNORECASE)
            
            week_col_index = None
            original_week_col = None
            
            # TASK 4: Use normalized headers for week column detection (robust against BOM/whitespace/quotes)
            for idx, col_normalized in enumerate(df_columns_normalized):
                if week_pattern.match(col_normalized):
                    week_col_index = idx
                    original_week_col = original_columns[idx]
                    _transfer_info(
                        status_logger,
                        f"Week column detected via pattern match: '{original_week_col}' (normalized: '{col_normalized}')",
                        debug_event="file_parsing",
                    )
                    break
            
            if week_col_index is not None and original_week_col:
                _transfer_info(
                    status_logger,
                    f"Found week column: '{original_week_col}', renaming to 'Date'",
                    debug_event="file_parsing",
                )
                df = df.rename(columns={original_week_col: 'Date'})
                
                # Update original_columns and df_columns_normalized to reflect the rename
                original_columns[week_col_index] = 'Date'
                # TASK 4: Recreate normalized headers after rename
                df_columns_normalized = [normalize_header_for_matching(col) for col in df.columns]
                df_columns_lower = df_columns_normalized
                
                # ============================================================================
                # PHASE 4: Robust Date Parsing Improvements
                # ============================================================================
                # Reformat date values to YYYY-MM-DD with robust parsing
                try:
                    # Save original values before any conversion attempts
                    original_date_values = df['Date'].copy()
                    
                    # Strategy 1: Try direct datetime parsing (handles most formats)
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', infer_datetime_format=True, dayfirst=False)
                    
                    # Strategy 2: If that fails, try numeric week format (YYYYWW)
                    if df['Date'].isna().all():
                        _transfer_debug(
                            status_logger,
                            "file_parsing",
                            "Direct parsing failed, trying numeric week format (YYYYWW)",
                        )
                        try:
                            # Try parsing as YYYYWW (e.g., 202301 for week 1 of 2023)
                            # This is a fallback - may not be accurate
                            numeric_weeks = pd.to_numeric(original_date_values, errors='coerce')
                            # Convert YYYYWW to date (approximate - first day of year)
                            # This is a rough approximation
                            valid_numeric = numeric_weeks.dropna()
                            if len(valid_numeric) > 0:
                                df['Date'] = pd.to_datetime(
                                    valid_numeric.astype(int).astype(str).str[:4] + '-01-01', 
                                    errors='coerce'
                                )
                                _transfer_warning(
                                    status_logger,
                                    "Using approximate date conversion from YYYYWW format",
                                    debug_event="file_parsing",
                                )
                        except Exception as fallback_error:
                            _transfer_debug(
                                status_logger,
                                "file_parsing",
                                f"Fallback date parsing also failed: {fallback_error}",
                                error=str(fallback_error),
                            )
                            pass
                    
                    # Strategy 3: Format to YYYY-MM-DD
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    
                    # Replace NaT with empty string
                    df['Date'] = df['Date'].fillna('')
                    
                    # ============================================================================
                    # PHASE 3: Enhanced Logging and Edge Case Handling
                    # ============================================================================
                    # Log validation metrics
                    valid_dates = (df['Date'] != '').sum()
                    total_rows = len(df)
                    _transfer_info(
                        status_logger,
                        f"Reformatted {valid_dates}/{total_rows} date values to YYYY-MM-DD format",
                        debug_event="file_parsing",
                    )
                    
                    if valid_dates == 0:
                        _transfer_warning(
                            status_logger,
                            "No valid dates found in week column after conversion",
                            debug_event="file_parsing",
                        )
                        
                except Exception as e:
                    _transfer_exception(status_logger, "Error reformatting dates", e, debug_event="file_parsing")
                    # Don't fail completely - keep original values
                    df['Date'] = df['Date'].fillna('')
            else:
                # ============================================================================
                # PHASE 3: Enhanced Logging - No exact week column found
                # ============================================================================
                # TASK 4: Check if there are columns containing "week" using normalized headers (for logging only)
                week_containing_cols = [
                    original_columns[i] for i, col_norm in enumerate(df_columns_normalized)
                    if 'week' in col_norm
                ]
                if week_containing_cols:
                    _transfer_warning(
                        status_logger,
                        f"Found columns containing 'week' but not matching pattern: {week_containing_cols}. "
                        f"These will NOT be processed. Expected column matching week pattern.",
                        debug_event="file_parsing",
                    )
                else:
                    _transfer_info(
                        status_logger,
                        "No 'week' column found. Date column may need to be provided in source data.",
                        debug_event="file_parsing",
                    )
                
                # EDGE CASE: Neither Date nor week exists - this is a validation issue
                # Should be caught by validate_required_columns, but log for visibility
                _transfer_warning(
                    status_logger,
                    "No Date column and no 'week' column found. "
                    "Data may fail downstream validation.",
                    debug_event="file_parsing",
                )
        
        # =========================================================================
        # CONTENT VALIDATION: Duplicate dates and date gaps (before retailer split)
        # =========================================================================
        if 'date' in df_columns_normalized:
            dup_result = detect_duplicate_dates(df, status_logger=status_logger)
            if dup_result.get('has_duplicates'):
                _transfer_error(
                    status_logger,
                    f"DUPLICATE DATES: Skipping file ingestion. "
                    f"{dup_result.get('duplicate_count', 0)} date(s) appear more than once.",
                    reason="duplicate_dates",
                )
                if filename:
                    send_duplicate_dates_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=event_retailer_id or 'unknown',
                        filename=filename,
                        duplicate_details=dup_result,
                        status_logger=status_logger,
                    )
                return {}
            gap_result = detect_date_gaps(df, status_logger=status_logger)
            if gap_result.get('has_gaps'):
                _transfer_error(
                    status_logger,
                    f"DATE GAPS: Skipping file ingestion. "
                    f"{gap_result.get('gap_count', 0)} gap(s) (missing weeks) detected.",
                    reason="date_gaps",
                )
                if filename:
                    send_date_gaps_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=event_retailer_id or 'unknown',
                        filename=filename,
                        gap_details=gap_result,
                        status_logger=status_logger,
                    )
                return {}
            helper = None
            if PipelineInfoHelper:
                try:
                    helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)
                except Exception as e:
                    _transfer_debug(
                        status_logger,
                        "pipeline_correlation",
                        f"PipelineInfoHelper not available for row count check: {e}",
                        error=str(e),
                    )
            row_result = validate_row_count(
                df,
                helper,
                client_id,
                brand_name,
                event_retailer_id or 'unknown',
                status_logger=status_logger,
            )
            if not row_result.get('valid'):
                _transfer_error(
                    status_logger,
                    f"ROW COUNT DROP: Skipping file ingestion. "
                    f"Current rows={row_result.get('current_rows', 0)} vs historical avg={row_result.get('historical_avg', 0)} "
                    f"({row_result.get('drop_percentage', 0)}% drop).",
                    reason="row_count_drop_red",
                )
                if filename:
                    send_row_count_drop_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=event_retailer_id or 'unknown',
                        filename=filename,
                        validation_result=row_result,
                        status_logger=status_logger,
                    )
                return {}
            if row_result.get('severity') == 'YELLOW':
                _transfer_warning(
                    status_logger,
                    f"ROW COUNT WARN: Current rows={row_result.get('current_rows', 0)} vs historical avg={row_result.get('historical_avg', 0)} "
                    f"({row_result.get('drop_percentage', 0)}% drop). Proceeding with ingestion.",
                    debug_event="row_count_check",
                )
                if filename:
                    send_row_count_drop_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=event_retailer_id or 'unknown',
                        filename=filename,
                        validation_result=row_result,
                        status_logger=status_logger,
                    )
        
        # =========================================================================
        # NATIONAL EARLY DETECTION - Process separately to prevent fan-out
        # =========================================================================
        # Check if this is a National retailer file BEFORE column-based detection
        # This prevents the fan-out issue where National files trigger processing
        # for individual retailers (amazon, bestbuy, walmart) found in column names
        
        # STEP 7: Tighten National routing - ONLY route if event_retailer_id is 'national'
        # Do NOT route based on presence of National_sales column (prevents false positives)
        is_national_file = (event_retailer_id and event_retailer_id.lower() == 'national')
        
        if is_national_file:
            _transfer_info(
                status_logger,
                "DETECTED: National retailer file - using separate processing logic",
                debug_event="national_processing",
            )
            _transfer_info(
                status_logger,
                "NATIONAL: Skipping column-based retailer detection to prevent fan-out",
                debug_event="national_processing",
            )
            national_result = process_national_data(
                df, available_retailers, client_id, brand_name, status_logger=status_logger
            )
            # Convert DataFrames to CSV bytes for return
            result = {}
            for retailer_id_key, retailer_df in national_result.items():
                csv_buffer = io.BytesIO()
                retailer_df.to_csv(csv_buffer, index=False)
                result[retailer_id_key] = csv_buffer.getvalue()
            return result
        
        # =========================================================================
        # EXISTING LOGIC FOR NON-NATIONAL RETAILERS
        # =========================================================================
        # CRITICAL: Detect retailers directly from column names
        # Pattern: {retailer}_columnname (e.g., walmart_sales, amazon_impressions, best_buy_sales)
        # This detects ALL retailers in file, not just those in available_retailers
        
        # Known retailer patterns to detect from columns
        # These are extracted from column prefixes like "walmart_sales", "amazon_impressions"
        # Pattern supports: simple names (walmart), compound names (best_buy), multi-word (best_buy_usa)
        detected_retailers = set()
        
        # Known media channel prefixes to exclude from retailer detection
        media_channel_prefixes = {'ooh', 'tv', 'paidsocial', 'paid_social', 'search', 'display', 'video', 'retailer'}
        
        # STEP 1: Sales-only retailer detection
        # Pattern: anything before _sales (retailer detection based ONLY on sales columns)
        # This prevents non-retailer columns (holiday_spend, promo_spend, etc.) from being treated as retailer evidence
        retailer_column_pattern = re.compile(r'^(.+?)_sales$', re.IGNORECASE)
        
        # TASK 4: Use normalized headers for retailer detection
        for i, col in enumerate(df.columns):
            col_normalized = df_columns_normalized[i]  # Use normalized version
            match = retailer_column_pattern.match(col_normalized)
            if match:
                retailer_prefix = match.group(1)
                # Skip known media channel prefixes (these are not retailers)
                if retailer_prefix.lower() in media_channel_prefixes:
                    _transfer_debug(
                        status_logger,
                        "column_mapping",
                        f"Skipping media channel prefix '{retailer_prefix}' from column '{col}'",
                        retailer_prefix=retailer_prefix,
                        column=col,
                    )
                    continue
                # Add the detected retailer (e.g., "walmart", "amazon", "best_buy")
                detected_retailers.add(retailer_prefix)
                _transfer_debug(
                    status_logger,
                    "column_mapping",
                    f"Detected retailer '{retailer_prefix}' from column '{col}'",
                    retailer_prefix=retailer_prefix,
                    column=col,
                )
        
        # CRITICAL: Filter detected retailers against pipeline-infos (source of truth)
        # Only process retailers that exist in available_retailers from pipeline-infos table
        valid_retailer_ids = {r.get('retailer_id', '').lower() for r in available_retailers}
        valid_retailer_names = {r.get('retailer_name', '').lower() for r in available_retailers}
        valid_retailers_set = valid_retailer_ids | valid_retailer_names
        
        # Filter detected_retailers to only include valid retailers from pipeline-infos
        filtered_retailers = {r.lower() for r in detected_retailers if r.lower() in valid_retailers_set}
        rejected_retailers = detected_retailers - filtered_retailers
        
        if rejected_retailers:
            _transfer_warning(
                status_logger,
                f"Rejected non-retailer column prefixes (not in pipeline-infos): {list(rejected_retailers)}",
                debug_event="retailer_detection",
            )
        
        if filtered_retailers:
            detected_retailers = filtered_retailers
            _transfer_info(
                status_logger,
                f"Filtered retailers (validated against pipeline-info): {list(detected_retailers)}",
                debug_event="retailer_detection",
            )
        else:
            # No column-based detection matched pipeline-info, use all available retailers
            detected_retailers = valid_retailer_ids
            _transfer_info(
                status_logger,
                f"No column-based retailers matched pipeline-info, using all available retailers: {list(detected_retailers)}",
                debug_event="retailer_detection",
            )
        
        # TASK 3: Prevent fan-out - only process event_retailer_id
        # If event_retailer_id is provided, force processing only for that retailer
        if event_retailer_id:
            event_retailer_id_lower = event_retailer_id.lower()
            # Check if event retailer is in detected retailers or available retailers
            if event_retailer_id_lower in detected_retailers or event_retailer_id_lower in valid_retailers_set:
                detected_retailers = {event_retailer_id_lower}
                _transfer_info(
                    status_logger,
                    f"TASK 3: Processing only event retailer '{event_retailer_id}' (preventing fan-out)",
                    debug_event="retailer_detection",
                )
            else:
                # Event retailer not found - log warning but still process if detected
                _transfer_warning(
                    status_logger,
                    f"TASK 3: Event retailer '{event_retailer_id}' not found in detected/available retailers. "
                    f"Detected: {list(detected_retailers)}, Available: {list(valid_retailers_set)}. "
                    f"Will process detected retailers but only upload event retailer output.",
                    debug_event="retailer_detection",
                )
                # Still restrict to event retailer if it's in available retailers
                if event_retailer_id_lower in valid_retailers_set:
                    detected_retailers = {event_retailer_id_lower}
        
        # TASK 6: Capture normalized source columns AFTER date normalization, BEFORE retailer column selection
        # This is used for drift detection to classify missing columns
        normalized_source_cols_for_drift = set(df_columns_normalized)
        
        # Create retailer map for column filtering (only from pipeline-info)
        retailer_map = {}
        for retailer in available_retailers:
            retailer_id = retailer.get('retailer_id', '').lower()
            retailer_name = retailer.get('retailer_name', '').lower()
            if retailer_id:
                retailer_map[retailer_id] = retailer_id
            if retailer_name:
                retailer_map[retailer_name] = retailer_id
        
        # DO NOT add auto-detected retailers not in available_retailers
        # This prevents creating folders for non-retailer column prefixes (e.g., total_sales, media_sales)
        
        # Split into retailer-specific DataFrames
        result = {}
        for retailer_id in detected_retailers:
            retailer_id_lower = retailer_id.lower()
            
            # FIX #4: Log start of retailer processing
            _transfer_info(
                status_logger,
                f"=== Processing retailer: '{retailer_id}' ===",
                retailer_id=retailer_id,
                debug_event="retailer_split",
            )
            _transfer_debug(
                status_logger,
                "retailer_split",
                f"Source DataFrame columns ({len(df.columns)}): {df.columns.tolist()}",
                retailer_id=retailer_id,
                column_count=len(df.columns),
            )
            
            # Defensive check: ensure retailer is in retailer_map (from pipeline-infos)
            if retailer_id_lower not in retailer_map:
                _transfer_warning(
                    status_logger,
                    f"Skipping invalid retailer '{retailer_id}' (not in pipeline-infos)",
                    retailer_id=retailer_id,
                    debug_event="retailer_split",
                )
                continue
            
            # STEP 3: Column selection that preserves non-retailer columns
            # Strategy:
            # 1. Keep all columns NOT prefixed by any valid retailer_id (preserves holiday_spend, TV_spend, etc.)
            # 2. Keep columns prefixed by THIS retailer_id
            # 3. Drop columns prefixed by OTHER valid retailer_ids
            
            # Common columns: ALL SOT columns that should be present in every retailer file
            common_cols = [
                'date', 'sales', 'retailer_sales',
                'in_stock_rate', 'gqv',
                'ooh_impressions', 'ooh_spend',
                'paidsocial_impressions', 'paidsocial_spend',
                'tv_impressions', 'tv_spend',
                'promo_flag'
            ]
            
            # STEP 2: Remove naming heuristics - use only canonical retailer_id from pipeline-infos
            # Get retailer identifiers (both retailer_id and retailer_name) but keep simple
            retailer_identifiers = [k for k, v in retailer_map.items() if v == retailer_id]
            
            # Always include retailer_id itself (lowercased for consistency)
            if retailer_id and retailer_id.lower() not in [rid.lower() for rid in retailer_identifiers]:
                retailer_identifiers.append(retailer_id.lower())
            
            # Remove duplicates while preserving case-insensitive uniqueness
            seen = set()
            unique_identifiers = []
            for rid in retailer_identifiers:
                rid_lower = rid.lower()
                if rid_lower not in seen:
                    seen.add(rid_lower)
                    unique_identifiers.append(rid)
            retailer_identifiers = unique_identifiers
            
            _transfer_debug(
                status_logger,
                "retailer_split",
                f"STEP 2: Retailer identifiers for '{retailer_id}': {retailer_identifiers} (no naming heuristics)",
                retailer_id=retailer_id,
            )
            
            # Get all valid retailer prefixes (for exclusion of other retailers)
            all_valid_retailer_prefixes = {r.get('retailer_id', '').lower() for r in available_retailers}
            all_valid_retailer_prefixes.update({r.get('retailer_name', '').lower() for r in available_retailers})
            all_valid_retailer_prefixes.discard('')  # Remove empty
            all_valid_retailer_prefixes.discard(retailer_id.lower())  # Remove current retailer
            
            # CRITICAL FIX: Recreate tracking lists from current DataFrame state
            original_columns = df.columns.tolist()
            df_columns_normalized = [normalize_header_for_matching(col) for col in original_columns]
            
            # Track dropped columns with reasons
            dropped_columns = []  # List of dicts: {'column': str, 'reason': str}
            
            # STEP 3: Build column selection list
            retailer_cols = []
            matched_common = []
            matched_this_retailer = []
            matched_non_retailer = []
            
            for i, col_normalized in enumerate(df_columns_normalized):
                original_col = original_columns[i]
                
                # 1. Include common columns (case-insensitive)
                if col_normalized in common_cols:
                    retailer_cols.append(original_col)
                    matched_common.append(original_col)
                    continue
                
                # 2. Check if column is prefixed by THIS retailer
                is_this_retailer = any(col_normalized.startswith(f"{rid.lower()}_") for rid in retailer_identifiers)
                
                # 3. Check if column is prefixed by OTHER valid retailers
                is_other_retailer = any(col_normalized.startswith(f"{rid.lower()}_") for rid in all_valid_retailer_prefixes)
                
                if is_this_retailer:
                    # Keep: This retailer's columns
                    retailer_cols.append(original_col)
                    matched_this_retailer.append(original_col)
                elif is_other_retailer:
                    # Drop: Other retailer's columns (prevent cross-retailer leakage)
                    reason = f"Other retailer column (prefix matches another retailer: {[rid for rid in all_valid_retailer_prefixes if col_normalized.startswith(f'{rid.lower()}_')][0]})"
                    dropped_columns.append({'column': original_col, 'reason': reason})
                    _transfer_warning(
                        status_logger,
                        f"STEP 8: DROPPED COLUMN: '{original_col}' - Reason: {reason}",
                        retailer_id=retailer_id,
                        column=original_col,
                        reason=reason,
                        debug_event="column_selection",
                    )
                else:
                    # Keep: Non-retailer columns (preserves holiday_spend, TV_spend, promo_spend, etc.)
                    retailer_cols.append(original_col)
                    matched_non_retailer.append(original_col)
            
            _transfer_debug(
                status_logger,
                "retailer_split",
                f"STEP 3: Column selection for '{retailer_id}': "
                f"{len(matched_common)} common, {len(matched_this_retailer)} this-retailer, "
                f"{len(matched_non_retailer)} non-retailer, {len(dropped_columns)} dropped",
                retailer_id=retailer_id,
            )
            
            if matched_non_retailer:
                _transfer_debug(
                    status_logger,
                    "retailer_split",
                    f"STEP 3: Preserved non-retailer columns: {matched_non_retailer}",
                    retailer_id=retailer_id,
                )
            
            # Create DataFrame with selected columns
            retailer_df = df[retailer_cols].copy()
            
            # CRITICAL VERIFICATION: Verify columns exist in DataFrame after selection
            missing_cols = [col for col in retailer_cols if col not in retailer_df.columns]
            if missing_cols:
                _transfer_error(
                    status_logger,
                    f"CRITICAL ERROR: Columns selected but missing in DataFrame: {missing_cols}",
                    reason="column_selection_invariant",
                    retailer_id=retailer_id,
                    missing_cols=missing_cols,
                )
            
            # TASK 4: Standardize ANY *_sales column to 'Sales' using normalized matching
            # This ensures consistent schema across all retailer files
            # Pattern: {retailer}_sales -> Sales (e.g., walmart_sales, amazon_sales -> Sales)
            # Matches any column ending in _sales (case-insensitive, robust against BOM/whitespace)
            sales_renamed = False
            for col in retailer_df.columns:
                col_normalized = normalize_header_for_matching(col)
                # Check if column ends with _sales (matches any retailer)
                if col_normalized.endswith('_sales') and col_normalized != 'sales':
                    # Only rename if 'Sales' column doesn't already exist
                    if 'Sales' not in retailer_df.columns:
                        retailer_df = retailer_df.rename(columns={col: 'Sales'})
                        _transfer_debug(
                            status_logger,
                            "retailer_split",
                            f"Renamed '{col}' to 'Sales' for retailer {retailer_id}",
                            retailer_id=retailer_id,
                            column=col,
                        )
                        sales_renamed = True
                    else:
                        # If 'Sales' already exists, drop the retailer-specific column
                        retailer_df = retailer_df.drop(columns=[col])
                        _transfer_debug(
                            status_logger,
                            "retailer_split",
                            f"Dropped duplicate '{col}' column (Sales already exists) for retailer {retailer_id}",
                            retailer_id=retailer_id,
                            column=col,
                        )
                    break
            
            # Verify 'Sales' column exists in output
            if 'Sales' not in retailer_df.columns:
                _transfer_warning(
                    status_logger,
                    f"No 'Sales' column found in output for retailer {retailer_id}. Columns: {list(retailer_df.columns)}",
                    retailer_id=retailer_id,
                    debug_event="sales_column",
                )
            
            # TASK 6: Use normalized source columns captured before retailer loop
            # This was captured after date normalization but before retailer column selection
            normalized_source_cols = normalized_source_cols_for_drift
            
            # Reorder columns to match SOT schema
            retailer_df = reorder_columns_sot(
                retailer_df, retailer_id, status_logger=status_logger
            )
            
            # DIAG[7]: Log ACTUAL columns being written (after reorder) - critical for debugging
            _transfer_debug(
                status_logger,
                "retailer_split",
                f"DIAG[7]: FINAL columns for '{retailer_id}' ({len(retailer_df.columns)}): {retailer_df.columns.tolist()}",
                retailer_id=retailer_id,
            )
            
            # ============================================================================
            # TASK 6: SCHEMA DRIFT DETECTION (SOT vs source + extra columns in output)
            # ============================================================================
            sot_cols_normalized = {
                normalize_header_for_matching(col) for col in UNIVERSAL_SOT_COLS
            }

            # Normalize output columns (after reorder)
            normalized_output_cols = {
                normalize_header_for_matching(col) for col in retailer_df.columns
            }

            # SOT columns absent from normalized source (no column injection in pipeline)
            # Sales: source often has {retailer}_sales; TASK 4 renames to Sales — treat *_sales as present
            has_retailer_sales = any(
                col.endswith("_sales") and col != "sales"
                for col in normalized_source_cols
            )
            missing_in_source: List[str] = []
            for sot_col in UNIVERSAL_SOT_COLS:
                sot_col_normalized = normalize_header_for_matching(sot_col)
                if sot_col_normalized not in normalized_source_cols:
                    if sot_col_normalized == "sales" and has_retailer_sales:
                        continue
                    missing_in_source.append(sot_col)

            # Extra: not in SOT and not a *spend / *impressions / *impression channel-style column
            extra_cols = [
                col
                for col in retailer_df.columns
                if normalize_header_for_matching(col) not in sot_cols_normalized
                and not normalize_header_for_matching(col).endswith("_spend")
                and not normalize_header_for_matching(col).endswith("_impressions")
                and not normalize_header_for_matching(col).endswith("_impression")
            ]

            # Send Slack alert if schema drift detected
            if missing_in_source or extra_cols:
                _transfer_warning(
                    status_logger,
                    f"SCHEMA DRIFT:\n"
                    f"Missing in source: {missing_in_source},\n"
                    f"Extra columns: {extra_cols}",
                    client_id=client_id,
                    brand_name=brand_name,
                    retailer_id=retailer_id,
                    debug_event="schema_drift",
                )
                send_schema_drift_alert(
                    client_id=client_id,
                    brand_name=brand_name,
                    retailer_id=retailer_id,
                    missing_columns=missing_in_source,
                    missing_in_source=missing_in_source,
                    extra_columns=extra_cols,
                    filename=f"{client_id}_{brand_name}_{retailer_id}",
                    status_logger=status_logger,
                )
            
            # Verify critical columns exist after reordering (legacy check)
            critical_cols = ['Sales']
            missing_critical = [c for c in critical_cols if normalize_header_for_matching(c) not in normalized_output_cols]
            if missing_critical:
                _transfer_error(
                    status_logger,
                    f"CRITICAL: Missing expected columns after reorder for {retailer_id}: {missing_critical}",
                    reason="missing_critical_after_reorder",
                    retailer_id=retailer_id,
                    missing_critical=missing_critical,
                )
            
            # STEP 8 & 9: Send column drop alert if columns were dropped
            if dropped_columns:
                _transfer_warning(
                    status_logger,
                    f"STEP 8: {len(dropped_columns)} columns dropped for '{retailer_id}': {[dc['column'] for dc in dropped_columns]}",
                    retailer_id=retailer_id,
                    debug_event="column_drop",
                )
                
                # Send Slack alert
                if filename:
                    send_column_drop_alert(
                        client_id=client_id,
                        brand_name=brand_name,
                        retailer_id=retailer_id,
                        filename=filename,
                        dropped_columns=dropped_columns,
                        total_source_columns=len(df.columns),
                        total_output_columns=len(retailer_df.columns),
                        status_logger=status_logger,
                    )
            
            # Data integrity validation: Ensure minimum column count to prevent data loss
            MIN_RETAILER_COLUMNS = 5  # Date, Sales, + at least 3 metrics
            if len(retailer_df.columns) < MIN_RETAILER_COLUMNS:
                _transfer_error(
                    status_logger,
                    f"CRITICAL: Output for {retailer_id} has only {len(retailer_df.columns)} columns. "
                    f"Expected at least {MIN_RETAILER_COLUMNS}. Columns: {list(retailer_df.columns)}. "
                    f"Aborting to prevent data loss.",
                    reason="min_retailer_columns",
                    retailer_id=retailer_id,
                    column_count=len(retailer_df.columns),
                )
                continue  # Skip this retailer to prevent corrupted output
            
            # Convert to CSV bytes
            output_buffer = io.BytesIO()
            retailer_df.to_csv(output_buffer, index=False)
            csv_bytes = output_buffer.getvalue()
            
            result[retailer_id] = csv_bytes
            _transfer_info(
                status_logger,
                f"Created retailer file for {retailer_id}: {len(retailer_df)} rows, {len(retailer_df.columns)} columns",
                retailer_id=retailer_id,
                row_count=len(retailer_df),
                column_count=len(retailer_df.columns),
                debug_event="retailer_split",
            )
        
        return result
        
    except Exception as e:
        _transfer_exception(status_logger, "Error processing with pandas", e, debug_event="process_with_pandas")
        return {}


def get_vip_bucket_name(client_id: str) -> str:
    """
    Compute VIP bucket name: mmm-{env}-data-{client_id}
    
    Uses standard naming convention enforced across all clients.
    
    Args:
        client_id: Client identifier
    
    Returns:
        S3 bucket name in format: mmm-{env}-data-{client_id}
    """
    # Use config-based naming if available
    if CONFIG_AVAILABLE:
        return config.get_vip_bucket_name(client_id)
    
    # Default pattern: mmm-{env}-data-{client_id}
    return f"{VIP_BUCKET_PREFIX}-{client_id}"


def upload_to_client_bucket(
    file_data: bytes,
    source_key: str,
    client_id: str,
    brand_name: str,
    retailer_id: str,
    status_logger: Optional[Any] = None,
) -> str:
    """
    Upload processed file to VIP client bucket.
    New structure: preprocessed/{brand_name}/{retailer_id}/{retailer_id}.csv
    
    Uses original brand_name (brand_id) to match DynamoDB schema and S3 path expectations.
    Note: brand_name parameter contains original brand_id (e.g., 'bella-US'), not normalized.
    
    Args:
        file_data: Processed CSV file data as bytes
        source_key: Source S3 key in tracer bucket
        client_id: Client identifier
        brand_name: Brand identifier (brand_id from event, e.g., 'bella-US')
        retailer_id: Retailer identifier
    """
    # Use original brand_name for S3 path (preserves 'bella-US' format from DynamoDB)
    # Do NOT normalize - destination paths must match brand_id exactly
    s3_brand_path = brand_name
    
    # Compute VIP bucket name dynamically (standard naming: mmm-{env}-data-{client_id})
    vip_bucket = get_vip_bucket_name(client_id)
    
    # New VIP bucket structure: preprocessed/brand_name/retailer_id/retailer_id.csv
    # Use retailer_id (lowercase) as filename to match folder name
    # Use config for destination path template if available
    if CONFIG_AVAILABLE:
        dest_key = config.get_destination_path(s3_brand_path, retailer_id)
    else:
        dest_key = f"preprocessed/{s3_brand_path}/{retailer_id}/{retailer_id}.csv"
    
    try:
        _transfer_debug(
            status_logger,
            "s3_transfer",
            f"Uploading to: s3://{vip_bucket}/{dest_key}",
            file_key=dest_key,
            vip_bucket=vip_bucket,
            source_key=source_key,
            sdk_operation="put_object",
        )
        s3_client.put_object(
            Bucket=vip_bucket,
            Key=dest_key,
            Body=file_data,
            Metadata={
                'client_id': client_id,
                'brand_name': brand_name,
                'retailer_id': retailer_id,
                'source_key': source_key,
                'upload_timestamp': datetime.now(timezone.utc).isoformat()
            }
        )
        _transfer_info(
            status_logger,
            f"Uploaded to VIP bucket: s3://{vip_bucket}/{dest_key} (brand={s3_brand_path})",
            dest_key=dest_key,
            vip_bucket=vip_bucket,
            s3_brand_path=s3_brand_path,
            sdk_operation="put_object",
        )
        return dest_key
    except ClientError as e:
        _transfer_error(
            status_logger,
            f"✗ Error uploading to {vip_bucket}: {str(e)}",
            reason="s3_put_object_failed",
            vip_bucket=vip_bucket,
            error=str(e),
        )
        raise


def get_available_retailers(
    client_id: str, brand_id: str, status_logger: Optional[Any] = None
) -> List[Dict[str, str]]:
    """
    Get available retailers for client/brand from pipeline-infos table.
    Returns list of dicts with retailer_id and retailer_name.
    
    Uses scan with filter (table may not have GSI) to find active retailers
    for the specified client and brand.
    
    Args:
        client_id: Client identifier
        brand_id: Brand identifier (e.g., 'bella-US') - compared against brand_id field in DynamoDB
    """
    retailers = []
    try:
        # Use module table handle so tests (and Lambda) share the same patched resource as transfer_logs.
        table = pipeline_info_table

        # Scan with filter for client_id
        # Note: Scan is used because the table may not have the ActiveClientsIndex GSI
        # For production, consider adding the GSI to improve performance
        response = table.scan(
            FilterExpression='client_id = :cid',
            ExpressionAttributeValues={
                ':cid': client_id
            }
        )
        
        # Handle pagination for large tables
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='client_id = :cid',
                ExpressionAttributeValues={
                    ':cid': client_id
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        for item in items:
            # Filter by brand_id in Python (case-insensitive) - brand_id is the canonical identifier
            if item.get('brand_id', '').lower() == brand_id.lower():
                # Check if active (is_active can be 1, "1", True, or "ACTIVE")
                is_active = item.get('is_active')
                if is_active in [1, '1', True, 'ACTIVE', 'active']:
                    retailers.append({
                        'retailer_id': item.get('retailer_id', ''),
                        'retailer_name': item.get('retailer_name', '')
                    })
    except Exception as e:
        _transfer_warning(
            status_logger,
            f"Error getting retailers from pipeline-infos: {e}",
            client_id=client_id,
            brand_id=brand_id,
            error=str(e),
        )
    
    return retailers


def process_file(
    file_key: str,
    client_id: str,
    brand_name: str,
    retailer_id: str,
    execution_id: str,
    status_logger: Optional[Any] = None,
) -> Tuple[bool, str, int]:
    """
    Process single file: download, validate, process with pandas, upload.
    
    Uses pandas to:
    - Rename "week" columns to "Date" and reformat to YYYY-MM-DD
    - Detect and split into retailer-specific files
    - Upload to preprocessed/{brand}/{retailer}/{retailer}.csv
    """
    try:
        _transfer_info(
            status_logger,
            f"Processing: {file_key}",
            file_key=file_key,
            client_id=client_id,
            retailer_id=retailer_id,
        )
        raw_data = download_from_tracer(file_key, status_logger=status_logger)
        
        # Basic content validation
        filename = file_key.split('/')[-1]
        
        # Check for empty file (only reject 0-byte files, not small files)
        if len(raw_data) == 0:
            _transfer_warning(
                status_logger,
                f"Skipping empty file {file_key} (0 bytes)",
                file_key=file_key,
            )
            return False, "", 0
        
        if not filename.lower().endswith('.csv'):
            _transfer_warning(
                status_logger,
                f"Skipping non-CSV file {file_key}",
                file_key=file_key,
            )
            return False, "", 0
        
        # Validate file content (checks for empty data, structure, etc.)
        is_valid, validation_errors = validate_file_content(
            raw_data, filename, retailer_id, status_logger=status_logger
        )
        if not is_valid:
            _transfer_warning(
                status_logger,
                f"Skipping invalid file {file_key}: {', '.join(validation_errors)}",
                file_key=file_key,
                validation_errors=validation_errors,
            )
            return False, "", 0
        
        # Get available retailers from pipeline-infos (central source of truth)
        # Use brand_name parameter (which is actually brand_id from event) for consistency
        available_retailers = get_available_retailers(
            client_id, brand_name, status_logger=status_logger
        )
        
        # If no retailers found, create a default one from context
        if not available_retailers:
            available_retailers = [{'retailer_id': retailer_id, 'retailer_name': ''}]
        else:
            # Ensure the requested retailer_id is in the list
            retailer_ids = [r.get('retailer_id', '').lower() for r in available_retailers]
            if retailer_id.lower() not in retailer_ids:
                available_retailers.append({'retailer_id': retailer_id, 'retailer_name': ''})
        
        # Transform data to standardized format before processing
        transformed_data, transform_metadata = transform_data(
            raw_data, file_key, retailer_id, status_logger=status_logger
        )
        
        # Log transformation results
        if transform_metadata.get('skipped'):
            _transfer_info(
                status_logger,
                f"Data transformation skipped for {filename}: {transform_metadata.get('reason', 'unknown')}",
                filename=filename,
                reason=transform_metadata.get("reason", "unknown"),
            )
        elif transform_metadata.get('errors'):
            _transfer_warning(
                status_logger,
                f"Data transformation had errors for {filename}, using original data",
                filename=filename,
            )
        else:
            _transfer_info(
                status_logger,
                f"Data transformation completed for {filename}",
                filename=filename,
            )
        
        # Process with pandas: split into retailer files, transform dates
        if not PANDAS_AVAILABLE:
            _transfer_error(
                status_logger,
                "Pandas not available - cannot process file",
                reason="pandas_unavailable",
                file_key=file_key,
            )
            return False, "", 0
        
        # TASK 3: Pass event_retailer_id to prevent fan-out
        # Pass filename for column drop alerting
        retailer_files = process_with_pandas(
            transformed_data,
            client_id,
            brand_name,
            available_retailers,
            event_retailer_id=retailer_id,
            filename=file_key,
            status_logger=status_logger,
        )
        
        if not retailer_files:
            _transfer_warning(
                status_logger,
                f"No retailer files created from {file_key}",
                file_key=file_key,
            )
            return False, "", 0
        
        # TASK 3: Safety belt - only upload event_retailer_id output (prevent fan-out)
        detected_retailers = list(retailer_files.keys())
        event_retailer_id_lower = retailer_id.lower()
        
        # Log warning if multiple retailers were processed (shouldn't happen with Task 3 fix)
        if len(detected_retailers) > 1:
            _transfer_warning(
                status_logger,
                f"TASK 3 SAFETY: Multiple retailers detected in processing output: {detected_retailers}. "
                f"Event retailer: {retailer_id}. Will only upload event retailer output to prevent fan-out.",
                file_key=file_key,
                detected_retailers=detected_retailers,
                retailer_id=retailer_id,
            )
        
        # Only upload event_retailer_id output
        if event_retailer_id_lower not in retailer_files:
            _transfer_error(
                status_logger,
                f"TASK 3 ERROR: Event retailer '{retailer_id}' not found in processed output. "
                f"Available retailers: {detected_retailers}. Cannot upload.",
                reason="event_retailer_missing",
                file_key=file_key,
                retailer_id=retailer_id,
                detected_retailers=detected_retailers,
            )
            return False, "", 0
        
        retailer_data = retailer_files[event_retailer_id_lower]
        
        # Upload to VIP bucket for event retailer only
        dest_key = upload_to_client_bucket(
            retailer_data,
            file_key,
            client_id,
            brand_name,
            retailer_id,
            status_logger=status_logger,
        )
        
        # Count rows
        try:
            df = pd.read_csv(io.BytesIO(retailer_data))
            total_records = len(df)
        except Exception:
            total_records = max(1, len(retailer_data) // 100)

        # ME-5401: Spend Regime Shift detection and actioning
        try:
            if PANDAS_AVAILABLE:
                if 'df' not in locals():
                    df = pd.read_csv(io.BytesIO(retailer_data))
                drift_profile = load_drift_profile(status_logger=status_logger)
                if drift_profile:
                    spend_regime_result = detect_spend_regime_shift(df, drift_profile)
                    if spend_regime_result.get('detected'):
                        _transfer_warning(
                            status_logger,
                            f"ME-5401 spend regime shift detected for {client_id}/{brand_name}/{retailer_id}: "
                            f"severity={spend_regime_result.get('severity')}, "
                            f"channels={[a.get('channel') for a in spend_regime_result.get('anomalies', [])]}",
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            file_key=file_key,
                            severity=spend_regime_result.get("severity"),
                            alert_type="spend_regime_shift",
                        )
                        handle_spend_regime_shift_actions(
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            filename=file_key,
                            detection_result=spend_regime_result,
                            status_logger=status_logger,
                        )
        except Exception as drift_error:
            _transfer_warning(
                status_logger,
                f"ME-5401 spend regime shift evaluation failed for {file_key}: {drift_error}",
                file_key=file_key,
                error=str(drift_error),
            )

        # ME-5402: KPI Behavior Break detection and actioning
        try:
            if PANDAS_AVAILABLE:
                if 'df' not in locals():
                    df = pd.read_csv(io.BytesIO(retailer_data))

                drift_profile = load_drift_profile(status_logger=status_logger)
                if drift_profile:
                    kpi_behavior_result = detect_kpi_behavior_break(df, drift_profile)
                    if kpi_behavior_result.get('detected'):
                        _transfer_warning(
                            status_logger,
                            f"ME-5402 KPI behavior break detected for {client_id}/{brand_name}/{retailer_id}: "
                            f"severity={kpi_behavior_result.get('severity')}, "
                            f"opposite_direction={kpi_behavior_result.get('opposite_direction')}",
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            file_key=file_key,
                            severity=kpi_behavior_result.get("severity"),
                            alert_type="kpi_behavior_break",
                        )
                        handle_kpi_behavior_break_actions(
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            filename=file_key,
                            detection_result=kpi_behavior_result,
                            status_logger=status_logger,
                        )
        except Exception as drift_error:
            _transfer_warning(
                status_logger,
                f"ME-5402 KPI behavior break evaluation failed for {file_key}: {drift_error}",
                file_key=file_key,
                error=str(drift_error),
            )

        # ME-5806a: Channel Activation/Deactivation detection
        try:
            if PANDAS_AVAILABLE:
                if 'df' not in locals():
                    df = pd.read_csv(io.BytesIO(retailer_data))
                drift_profile = load_drift_profile(status_logger=status_logger)
                if drift_profile:
                    channel_ad_result = detect_channel_activation_deactivation(df, drift_profile)
                    if channel_ad_result.get('detected'):
                        _transfer_warning(
                            status_logger,
                            f"ME-5806a Channel activation/deactivation detected for {client_id}/{brand_name}/{retailer_id}: "
                            f"severity={channel_ad_result.get('severity')}",
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            file_key=file_key,
                            severity=channel_ad_result.get('severity'),
                            alert_type="channel_activation_deactivation",
                        )
                        handle_channel_activation_deactivation_actions(
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            filename=file_key,
                            detection_result=channel_ad_result,
                            status_logger=status_logger,
                        )
        except Exception as drift_error:
            _transfer_warning(
                status_logger,
                f"ME-5806a Channel activation/deactivation evaluation failed for {file_key}: {drift_error}",
                file_key=file_key,
                error=str(drift_error),
            )

        # ME-5806b: Spend Mix Reallocation detection
        try:
            if PANDAS_AVAILABLE:
                if 'df' not in locals():
                    df = pd.read_csv(io.BytesIO(retailer_data))
                drift_profile = load_drift_profile(status_logger=status_logger)
                if drift_profile:
                    mix_result = detect_spend_mix_reallocation(df, drift_profile)
                    if mix_result.get('detected'):
                        _transfer_warning(
                            status_logger,
                            f"ME-5806b Spend mix reallocation detected for {client_id}/{brand_name}/{retailer_id}: "
                            f"severity={mix_result.get('severity')}",
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            file_key=file_key,
                            severity=mix_result.get('severity'),
                            alert_type="spend_mix_reallocation",
                        )
                        handle_spend_mix_reallocation_actions(
                            client_id=client_id,
                            brand_name=brand_name,
                            retailer_id=retailer_id,
                            filename=file_key,
                            detection_result=mix_result,
                            status_logger=status_logger,
                        )
        except Exception as drift_error:
            _transfer_warning(
                status_logger,
                f"ME-5806b Spend mix reallocation evaluation failed for {file_key}: {drift_error}",
                file_key=file_key,
                error=str(drift_error),
            )

        _transfer_info(
            status_logger,
            f"✓ Uploaded: {file_key} -> {dest_key} (event retailer: {retailer_id})",
            file_key=file_key,
            dest_key=dest_key,
            retailer_id=retailer_id,
            client_id=client_id,
        )
        _transfer_info(
            status_logger,
            f"✓ Processed: {file_key} -> {retailer_id} ({total_records} rows)",
            file_key=file_key,
            retailer_id=retailer_id,
            records_processed=total_records,
            client_id=client_id,
        )
        return True, dest_key, total_records
    except Exception as e:
        _transfer_exception(
            status_logger,
            f"✗ Error processing {file_key}: {str(e)}",
            e,
            file_key=file_key,
            client_id=client_id,
            retailer_id=retailer_id,
        )
        return False, "", 0


def write_transfer_log(
    execution_id: str,
    client_id: str,
    brand_name: str,
    retailer_id: str,
    files_processed: int,
    records_processed: int,
    status: str,
    error_message: Optional[str] = None,
    status_logger: Optional[Any] = None,
) -> None:
    """Write transfer log to DynamoDB.
    
    Schema: Partition Key = client_id, Sort Key = timestamp
    """
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        item = {
            'client_id': client_id,  # Partition Key
            'timestamp': timestamp,  # Sort Key
            'transfer_id': f"{client_id}#{timestamp}",  # Attribute (not part of key)
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'execution_id': execution_id,
            'status': status,
            'files_processed': files_processed,
            'records_processed': records_processed
        }
        if error_message:
            item['error_message'] = error_message
        transfer_logs_table.put_item(Item=item)
        _transfer_info(
            status_logger,
            "✓ Transfer log written",
            operation="transfer_log",
            transfer_logs_table=TRANSFER_LOGS_TABLE,
            client_id=client_id,
            transfer_status=status,
        )
    except ClientError as e:
        _transfer_error(
            status_logger,
            f"✗ Error writing transfer log: {str(e)}",
            reason=str(e),
            operation="transfer_log",
            transfer_logs_table=TRANSFER_LOGS_TABLE,
        )


def write_audit_log_to_s3(
    execution_id: str,
    client_id: str,
    brand_name: str,
    retailer_id: str,
    files_processed: int,
    records_processed: int,
    status: str,
    duration_ms: int,
    source_files: List[str],
    destination_files: List[str],
    errors: List[str] = None,
    status_logger: Optional[Any] = None,
) -> None:
    """Write audit log to S3."""
    try:
        now = datetime.now(timezone.utc)
        path_key = (
            f"data-transfer/{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}/"
            f"{client_id}/{brand_name}/{retailer_id}/{execution_id}.json"
        )
        
        audit_log = {
            'execution_id': execution_id,
            'lambda_name': 'mmm-data-transfer-lambda',
            'client_id': client_id,
            'brand_name': brand_name,
            'retailer_id': retailer_id,
            'operation': 'DATA_TRANSFER',
            'status': status,
            'duration_ms': duration_ms,
            'files_processed': files_processed,
            'records_processed': records_processed,
            'source_files': source_files,
            'destination_files': destination_files,
            'errors': errors or [],
            'timestamp': now.isoformat()
        }
        
        s3_client.put_object(
            Bucket=AUDIT_LOGS_BUCKET,
            Key=path_key,
            Body=json.dumps(audit_log, indent=2),
            ContentType='application/json'
        )
        _transfer_info(
            status_logger,
            "✓ Audit log written to S3",
            operation="audit_s3",
            audit_logs_bucket=AUDIT_LOGS_BUCKET,
            audit_key=path_key,
            client_id=client_id,
            transfer_status=status,
        )
    except ClientError as e:
        _transfer_error(
            status_logger,
            f"✗ Error writing audit log: {str(e)}",
            reason=str(e),
            operation="audit_s3",
            audit_logs_bucket=AUDIT_LOGS_BUCKET,
        )


def update_pipeline_info_after_transfer(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    files_processed: int,
    records_processed: Optional[int] = None,
    status_logger: Optional[Any] = None,
) -> None:
    """
    Update pipeline_info table after successful data transfer.
    
    Marks data as updated and schedules next update for tomorrow (daily schedule).
    Optionally persists last_transfer_row_count for row-count sanity check on next run.
    """
    if not PipelineInfoHelper:
        _transfer_warning(
            status_logger,
            "PipelineInfoHelper not available, skipping pipeline_info update",
            client_id=client_id,
            retailer_id=retailer_id,
        )
        return

    try:
        # Don't normalize here - build_sort_key() in mark_data_updated() normalizes internally
        normalized_brand = normalize_brand_name(brand_name)
        helper = PipelineInfoHelper(ENVIRONMENT, status_logger=status_logger)
        
        # Calculate next data update (tomorrow for daily schedule)
        next_update_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')
        
        _transfer_info(
            status_logger,
            f"Updating pipeline_info: {client_id}/{normalized_brand}#{retailer_id}",
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            normalized_brand=normalized_brand,
        )
        helper.mark_data_updated(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            next_data_update=next_update_date
        )
        if records_processed is not None:
            helper.update_pipeline_info(
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
                updates={'last_transfer_row_count': int(records_processed)},
            )
            _transfer_debug(
                status_logger,
                "metadata_write",
                f"Updated last_transfer_row_count={records_processed}",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
                last_transfer_row_count=int(records_processed),
            )
        _transfer_info(
            status_logger,
            f"✓ Pipeline info updated: next_update={next_update_date}",
            client_id=client_id,
            retailer_id=retailer_id,
            next_data_update=next_update_date,
        )
    except Exception as e:
        _transfer_error(
            status_logger,
            f"✗ Error updating pipeline_info: {str(e)}",
            reason=str(e),
            client_id=client_id,
            retailer_id=retailer_id,
        )
        # Don't raise - update is informational


INGESTION_FAILURE_REASON_MAX_LEN = 512


def _ingestion_timestamp_utc_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_failure_reason(reason: str) -> str:
    """Redact common sensitive patterns from failure text for logs."""
    if not reason:
        return ""
    text = str(reason)
    text = re.sub(r"\b\d{12}\b", "<acct>", text)
    text = re.sub(r"s3://[^\s]+", "s3://<redacted>", text)
    return text


def _summarize_errors_for_ingestion(errors: List[str]) -> str:
    if not errors:
        return ""
    joined = "; ".join(errors[:5])
    if len(errors) > 5:
        joined += "; ..."
    return joined[:INGESTION_FAILURE_REASON_MAX_LEN]


def _resolve_retailer_name(
    client_id: str,
    brand_name: Optional[str],
    retailer_id: str,
    pipeline_info: Optional[Dict[str, Any]],
) -> str:
    if pipeline_info and pipeline_info.get("retailer_name"):
        return str(pipeline_info["retailer_name"])
    return ""


def compute_ingestion_status_after_loop(
    files_processed: int,
    errors: List[str],
) -> Tuple[str, Optional[str]]:
    """Status for Datadog observability log only (not persisted to DynamoDB)."""
    if files_processed >= 1 and not errors:
        return "SUCCESS", None
    if files_processed >= 1 and errors:
        return "PARTIAL", _summarize_errors_for_ingestion(errors)
    if files_processed == 0 and errors:
        return "FAILED", _summarize_errors_for_ingestion(errors)
    return "NO_FILES", "no_files_after_filtering"


def emit_ingestion_observability_log(
    execution_id: str,
    client_id: Optional[str],
    brand_name: Optional[str],
    retailer_id: Optional[str],
    country: str,
    status: str,
    files_processed: int,
    records_processed: int,
    failure_reason: Optional[str] = None,
    retailer_name: str = "",
    *,
    use_case: str = "data_ingestion_transfer",
    data_source_provider: str = "Tracer",
    data_source_bucket: str = "",
    _mikaura_status_logger: Optional[Any] = None,
) -> None:
    """Emit one single-line JSON object to stdout for Datadog.

    Emits both ``ingestion`` (backward compat for existing dashboards) and
    ``mikaura`` (new pipeline-agnostic facets) keys in a single JSON line.
    """
    ingestion: Dict[str, Any] = {
        "evt": "transfer",
        "execution_id": execution_id or "",
        "client_id": client_id or "",
        "brand_name": brand_name or "",
        "retailer_id": retailer_id or "",
        "retailer_name": retailer_name or "",
        "country": country or "unknown",
        "status": status,
        "files_processed": int(files_processed),
        "records_processed": int(records_processed),
        "timestamp": _ingestion_timestamp_utc_z(),
        "use_case": use_case,
        "data_source_provider": data_source_provider,
        "data_source_bucket": data_source_bucket or "",
    }
    if failure_reason:
        fr = sanitize_failure_reason(failure_reason)[:INGESTION_FAILURE_REASON_MAX_LEN]
        if fr:
            ingestion["failure_reason"] = fr

    combined: Dict[str, Any] = {"ingestion": ingestion}

    if _mikaura_status_logger and _MIKAURA_AVAILABLE:
        if status == "SUCCESS":
            mikaura_status = "success"
        elif status == "FAILED":
            mikaura_status = "failed"
        elif status == "NO_FILES":
            mikaura_status = "no_file"
        else:
            mikaura_status = "info"
        if mikaura_status not in INGESTION_MIKAURA_ALLOWED_STATUSES:
            mikaura_status = "info"
        mikaura_entry = _mikaura_status_logger._build_entry(
            mikaura_status,
            f"Data transfer {status.lower()}",
            "ERROR" if mikaura_status == "failed" else "INFO",
            files_processed=int(files_processed),
            records_processed=int(records_processed),
        )
        if failure_reason:
            mikaura_entry["reason"] = ingestion.get("failure_reason", str(failure_reason))
        combined["mikaura"] = mikaura_entry

    print(json.dumps(combined, default=str, separators=(",", ":")), flush=True)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda 2: Data Transfer Handler
    
    Process files from Tracer to Client buckets.
    """
    # Use aws_request_id for Python 3.12 compatibility (request_id is deprecated)
    execution_id = context.aws_request_id
    start_time = datetime.now(timezone.utc)
    
    client_id = event.get('client_id')
    brand_id = event.get('brand_id')  # Extract brand_id for file matching
    brand_name = event.get('brand_name')  # Keep for metadata/display
    retailer_id = event.get('retailer_id')
    # Step Function passes execution_id via Parameters ($$.Execution.Id); fallback to Lambda request ID if missing
    step_function_execution_id = event.get('execution_id') or context.aws_request_id or 'unknown'
    _raw_country = event.get("country")
    obs_country = (
        _raw_country.strip()
        if isinstance(_raw_country, str) and _raw_country.strip()
        else "unknown"
    )

    # National retailer is now supported with separate processing logic
    # The process_with_pandas function will detect National files early and process them
    # separately to prevent fan-out and drop retailer-specific columns appropriately
    
    # Use brand_id for file matching and DynamoDB lookups (normalized)
    # Fallback to brand_name if brand_id not available (backward compatibility)
    brand_for_matching = brand_id if brand_id else brand_name
    normalized_brand = normalize_brand_name(brand_for_matching) if brand_for_matching else None

    # MikAura observability (pipeline-agnostic)
    _mikaura_sl = None
    _mikaura_ml = None
    if _MIKAURA_AVAILABLE:
        _mikaura_config = MikAuraObservabilityConfig(
            context={
                "pipeline_context": "Data Ingestion Pipeline",
                "client_name": client_id or "unknown",
                "brand_name": brand_name or "unknown",
                "retailer_name": retailer_id or "unknown",
                "country": obs_country,
            },
            environment=ENVIRONMENT,
            correlation_id=step_function_execution_id,
            allowed_statuses=set(INGESTION_MIKAURA_ALLOWED_STATUSES),
        )
        _mikaura_sl = MikAuraStatusLogger.from_config(_mikaura_config, min_level=LOG_LEVEL)
        _mikaura_ml = MikAuraMetricLogger.from_config(_mikaura_config)

    def _run_data_transfer() -> Dict[str, Any]:
        nonlocal _mikaura_sl, _mikaura_ml
        _transfer_running(
            _mikaura_sl,
            f"Lambda 2 started: {client_id}/{normalized_brand}/{retailer_id} "
            f"(brand_id={brand_id}, brand_name={brand_name})",
            client_id=client_id or "",
            normalized_brand=normalized_brand or "",
            retailer_id=retailer_id or "",
            brand_id=brand_id or "",
            brand_name=brand_name or "",
        )

        files_processed = 0
        records_processed = 0
        source_files = []
        destination_files = []
        errors = []

        try:
            pipeline_info = None
            if not all([client_id, brand_for_matching, retailer_id]):
                raise ValueError("Missing required client parameters (client_id, brand_id/brand_name, retailer_id)")

            # Fetch pipeline_info to get last_data_updated for correlation
            min_last_modified = None
            last_data_updated = None
            if PipelineInfoHelper:
                try:
                    helper = PipelineInfoHelper(ENVIRONMENT, status_logger=_mikaura_sl)
                    # Use brand_id (not normalized_brand) - build_sort_key() normalizes internally
                    # brand_retailer_key format is {brand_id}#{retailer_id} per DynamoDB schema
                    pipeline_info = helper.get_pipeline_info(client_id, brand_for_matching, retailer_id)
                    if pipeline_info:
                        if pipeline_info.get('last_data_updated'):
                            last_data_updated = pipeline_info['last_data_updated']
                            min_last_modified = datetime.strptime(
                                last_data_updated, '%Y-%m-%d'
                            ).replace(tzinfo=timezone.utc)
                            _transfer_info(
                                _mikaura_sl,
                                f"Found last_data_updated: {last_data_updated} "
                                f"(will filter files older than this)",
                                last_data_updated=last_data_updated,
                                client_id=client_id,
                                retailer_id=retailer_id,
                            )
                except Exception as e:
                    _transfer_warning(
                        _mikaura_sl,
                        f"Could not fetch pipeline_info for correlation: {e}",
                        error=str(e),
                    )
            else:
                _transfer_failed(
                    _mikaura_sl,
                    "PipelineInfoHelper not available",
                    "pipeline-info table updates will fail",
                )

            # Align observability brand_name with DynamoDB (avoid raw Step Function casing in logs/metrics).
            if _MIKAURA_AVAILABLE and _mikaura_sl is not None and pipeline_info:
                db_brand = pipeline_info.get("brand_name")
                if isinstance(db_brand, str) and db_brand.strip():
                    bn = db_brand.strip()
                    _mikaura_sl = _mikaura_sl.derive(brand_name=bn)
                    if _mikaura_ml is not None:
                        _mikaura_ml = _mikaura_ml.derive(brand_name=bn)

            # Find files in Tracer (bulletproof discovery with 4-layer validation + last_data_updated filtering)
            # Use normalized_brand (from brand_id) for file matching
            tracer_files = find_tracer_files(
                client_id,
                normalized_brand,
                retailer_id,
                min_last_modified=min_last_modified,
                status_logger=_mikaura_sl,
            )

            if not tracer_files:
                _transfer_warning(
                    _mikaura_sl,
                    "No files found in Tracer",
                    client_id=client_id,
                    brand_name=normalized_brand,
                    retailer_id=retailer_id,
                    debug_event="tracer_discovery",
                )
                duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
                emit_ingestion_observability_log(
                    step_function_execution_id,
                    client_id,
                    normalized_brand,
                    retailer_id,
                    obs_country,
                    "NO_FILES",
                    0,
                    0,
                    "no_tracer_files",
                    retailer_name=_resolve_retailer_name(
                        client_id, brand_for_matching, retailer_id, pipeline_info
                    ),
                    data_source_bucket=TRACER_BUCKET,
                    _mikaura_status_logger=_mikaura_sl,
                )
                write_transfer_log(
                    execution_id,
                    client_id,
                    normalized_brand,
                    retailer_id,
                    0,
                    0,
                    "NO_FILES",
                    status_logger=_mikaura_sl,
                )
                write_audit_log_to_s3(
                    step_function_execution_id,
                    client_id,
                    normalized_brand,
                    retailer_id,
                    0,
                    0,
                    "NO_FILES",
                    duration_ms,
                    [],
                    [],
                    status_logger=_mikaura_sl,
                )
                if _mikaura_ml:
                    _mikaura_ml.increment("data_ingestion.transfer.outcome", extra_tags=["result:no_files"])
                elif _pipeline_metrics:
                    _pipeline_metrics.increment(
                        "data_ingestion.transfer.outcome",
                        tags=[f"env:{ENVIRONMENT}", "result:no_files"],
                    )
                    _pipeline_metrics.timing(
                        "data_ingestion.transfer.duration_ms",
                        float(duration_ms),
                        tags=[f"env:{ENVIRONMENT}", "result:no_files"],
                    )
                return {
                    'statusCode': 200,
                    'client_id': client_id,
                    'brand_name': normalized_brand,
                    'files_processed': 0,
                    'status': 'NO_FILES',
                    'execution_id': execution_id
                }

            # Process each file
            for file_key in tracer_files:
                try:
                    # Validate file freshness before processing
                    if last_data_updated:
                        try:
                            # Get file LastModified from S3
                            obj = s3_client.head_object(Bucket=TRACER_BUCKET, Key=file_key)
                            file_last_modified = obj['LastModified']

                            is_valid, reason = validate_file_freshness(
                                file_last_modified,
                                last_data_updated,
                                client_id,
                                normalized_brand,
                                retailer_id,
                                status_logger=_mikaura_sl,
                            )

                            if not is_valid:
                                _transfer_warning(
                                    _mikaura_sl,
                                    f"Skipping stale file {file_key}: {reason}",
                                    file_key=file_key,
                                    reason=reason,
                                    validation_stage="freshness",
                                )
                                errors.append(f"Skipped {file_key}: {reason}")
                                continue
                            _transfer_info(
                                _mikaura_sl,
                                f"File freshness validated: {reason}",
                                file_key=file_key,
                                reason=reason,
                                validation_stage="freshness",
                            )
                        except ClientError as e:
                            _transfer_warning(
                                _mikaura_sl,
                                f"Could not validate file freshness for {file_key}: {e}",
                                file_key=file_key,
                                error=str(e),
                            )
                            # Continue processing if validation fails (don't block on validation errors)

                    # Pass brand_for_matching (brand_id) not normalized_brand for consistency
                    success, dest_key, records = process_file(
                        file_key,
                        client_id,
                        brand_for_matching,
                        retailer_id,
                        step_function_execution_id,
                        status_logger=_mikaura_sl,
                    )
                    if success:
                        files_processed += 1
                        records_processed += records
                        vip_bucket = get_vip_bucket_name(client_id)
                        source_files.append(f"s3://{TRACER_BUCKET}/{file_key}")
                        destination_files.append(f"s3://{vip_bucket}/{dest_key}")
                    else:
                        errors.append(f"Failed: {file_key}")
                except Exception as e:
                    _transfer_warning(
                        _mikaura_sl,
                        f"Error processing {file_key}: {str(e)}",
                        file_key=file_key,
                        error=str(e),
                    )
                    errors.append(f"{file_key}: {str(e)}")

            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            ing_status, ing_failure_reason = compute_ingestion_status_after_loop(files_processed, errors)
            emit_ingestion_observability_log(
                step_function_execution_id,
                client_id,
                normalized_brand,
                retailer_id,
                obs_country,
                ing_status,
                files_processed,
                records_processed,
                ing_failure_reason,
                retailer_name=_resolve_retailer_name(
                    client_id, brand_for_matching, retailer_id, pipeline_info
                ),
                data_source_bucket=TRACER_BUCKET,
                _mikaura_status_logger=_mikaura_sl,
            )
            write_transfer_log(
                execution_id,
                client_id,
                normalized_brand,
                retailer_id,
                files_processed,
                records_processed,
                "SUCCESS",
                status_logger=_mikaura_sl,
            )
            write_audit_log_to_s3(
                step_function_execution_id,
                client_id,
                normalized_brand,
                retailer_id,
                files_processed,
                records_processed,
                "SUCCESS",
                duration_ms,
                source_files,
                destination_files,
                errors,
                status_logger=_mikaura_sl,
            )

            if files_processed > 0:
                update_pipeline_info_after_transfer(
                    client_id,
                    brand_for_matching,
                    retailer_id,
                    files_processed,
                    records_processed=records_processed,
                    status_logger=_mikaura_sl,
                )

            _transfer_info(
                _mikaura_sl,
                f"Lambda 2 completed: {files_processed} files, {records_processed} records",
                files_processed=files_processed,
                records_processed=records_processed,
                client_id=client_id,
                retailer_id=retailer_id,
            )

            if _mikaura_ml:
                _mikaura_ml.increment(
                    "data_ingestion.transfer.outcome",
                    extra_tags=[f"result:{ing_status.lower()}"],
                )
                _mikaura_ml.timing(
                    "data_ingestion.transfer.duration_ms",
                    float(duration_ms),
                    extra_tags=[f"result:{ing_status.lower()}"],
                )
            elif _pipeline_metrics:
                _pipeline_metrics.increment(
                    "data_ingestion.transfer.outcome",
                    tags=[f"env:{ENVIRONMENT}", f"result:{ing_status.lower()}"],
                )
                _pipeline_metrics.timing(
                    "data_ingestion.transfer.duration_ms",
                    float(duration_ms),
                    tags=[f"env:{ENVIRONMENT}", f"result:{ing_status.lower()}"],
                )

            return {
                'statusCode': 200,
                'client_id': client_id,
                'brand_name': normalized_brand,
                'retailer_id': retailer_id,
                'files_processed': files_processed,
                'records_processed': records_processed,
                'status': ing_status,
                'timestamp': get_current_timestamp(),
                'execution_id': execution_id
            }

        except Exception as e:
            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            _transfer_exception(_mikaura_sl, f"✗ Lambda 2 error: {str(e)}", e)

            # Use normalized_brand for consistency (falls back safely if brand_name/brand_id is None)
            brand_for_logs = normalized_brand if normalized_brand else (brand_name if brand_name else brand_id)
            brand_for_retailer_lookup = brand_for_matching if brand_for_matching else brand_for_logs

            write_transfer_log(
                execution_id,
                client_id,
                brand_for_logs,
                retailer_id,
                files_processed,
                records_processed,
                "FAILED",
                str(e),
                status_logger=_mikaura_sl,
            )
            write_audit_log_to_s3(
                step_function_execution_id,
                client_id,
                brand_for_logs,
                retailer_id,
                files_processed,
                records_processed,
                "FAILED",
                duration_ms,
                source_files,
                destination_files,
                [str(e)],
                status_logger=_mikaura_sl,
            )
            emit_ingestion_observability_log(
                step_function_execution_id,
                client_id,
                brand_for_logs,
                retailer_id,
                obs_country,
                "FAILED",
                files_processed,
                records_processed,
                str(e),
                retailer_name=_resolve_retailer_name(
                    client_id, brand_for_retailer_lookup, retailer_id, pipeline_info
                ),
                data_source_bucket=TRACER_BUCKET,
                _mikaura_status_logger=_mikaura_sl,
            )
            if _mikaura_ml:
                _mikaura_ml.increment(
                    "data_ingestion.transfer.outcome",
                    extra_tags=["result:failed"],
                )
                _mikaura_ml.timing(
                    "data_ingestion.transfer.duration_ms",
                    float(duration_ms),
                    extra_tags=["result:failed"],
                )
            elif _pipeline_metrics:
                _pipeline_metrics.increment(
                    "data_ingestion.transfer.outcome",
                    tags=[f"env:{ENVIRONMENT}", "result:failed"],
                )
                _pipeline_metrics.timing(
                    "data_ingestion.transfer.duration_ms",
                    float(duration_ms),
                    tags=[f"env:{ENVIRONMENT}", "result:failed"],
                )

            return {
                'statusCode': 500,
                'client_id': client_id,
                'brand_name': brand_for_logs,
                'retailer_id': retailer_id,
                'error': str(e),
                'status': 'FAILED',
                'execution_id': execution_id
            }

    return _run_data_transfer()
