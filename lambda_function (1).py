"""
Lambda Function: data_ingestion_slack

Purpose:
Send Slack notifications for data ingestion pipeline status updates.
Reports client/brand/retailer processing results to designated Slack channel.

Environment Variables Required:
- SLACK_WEBHOOK_URL: Slack webhook URL for the target channel
- LOG_LEVEL: MikAura min_level (default: INFO)

Observability:
- Operational lifecycle: MikAura JSON on stdout when import succeeds (`_slack_*` helpers + `status_logger`); no stdlib logging fallback.
- DEBUG: structured via `log_debug` with `debug_event=slack_handler` when MikAura is on.

Version: 1.0.0
Date: 2025-12-17
"""

import json
import urllib3
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

_slack_lambda_dir = os.path.dirname(os.path.abspath(__file__))
for _rel in ("src", os.path.join("..", "src")):
    _sp = os.path.normpath(os.path.join(_slack_lambda_dir, _rel))
    if os.path.isdir(_sp):
        sys.path.insert(0, _sp)
        break

try:
    from utils.mikaura_observability import (
        MikAuraObservabilityConfig,
        MikAuraStatusLogger,
        MikAuraMetricLogger,
    )
    _MIKAURA_AVAILABLE = True
except ImportError:
    _MIKAURA_AVAILABLE = False

try:
    from utils.metrics_utils import get_metrics_utils

    _slack_metrics = get_metrics_utils()
except ImportError:
    _slack_metrics = None

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

SLACK_ALLOWED_STATUSES = frozenset(
    {"running", "success", "failed", "warning", "info"}
)

# HTTP client for Slack requests
http = urllib3.PoolManager()

# Environment variables
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', '')
SLACK_MAX_RETRIES = max(1, int(os.environ.get('SLACK_MAX_RETRIES', '3')))
SLACK_RETRY_BASE_SECONDS = float(os.environ.get('SLACK_RETRY_BASE_SECONDS', '1.0'))


def _slack_info(status_logger: Optional[Any], message: str, **extra: Any) -> None:
    if status_logger:
        status_logger.log_info(message, force=True, **extra)


def _slack_warning(status_logger: Optional[Any], message: str, **extra: Any) -> None:
    if status_logger:
        status_logger.log_warning(message, **extra)


def _check_logger_required(message: str) -> None:
    """Write stderr when status_logger is unavailable; optionally hard-fail via env."""
    sys.stderr.write(f"[STATUS_LOGGER_UNAVAILABLE] {message}\n")
    if os.environ.get("FAIL_ON_MISSING_LOGGER", "false").lower() == "true":
        raise RuntimeError(f"Status logger was not available: {message}")


def _slack_error(
    status_logger: Optional[Any],
    message: str,
    reason: Optional[str] = None,
    **extra: Any,
) -> None:
    if status_logger:
        status_logger.log_error(message, reason=reason or "slack_delivery", **extra)
    else:
        _check_logger_required(message)


def _slack_exception(
    status_logger: Optional[Any],
    message: str,
    exc: BaseException,
    **extra: Any,
) -> None:
    if status_logger:
        status_logger.log_exception(message, exc, **extra)
    else:
        _check_logger_required(f"{message}: {exc}")


def _slack_debug(status_logger: Optional[Any], message: str, **extra: Any) -> None:
    if status_logger:
        status_logger.log_debug(message, debug_event="slack_handler", **extra)


def get_environment(context: Any = None) -> str:
    """
    Get environment from ENVIRONMENT variable or Lambda function name.
    
    Checks ENVIRONMENT environment variable first. If not set, infers from
    Lambda function name pattern: mmm_{env}_data_ingestion_slack or 
    mmm-data-ingestion-slack-{env}
    
    Args:
        context: Lambda context object (optional)
        
    Returns:
        Environment string in original case (qa, stg, prd, dev)
    """
    # Check ENVIRONMENT variable first
    env = os.environ.get('ENVIRONMENT', '').strip()
    if env:
        return env.lower()
    
    # Fallback: Infer from function name
    if context and hasattr(context, 'function_name'):
        function_name = context.function_name
        # Try pattern: mmm_{env}_data_ingestion_slack
        if '_data_ingestion_slack' in function_name:
            parts = function_name.split('_')
            if len(parts) >= 2:
                return parts[1]  # Extract env from mmm_{env}_data_ingestion_slack
        # Try pattern: mmm-data-ingestion-slack-{env}
        elif '-data-ingestion-slack-' in function_name:
            parts = function_name.split('-data-ingestion-slack-')
            if len(parts) == 2:
                return parts[1]
    
    # Default fallback
    return 'unknown'


def format_environment_badge(environment: str) -> str:
    """
    Format environment badge for Slack messages.
    
    Args:
        environment: Environment string (qa, stg, prd, dev)
        
    Returns:
        Formatted badge text with emoji
    """
    return f"🏷️ *Environment: {environment}*"


def get_current_timestamp() -> str:
    """Get current timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def format_slack_message(
    status: str,
    clients_processed: List[Dict[str, Any]],
    total_files: int,
    total_records: int,
    errors: List[str],
    execution_id: str,
    timestamp: str,
    environment: str
) -> Dict[str, Any]:
    """
    Format data ingestion results into a Slack Block Kit message.
    
    Args:
        status: Overall status (SUCCESS, PARTIAL, FAILED, NO_ACTIVE_CLIENTS)
        clients_processed: List of client processing results
        total_files: Total files processed
        total_records: Total records processed
        errors: List of error messages
        execution_id: Step Function execution ID
        timestamp: Execution timestamp
        environment: Environment name (dev, qa, stg, prd)
    
    Returns:
        Slack message payload with attachments
    """
    # Determine emoji and color based on status
    if status == 'SUCCESS':
        emoji = '✅'
        color = 'good'  # Green
        title = 'Data Ingestion Completed Successfully'
    elif status == 'PARTIAL':
        emoji = '⚠️'
        color = 'warning'  # Yellow
        title = 'Data Ingestion Completed with Warnings'
    elif status == 'NO_ACTIVE_CLIENTS':
        emoji = 'ℹ️'
        color = '#439FE0'  # Blue
        title = 'No Active Clients to Process'
    else:  # FAILED
        emoji = '❌'
        color = 'danger'  # Red
        title = 'Data Ingestion Failed'
    
    # Build client summary
    client_summaries = []
    for client in clients_processed[:10]:  # Limit to 10 clients
        client_id = client.get('client_id', 'unknown')
        brand_name = client.get('brand_name', 'unknown')
        retailer_id = client.get('retailer_id', 'unknown')
        files = client.get('files_processed', 0)
        records = client.get('records_processed', 0)
        client_status = client.get('status', 'unknown')
        
        # Normalize empty strings to 'unknown' for display
        if not client_id or (isinstance(client_id, str) and client_id.strip() == ''):
            client_id = 'unknown'
        if not brand_name or (isinstance(brand_name, str) and brand_name.strip() == ''):
            brand_name = 'unknown'
        if not retailer_id or (isinstance(retailer_id, str) and retailer_id.strip() == ''):
            retailer_id = 'unknown'
        
        status_emoji = '✓' if client_status == 'SUCCESS' else '✗'
        client_summaries.append(
            f"{status_emoji} `{client_id}` / {brand_name} / {retailer_id} - {files} files, {records} records"
        )
    
    if len(clients_processed) > 10:
        client_summaries.append(f"_...and {len(clients_processed) - 10} more clients_")
    
    # Build blocks
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{title}*"
            }
        }
    ]
    
    # Add environment badge
    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": format_environment_badge(environment)}
        ]
    })
    
    # Add summary stats
    if clients_processed or status == 'NO_ACTIVE_CLIENTS':
        # Count unique client IDs instead of total entries
        unique_clients = len(set(c.get('client_id', 'unknown') for c in clients_processed if c.get('client_id', 'unknown') != 'unknown'))
        clients_count = unique_clients if unique_clients > 0 else len(clients_processed)
        
        fields = [
            {"type": "mrkdwn", "text": f"*Clients Processed:*\n{clients_count}"},
            {"type": "mrkdwn", "text": f"*Total Files:*\n{total_files}"},
            {"type": "mrkdwn", "text": f"*Total Records:*\n{total_records:,}"}
        ]
        
        if errors:
            fields.append({"type": "mrkdwn", "text": f"*Errors:*\n{len(errors)}"})
        
        blocks.append({"type": "section", "fields": fields})
    
    # Add client details (if any)
    if client_summaries:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Client Details:*\n" + "\n".join(client_summaries)
            }
        })
    
    # Add error details (if any)
    if errors:
        blocks.append({"type": "divider"})
        error_text = "\n".join([f"• {err[:100]}" for err in errors[:5]])
        if len(errors) > 5:
            error_text += f"\n_...and {len(errors) - 5} more errors_"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Errors:*\n{error_text}"
            }
        })
    
    # Add context with timestamp and execution ID
    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
            {"type": "mrkdwn", "text": f"🆔 `{execution_id[:8]}...`"}
        ]
    })
    
    return {
        "attachments": [{
            "color": color,
            "blocks": blocks
        }]
    }


def send_slack_notification(
    message: Dict[str, Any],
    correlation_id: str = '',
    status_logger: Optional[Any] = None,
) -> bool:
    """
    Send notification to Slack webhook.
    
    Args:
        message: Slack message payload
        correlation_id: Optional correlation ID for tracing
        status_logger: Optional MikAura logger (single-path operational logs when set)
    
    Returns:
        True if successful, False otherwise
    """
    if not SLACK_WEBHOOK_URL:
        _slack_warning(
            status_logger,
            "SLACK_WEBHOOK_URL not configured, skipping notification",
            operation="slack_webhook",
        )
        return False

    for attempt in range(1, SLACK_MAX_RETRIES + 1):
        try:
            response = http.request(
                'POST',
                SLACK_WEBHOOK_URL,
                body=json.dumps(message).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )

            if response.status == 200:
                _slack_info(
                    status_logger,
                    "Slack notification sent successfully",
                    correlation_id=correlation_id or None,
                    attempt=attempt,
                    operation="slack_post",
                )
                return True

            error_body = response.data.decode('utf-8', errors='replace') if getattr(response, 'data', None) else ''
            _slack_error(
                status_logger,
                f"Slack API error: {response.status} - {error_body}",
                reason="slack_http_error",
                http_status=response.status,
                correlation_id=correlation_id or None,
                attempt=attempt,
            )
            if attempt >= SLACK_MAX_RETRIES:
                return False
        except Exception as e:
            _slack_error(
                status_logger,
                f"Error sending Slack notification: {e}",
                reason="slack_request_exception",
                correlation_id=correlation_id or None,
                attempt=attempt,
                error=str(e),
            )
            if attempt >= SLACK_MAX_RETRIES:
                return False

        sleep_seconds = SLACK_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
        _slack_warning(
            status_logger,
            f"Retrying Slack notification in {sleep_seconds:.1f}s (attempt {attempt}/{SLACK_MAX_RETRIES})",
            correlation_id=correlation_id or None,
            attempt=attempt,
            sleep_seconds=sleep_seconds,
            operation="slack_retry",
        )
        time.sleep(sleep_seconds)

    return False


def extract_results_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract processing results from Step Function event.
    
    The event structure from Step Function Map state:
    {
        "clientsResult": {
            "status": "active_clients",
            "clients": [...]
        },
        "ProcessClients": [  # Map state results - one entry per brand-retailer combination
            {"client_id": "...", "status": "SUCCESS", ...},
            ...
        ]
    }
    
    Note: ProcessClients array contains one entry per brand-retailer combination,
    not per unique client. Multiple entries may share the same client_id.
    """
    clients_processed = []
    total_files = 0
    total_records = 0
    errors = []
    unique_client_ids = set()  # Track unique client IDs
    
    # Extract Map state results (ProcessClients array)
    map_results = event.get('ProcessClients', [])
    if not map_results:
        # Try alternative path - direct array from Map state
        map_results = event if isinstance(event, list) else []
    
    for result in map_results:
        if isinstance(result, dict):
            client_id = result.get('client_id', 'unknown')
            brand_name = result.get('brand_name', 'unknown')
            retailer_id = result.get('retailer_id', 'unknown')
            status = result.get('status', 'unknown')
            files = result.get('files_processed', 0)
            records = result.get('records_processed', 0)
            error = result.get('error', '')
            
            # Normalize empty strings to 'unknown' for display
            if not client_id or (isinstance(client_id, str) and client_id.strip() == ''):
                client_id = 'unknown'
            if not brand_name or (isinstance(brand_name, str) and brand_name.strip() == ''):
                brand_name = 'unknown'
            if not retailer_id or (isinstance(retailer_id, str) and retailer_id.strip() == ''):
                retailer_id = 'unknown'
            
            # Track unique client IDs
            if client_id != 'unknown':
                unique_client_ids.add(client_id)
            
            clients_processed.append({
                'client_id': client_id,
                'brand_name': brand_name,
                'retailer_id': retailer_id,
                'status': status,
                'files_processed': files,
                'records_processed': records
            })
            
            total_files += files
            total_records += records
            
            if error:
                errors.append(f"{client_id}/{brand_name}: {error}")
            elif status == 'FAILED':
                errors.append(f"{client_id}/{brand_name}: Processing failed")
    
    # Determine overall status
    if not clients_processed:
        overall_status = event.get('clientsResult', {}).get('status', 'NO_ACTIVE_CLIENTS')
        if overall_status == 'no_active_clients':
            overall_status = 'NO_ACTIVE_CLIENTS'
    elif errors:
        # Check if all failed or partial
        failed_count = sum(1 for c in clients_processed if c['status'] != 'SUCCESS')
        if failed_count == len(clients_processed):
            overall_status = 'FAILED'
        else:
            overall_status = 'PARTIAL'
    else:
        overall_status = 'SUCCESS'
    
    return {
        'status': overall_status,
        'clients_processed': clients_processed,
        'unique_clients_count': len(unique_client_ids),  # Count of unique client IDs
        'total_files': total_files,
        'total_records': total_records,
        'errors': errors
    }


def format_schema_drift_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    missing_columns: List[str],
    extra_columns: List[str],
    filename: str,
    timestamp: str,
    environment: str,
    missing_in_source: List[str] = None,
) -> Dict[str, Any]:
    """
    Format a schema drift alert message when source file differs from SOT.

    Creates a warning-style Slack message asking if the schema difference is expected.

    Args:
        client_id: Client identifier
        brand_name: Brand name
        retailer_id: Retailer identifier
        missing_columns: SOT columns missing from source (backward compatibility)
        extra_columns: Extra columns in output not in SOT / suffix rules
        filename: Source filename
        timestamp: Current timestamp
        environment: Environment name (dev, qa, stg, prd)
        missing_in_source: SOT columns never present in source (optional; defaults to missing_columns)

    Returns:
        Slack message payload with warning styling
    """
    if missing_in_source is None:
        missing_in_source = list(missing_columns)

    missing_in_source_text = ", ".join(missing_in_source[:5]) if missing_in_source else "None"
    if len(missing_in_source) > 5:
        missing_in_source_text += f" (+{len(missing_in_source) - 5} more)"

    extra_text = ", ".join(extra_columns[:5]) if extra_columns else "None"
    if len(extra_columns) > 5:
        extra_text += f" (+{len(extra_columns) - 5} more)"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚠️ *SCHEMA DRIFT DETECTED - Is This Expected?*"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
            ]
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Missing in source:*\n{missing_in_source_text}"},
                {"type": "mrkdwn", "text": f"*Extra columns:*\n{extra_text}"},
            ]
        },
    ]
    
    blocks.extend([
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Action:*\nReview and confirm if changes are expected"},
            ]
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                {"type": "mrkdwn", "text": "📊 File was processed but schema differs from SOT"}
            ]
        }
    ])
    
    return {
        "attachments": [{
            "color": "warning",  # Yellow sidebar
            "blocks": blocks
        }]
    }


def handle_schema_drift_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Handle schema drift alert event.
    
    Sends an alert when source file columns differ from SOT schema.
    
    Args:
        event: Event containing schema drift details
        timestamp: Current timestamp
        environment: Environment name (dev, qa, stg, prd)
        
    Returns:
        Result dict indicating success/failure
    """
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    missing_columns = event.get('missing_columns', [])
    missing_in_source = event.get('missing_in_source', [])
    extra_columns = event.get('extra_columns', [])
    filename = event.get('filename', 'unknown')

    message = format_schema_drift_alert(
        client_id=client_id,
        brand_name=brand_name,
        retailer_id=retailer_id,
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        filename=filename,
        timestamp=timestamp,
        environment=environment,
        missing_in_source=missing_in_source,
    )
    
    success = send_slack_notification(message, status_logger=status_logger)
    
    _slack_info(
        status_logger,
        f"Schema drift alert for {client_id}/{brand_name}/{retailer_id}: {'sent' if success else 'failed'}",
        alert_type="SCHEMA_DRIFT",
        client_id=client_id,
        brand_name=brand_name,
        retailer_id=retailer_id,
        notification_sent=success,
    )
    
    return {
        'alert_sent': success,
        'client_id': client_id,
        'brand_name': brand_name,
        'retailer_id': retailer_id,
        'missing_columns_count': len(missing_columns),
        'extra_columns_count': len(extra_columns)
    }


def format_stale_data_alert(client: Dict[str, Any], timestamp: str, environment: str) -> Dict[str, Any]:
    """
    Format a single stale client alert message.
    
    Creates a warning-style Slack message for a client with stale data.
    
    Args:
        client: Stale client record with days_stale calculated
        timestamp: Current timestamp for the message
        environment: Environment name (dev, qa, stg, prd)
        
    Returns:
        Slack message payload with red alert styling
    """
    client_id = client.get('client_id', 'unknown')
    brand_name = client.get('brand_name', 'unknown')
    retailer_id = client.get('retailer_id', 'unknown')
    client_name = client.get('client_name', client_id)
    last_updated = client.get('last_data_updated', 'N/A')
    days_stale = client.get('days_stale', '?')
    
    return {
        "attachments": [{
            "color": "danger",  # Red sidebar
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "⚠️ *ALERT: Missing Data Update*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                        {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                        {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                        {"type": "mrkdwn", "text": f"*Last Updated:*\n{last_updated} ({days_stale} days ago)"},
                        {"type": "mrkdwn", "text": "*Action Required:*\nCheck source file in Tracer bucket"}
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"⏱️ {timestamp}"}
                    ]
                }
            ]
        }]
    }


def format_column_drop_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    dropped_columns: List[Dict[str, str]],
    total_source_columns: int,
    total_output_columns: int,
    environment: str
) -> Dict[str, Any]:
    """
    Format column drop alert message for Slack.
    
    Args:
        client_id: Client identifier
        brand_name: Brand name
        retailer_id: Retailer identifier
        filename: Source filename
        dropped_columns: List of dicts with 'column' and 'reason' keys
        total_source_columns: Total columns in source file
        total_output_columns: Total columns in output file
        environment: Environment name (dev, qa, stg, prd)
    
    Returns:
        Slack message dict
    """
    # Format dropped columns list with reasons
    dropped_text = ""
    for i, dc in enumerate(dropped_columns[:10], 1):  # Show first 10
        dropped_text += f"{i}. `{dc['column']}` - {dc['reason']}\n"
    if len(dropped_columns) > 10:
        dropped_text += f"... and {len(dropped_columns) - 10} more\n"
    
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🚨 *COLUMN DROP ALERT - Data Loss Detected*"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
            ]
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Columns Dropped:*\n{dropped_text}"},
                {"type": "mrkdwn", "text": f"*Column Count:*\n{total_source_columns} → {total_output_columns} ({len(dropped_columns)} dropped)"},
            ]
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                {"type": "mrkdwn", "text": "⚠️ Columns were dropped during processing - review reasons"}
            ]
        }
    ]
    
    return {
        "attachments": [{
            "color": "danger",  # Red sidebar for data loss
            "blocks": blocks
        }]
    }


def handle_column_drop_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Handle column drop alert event.
    
    Args:
        event: Event dict with column drop details
        timestamp: Timestamp string
        environment: Environment name (dev, qa, stg, prd)
    
    Returns:
        Result dict with alert_sent status
    """
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    dropped_columns = event.get('dropped_columns', [])
    total_source_columns = event.get('total_source_columns', 0)
    total_output_columns = event.get('total_output_columns', 0)
    
    if not dropped_columns:
        _slack_warning(
            status_logger,
            "Column drop alert received but no dropped columns",
            alert_type="COLUMN_DROP",
        )
        return {'alert_sent': False, 'reason': 'No dropped columns'}
    
    try:
        message = format_column_drop_alert(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            filename=filename,
            dropped_columns=dropped_columns,
            total_source_columns=total_source_columns,
            total_output_columns=total_output_columns,
            environment=environment
        )
        
        response = http.request(
            'POST',
            SLACK_WEBHOOK_URL,
            body=json.dumps(message).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Column drop alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="COLUMN_DROP",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {'alert_sent': True}
        else:
            _slack_error(
                status_logger,
                f"Failed to send column drop alert: Status {response.status}",
                reason="slack_http_error",
                http_status=response.status,
                alert_type="COLUMN_DROP",
                client_id=client_id,
            )
            return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
            
    except Exception as e:
        _slack_exception(status_logger, "Error sending column drop alert", e, alert_type="COLUMN_DROP")
        return {'alert_sent': False, 'reason': str(e)}


def format_invalid_sales_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    invalid_columns: List[Dict[str, Any]],
    total_rows: int,
    invalid_row_count: int,
    timestamp: str,
    environment: str
) -> Dict[str, Any]:
    """
    Format invalid sales data alert message for Slack.

    Creates a critical alert when file ingestion is skipped due to null/NaN/missing sales values.

    Args:
        client_id: Client identifier
        brand_name: Brand name
        retailer_id: Retailer identifier
        filename: Source filename
        invalid_columns: List of dicts with 'column', 'invalid_rows', and 'invalid_count' keys
        total_rows: Total number of rows in the file
        invalid_row_count: Total number of rows with invalid sales data
        timestamp: Current timestamp
        environment: Environment name (dev, qa, stg, prd)

    Returns:
        Slack message dict
    """
    # Format invalid columns list with details
    invalid_columns_text = ""
    for i, col_info in enumerate(invalid_columns[:10], 1):  # Show first 10 columns
        col_name = col_info.get('column', 'unknown')
        invalid_count = col_info.get('invalid_count', 0)
        invalid_rows = col_info.get('invalid_rows', [])
        sample_rows = invalid_rows[:10]
        rows_text = ", ".join(map(str, sample_rows))
        if len(invalid_rows) > 10:
            rows_text += f", ... (+{len(invalid_rows) - 10} more)"
        invalid_columns_text += f"{i}. `{col_name}`: {invalid_count} row(s) with null/NaN/empty values"
        if sample_rows:
            invalid_columns_text += f" (rows: {rows_text})"
        invalid_columns_text += "\n"
    if len(invalid_columns) > 10:
        invalid_columns_text += f"... and {len(invalid_columns) - 10} more column(s) with invalid data\n"
    impact_percentage = (invalid_row_count / total_rows * 100) if total_rows > 0 else 0
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "🚨 *FILE INGESTION SKIPPED - Invalid Sales Data*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
            {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
            {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
            {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Invalid Sales Columns:*\n{invalid_columns_text.strip() or 'None'}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Impact:*\n{invalid_row_count} invalid row(s) out of {total_rows} total ({impact_percentage:.1f}%)"},
            {"type": "mrkdwn", "text": "*Action Required:*\nReview source file and fix missing sales data"}
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": "⚠️ *File was NOT ingested because sales values are null/NaN/missing*"}},
        {"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
            {"type": "mrkdwn", "text": "🚫 File ingestion blocked due to invalid sales data"}
        ]}
    ]
    return {"attachments": [{"color": "danger", "blocks": blocks}]}


def format_missing_channel_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    missing_channels: List[Dict[str, Any]],
    quality_issues: List[Dict[str, Any]],
    total_rows: int,
    timestamp: str,
    environment: str,
) -> Dict[str, Any]:
    """
    Format Slack alert when file ingestion is skipped due to missing channel data.
    """
    miss_lines = []
    for i, m in enumerate(missing_channels[:15], 1):
        col = m.get("column", m.get("message", "unknown"))
        reason = m.get("reason", "")
        miss_lines.append(f"{i}. `{col}` — {reason or 'missing/empty'}")
    if len(missing_channels) > 15:
        miss_lines.append(f"... and {len(missing_channels) - 15} more")
    miss_text = "\n".join(miss_lines) if miss_lines else "None"

    q_lines = []
    for i, q in enumerate(quality_issues[:8], 1):
        cname = q.get("column", "unknown")
        q_lines.append(
            f"{i}. `{cname}` partial_null={q.get('partial_null_count', 0)}, "
            f"non_numeric={q.get('non_numeric_count', 0)}, negative={q.get('negative_count', 0)}"
        )
    if len(quality_issues) > 8:
        q_lines.append(f"... and {len(quality_issues) - 8} more quality note(s)")
    q_text = "\n".join(q_lines) if q_lines else "None"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🚨 *FILE INGESTION SKIPPED - Missing Channel Data*",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Missing channel issues:*\n{miss_text}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Related quality context (if any):*\n{q_text}",
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Total rows in file:*\n{total_rows}",
                },
                {
                    "type": "mrkdwn",
                    "text": "*Action Required:*\nReview source file for impression/channel columns",
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                {"type": "mrkdwn", "text": "🚫 Ingestion blocked: no usable channel impressions"},
            ],
        },
    ]
    return {"attachments": [{"color": "danger", "blocks": blocks}]}


def handle_missing_channel_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle missing channel data alert (ingestion skipped)."""
    client_id = event.get("client_id", "unknown")
    brand_name = event.get("brand_name", "unknown")
    retailer_id = event.get("retailer_id", "unknown")
    filename = event.get("filename", "unknown")
    missing_channels = event.get("missing_channels", [])
    quality_issues = event.get("quality_issues", [])
    total_rows = event.get("total_rows", 0)
    if not missing_channels:
        _slack_warning(
            status_logger,
            "Missing channel alert received but no missing_channels",
            alert_type="MISSING_CHANNEL_DATA",
        )
        return {"alert_sent": False, "reason": "No missing_channels"}
    try:
        message = format_missing_channel_alert(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            filename=filename,
            missing_channels=missing_channels,
            quality_issues=quality_issues,
            total_rows=int(total_rows),
            timestamp=timestamp,
            environment=environment,
        )
        if send_slack_notification(message, status_logger=status_logger):
            _slack_info(
                status_logger,
                f"Missing channel data alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="MISSING_CHANNEL_DATA",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {"alert_sent": True}
        _slack_error(
            status_logger,
            "Failed to send missing channel data alert after retries",
            reason="slack_delivery_failed",
            alert_type="MISSING_CHANNEL_DATA",
        )
        return {"alert_sent": False, "reason": "Slack delivery failed"}
    except Exception as e:
        _slack_exception(
            status_logger, "Error sending missing channel data alert", e, alert_type="MISSING_CHANNEL_DATA"
        )
        return {"alert_sent": False, "reason": str(e)}


def handle_invalid_sales_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Handle invalid sales data alert event.
    Sends an alert when file ingestion is skipped due to null/NaN/missing sales values.
    """
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    invalid_columns = event.get('invalid_columns', [])
    total_rows = event.get('total_rows', 0)
    invalid_row_count = event.get('invalid_row_count', 0)
    if not invalid_columns:
        _slack_warning(
            status_logger,
            "Invalid sales data alert received but no invalid columns",
            alert_type="INVALID_SALES_DATA",
        )
        return {'alert_sent': False, 'reason': 'No invalid columns'}
    try:
        message = format_invalid_sales_alert(
            client_id=client_id, brand_name=brand_name, retailer_id=retailer_id,
            filename=filename, invalid_columns=invalid_columns, total_rows=total_rows,
            invalid_row_count=invalid_row_count, timestamp=timestamp, environment=environment
        )
        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Invalid sales data alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="INVALID_SALES_DATA",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {'alert_sent': True}
        _slack_error(
            status_logger,
            f"Failed to send invalid sales data alert: Status {response.status}",
            reason="slack_http_error",
            http_status=response.status,
            alert_type="INVALID_SALES_DATA",
        )
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending invalid sales data alert", e, alert_type="INVALID_SALES_DATA")
        return {'alert_sent': False, 'reason': str(e)}


def format_duplicate_dates_alert(
    client_id: str, brand_name: str, retailer_id: str, filename: str,
    duplicates: List[Dict[str, Any]], duplicate_count: int,
    timestamp: str, environment: str
) -> Dict[str, Any]:
    """Format duplicate dates alert message for Slack."""
    dup_lines = []
    for i, d in enumerate(duplicates[:10], 1):
        date_val = d.get('date', 'n/a')
        occ = d.get('occurrences', 0)
        indices = d.get('row_indices', [])
        idx_preview = ", ".join(map(str, indices[:5]))
        if len(indices) > 5:
            idx_preview += f", ... (+{len(indices) - 5} more)"
        dup_lines.append(f"{i}. Date `{date_val}` appears {occ} time(s) (row indices: {idx_preview})")
    if len(duplicates) > 10:
        dup_lines.append(f"... and {len(duplicates) - 10} more duplicate date(s)")
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "🚨 *FILE INGESTION BLOCKED - Duplicate Dates*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
            {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
            {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
            {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Duplicate dates ({duplicate_count}):*\n" + "\n".join(dup_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "⚠️ *File was NOT ingested. Each date must appear exactly once (weekly data).*"}},
        {"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
            {"type": "mrkdwn", "text": "🚫 Duplicate date detection"}
        ]}
    ]
    return {"attachments": [{"color": "danger", "blocks": blocks}]}


def handle_duplicate_dates_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle duplicate dates alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    duplicates = event.get('duplicates', [])
    duplicate_count = event.get('duplicate_count', 0)
    if not duplicates and duplicate_count == 0:
        _slack_warning(
            status_logger,
            "Duplicate dates alert received but no duplicates",
            alert_type="DUPLICATE_DATES",
        )
        return {'alert_sent': False, 'reason': 'No duplicates'}
    try:
        message = format_duplicate_dates_alert(
            client_id=client_id, brand_name=brand_name, retailer_id=retailer_id,
            filename=filename, duplicates=duplicates, duplicate_count=duplicate_count,
            timestamp=timestamp, environment=environment
        )
        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Duplicate dates alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="DUPLICATE_DATES",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {'alert_sent': True}
        _slack_error(
            status_logger,
            f"Failed to send duplicate dates alert: Status {response.status}",
            reason="slack_http_error",
            http_status=response.status,
            alert_type="DUPLICATE_DATES",
        )
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending duplicate dates alert", e, alert_type="DUPLICATE_DATES")
        return {'alert_sent': False, 'reason': str(e)}


def format_date_gaps_alert(
    client_id: str, brand_name: str, retailer_id: str, filename: str,
    gaps: List[Dict[str, Any]], gap_count: int,
    timestamp: str, environment: str
) -> Dict[str, Any]:
    """Format date gaps (missing weeks) alert message for Slack."""
    gap_lines = []
    for i, g in enumerate(gaps[:10], 1):
        after = g.get('after_date', 'n/a')
        before = g.get('before_date', 'n/a')
        weeks = g.get('missing_weeks', 0)
        gap_lines.append(f"{i}. After `{after}` → next row `{before}`: {weeks} week(s) missing")
    if len(gaps) > 10:
        gap_lines.append(f"... and {len(gaps) - 10} more gap(s)")
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "🚨 *FILE INGESTION BLOCKED - Date Gaps (Missing Weeks)*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
            {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
            {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
            {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Gaps ({gap_count}):*\n" + "\n".join(gap_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "⚠️ *File was NOT ingested. Weekly data must have no missing weeks.*"}},
        {"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
            {"type": "mrkdwn", "text": "🚫 Date gap detection"}
        ]}
    ]
    return {"attachments": [{"color": "danger", "blocks": blocks}]}


def handle_date_gaps_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle date gaps alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    gaps = event.get('gaps', [])
    gap_count = event.get('gap_count', 0)
    if not gaps and gap_count == 0:
        _slack_warning(
            status_logger,
            "Date gaps alert received but no gaps",
            alert_type="DATE_GAPS",
        )
        return {'alert_sent': False, 'reason': 'No gaps'}
    try:
        message = format_date_gaps_alert(
            client_id=client_id, brand_name=brand_name, retailer_id=retailer_id,
            filename=filename, gaps=gaps, gap_count=gap_count,
            timestamp=timestamp, environment=environment
        )
        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Date gaps alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="DATE_GAPS",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {'alert_sent': True}
        _slack_error(
            status_logger,
            f"Failed to send date gaps alert: Status {response.status}",
            reason="slack_http_error",
            http_status=response.status,
            alert_type="DATE_GAPS",
        )
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending date gaps alert", e, alert_type="DATE_GAPS")
        return {'alert_sent': False, 'reason': str(e)}


def format_row_count_drop_alert(
    client_id: str, brand_name: str, retailer_id: str, filename: str,
    severity: str, current_rows: int, historical_avg: int, drop_percentage: float,
    timestamp: str, environment: str
) -> Dict[str, Any]:
    """Format row count drop alert message for Slack (RED block or YELLOW warn)."""
    color = "danger" if severity == "RED" else "warning"
    title = "FILE INGESTION BLOCKED - Row Count Drop" if severity == "RED" else "Row Count Drop Warning"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"🚨 *{title}*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
            {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
            {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
            {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
        ]},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Current rows:*\n{current_rows}"},
            {"type": "mrkdwn", "text": f"*Historical avg:*\n{historical_avg}"},
            {"type": "mrkdwn", "text": f"*Drop:*\n{drop_percentage:.1f}%"},
            {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
        ]},
        {"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
            {"type": "mrkdwn", "text": "📉 Row count sanity check"}
        ]}
    ]
    return {"attachments": [{"color": color, "blocks": blocks}]}


def handle_row_count_drop_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle row count drop alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    severity = event.get('severity', 'NONE')
    current_rows = event.get('current_rows', 0)
    historical_avg = event.get('historical_avg', 0)
    drop_percentage = event.get('drop_percentage', 0)
    try:
        message = format_row_count_drop_alert(
            client_id=client_id, brand_name=brand_name, retailer_id=retailer_id,
            filename=filename, severity=severity, current_rows=current_rows,
            historical_avg=historical_avg, drop_percentage=drop_percentage,
            timestamp=timestamp, environment=environment
        )
        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Row count drop alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="ROW_COUNT_DROP",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {'alert_sent': True}
        _slack_error(
            status_logger,
            f"Failed to send row count drop alert: Status {response.status}",
            reason="slack_http_error",
            http_status=response.status,
            alert_type="ROW_COUNT_DROP",
        )
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending row count drop alert", e, alert_type="ROW_COUNT_DROP")
        return {'alert_sent': False, 'reason': str(e)}


def format_spend_regime_shift_alert(
    client_id: str, brand_name: str, retailer_id: str, filename: str,
    severity: str, drift_metric_current: float, anomalies: List[Dict[str, Any]],
    timestamp: str, environment: str
) -> Dict[str, Any]:
    """Format Spend Regime Shift (ME-5401) alert for Slack."""
    anomaly_lines = []
    for index, anomaly in enumerate(anomalies[:8], 1):
        channel = anomaly.get('channel', 'unknown')
        value = anomaly.get('value', 'n/a')
        z_score = anomaly.get('z_score', 'n/a')
        p99 = anomaly.get('p99', 'n/a')
        anomaly_lines.append(f"{index}. `{channel}` value={value}, z={z_score}, p99={p99}")
    if len(anomalies) > 8:
        anomaly_lines.append(f"... and {len(anomalies) - 8} more channel(s)")
    anomaly_text = "\n".join(anomaly_lines) if anomaly_lines else "None"
    alert_color = "danger" if str(severity).upper() == "RED" else "warning"
    return {
        "attachments": [{
            "color": alert_color,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "🚨 *SPEND REGIME SHIFT DETECTED*"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "<!here> Anomaly detected; retraining_required has been set to true."}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                    {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                    {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                    {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                    {"type": "mrkdwn", "text": f"*Drift Metric (max |z|):*\n{drift_metric_current}"},
                ]},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Inflated Values / Breaches:*\n{anomaly_text}"}},
                {"type": "context", "elements": [
                    {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                    {"type": "mrkdwn", "text": "📈 Spend outlier checks use z-score and p99/p995 thresholds"}
                ]}
            ]
        }]
    }


def handle_spend_regime_shift_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle Spend Regime Shift alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    severity = event.get('severity', 'YELLOW')
    drift_metric_current = event.get('drift_metric_current', 0.0)
    anomalies = event.get('anomalies', [])
    if not anomalies:
        _slack_warning(
            status_logger,
            "Spend regime shift alert received without anomalies",
            alert_type="SPEND_REGIME_SHIFT",
        )
        return {'alert_sent': False, 'reason': 'No anomalies provided'}
    try:
        message = format_spend_regime_shift_alert(
            client_id=client_id, brand_name=brand_name, retailer_id=retailer_id,
            filename=filename, severity=severity, drift_metric_current=drift_metric_current,
            anomalies=anomalies, timestamp=timestamp, environment=environment
        )
        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Spend regime shift alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="SPEND_REGIME_SHIFT",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
                severity=severity,
                anomaly_count=len(anomalies),
            )
            return {'alert_sent': True, 'client_id': client_id, 'brand_name': brand_name, 'retailer_id': retailer_id, 'severity': severity, 'anomaly_count': len(anomalies)}
        _slack_error(
            status_logger,
            f"Failed to send spend regime shift alert: Status {response.status}",
            reason="slack_http_error",
            http_status=response.status,
            alert_type="SPEND_REGIME_SHIFT",
        )
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending spend regime shift alert", e, alert_type="SPEND_REGIME_SHIFT")
        return {'alert_sent': False, 'reason': str(e)}


def format_kpi_behavior_break_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    severity: str,
    drift_metric_current: float,
    anomalies: List[Dict[str, Any]],
    timestamp: str,
    environment: str,
    correlation_id: str = ''
) -> Dict[str, Any]:
    """Format KPI Behavior Break (ME-5402) alert for Slack."""
    anomaly_lines = []
    for index, anomaly in enumerate(anomalies[:6], 1):
        residual_z = anomaly.get('residual_z', 'n/a')
        kpi_delta = anomaly.get('kpi_delta', 'n/a')
        spend_delta = anomaly.get('spend_delta', 'n/a')
        opposite = anomaly.get('threshold_breaches', {}).get('opposite_direction', False)
        anomaly_lines.append(
            f"{index}. residual_z={residual_z}, kpi_delta={kpi_delta}, spend_delta={spend_delta}, opposite_direction={opposite}"
        )
    if len(anomalies) > 6:
        anomaly_lines.append(f"... and {len(anomalies) - 6} more signal(s)")

    anomaly_text = "\n".join(anomaly_lines) if anomaly_lines else "None"
    alert_color = "danger" if str(severity).upper() == "RED" else "warning"

    return {
        "attachments": [{
            "color": alert_color,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🚨 *KPI BEHAVIOR BREAK DETECTED*"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "<!here> ME-5402 anomaly detected; retraining_required has been set to true."
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                        {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                        {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                        {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
                        {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                        {"type": "mrkdwn", "text": f"*Drift Metric (|residual z|):*\n{drift_metric_current}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Behavior-Break Signals:*\n{anomaly_text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                        {"type": "mrkdwn", "text": "📉 KPI movement diverged from media-expected behavior"},
                        {"type": "mrkdwn", "text": f"🔗 correlation_id={correlation_id}" if correlation_id else "🔗 correlation_id=not-provided"}
                    ]
                }
            ]
        }]
    }


def handle_kpi_behavior_break_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle KPI Behavior Break alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    severity = event.get('severity', 'YELLOW')
    drift_metric_current = event.get('drift_metric_current', 0.0)
    anomalies = event.get('anomalies', [])
    correlation_id = event.get('correlation_id', '')

    if not anomalies:
        _slack_warning(
            status_logger,
            "KPI behavior break alert received without anomalies",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
        return {'alert_sent': False, 'reason': 'No anomalies provided'}

    try:
        message = format_kpi_behavior_break_alert(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            filename=filename,
            severity=severity,
            drift_metric_current=drift_metric_current,
            anomalies=anomalies,
            timestamp=timestamp,
            environment=environment,
            correlation_id=correlation_id
        )

        if send_slack_notification(message, correlation_id=correlation_id, status_logger=status_logger):
            _slack_info(
                status_logger,
                f"KPI behavior break alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="KPI_BEHAVIOR_BREAK",
                client_id=client_id,
                brand_name=brand_name,
                retailer_id=retailer_id,
            )
            return {
                'alert_sent': True,
                'client_id': client_id,
                'brand_name': brand_name,
                'retailer_id': retailer_id,
                'severity': severity,
                'anomaly_count': len(anomalies)
            }

        _slack_error(
            status_logger,
            "Failed to send KPI behavior break alert after retries",
            reason="slack_delivery_failed",
            alert_type="KPI_BEHAVIOR_BREAK",
        )
        return {'alert_sent': False, 'reason': 'Slack delivery failed after retries'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending KPI behavior break alert", e, alert_type="KPI_BEHAVIOR_BREAK")
        return {'alert_sent': False, 'reason': str(e)}


def format_channel_activation_deactivation_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    severity: str,
    drift_metric_current: float,
    anomalies: List[Dict[str, Any]],
    timestamp: str,
    environment: str,
    correlation_id: str = ''
) -> Dict[str, Any]:
    """Format Channel Activation/Deactivation (ME-5806a) alert for Slack."""
    anomaly_lines = []
    for index, anomaly in enumerate(anomalies[:6], 1):
        channel = anomaly.get('channel', 'unknown')
        a_type = anomaly.get('type', 'unknown')
        history_wks = anomaly.get('active_weeks', 0)
        req_wks = anomaly.get('threshold', 0)
        val = anomaly.get('current_value', 0.0)
        anomaly_lines.append(
            f"{index}. `{channel}` [{a_type}] | spend={val}, history={history_wks}wks (needs {req_wks}wks minimum)"
        )
    if len(anomalies) > 6:
        anomaly_lines.append(f"... and {len(anomalies) - 6} more signal(s)")

    anomaly_text = "\n".join(anomaly_lines) if anomaly_lines else "None"
    alert_color = "danger" if str(severity).upper() == "RED" else "warning"

    return {
        "attachments": [{
            "color": alert_color,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🚨 *CHANNEL ACTIVATION/DEACTIVATION DETECTED*"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "<!here> ME-5806a anomaly detected; retraining_required has been set to true."
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                        {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                        {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                        {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
                        {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                        {"type": "mrkdwn", "text": f"*Affected Channels:*\n{int(drift_metric_current)}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Signals:*\n{anomaly_text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                        {"type": "mrkdwn", "text": "📉 Channel structure changed unexpectedly (no posterior / strict credit)"},
                        {"type": "mrkdwn", "text": f"🔗 correlation_id={correlation_id}" if correlation_id else "🔗 correlation_id=not-provided"}
                    ]
                }
            ]
        }]
    }


def handle_channel_activation_deactivation_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle Channel Activation/Deactivation alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    severity = event.get('severity', 'RED')
    drift_metric_current = event.get('drift_metric_current', 0.0)
    anomalies = event.get('anomalies', [])
    correlation_id = event.get('correlation_id', '')

    if not anomalies:
        _slack_warning(
            status_logger,
            "Channel activation/deactivation alert received without anomalies",
            alert_type="CHANNEL_ACTIVATION_DEACTIVATION",
        )
        return {'alert_sent': False, 'reason': 'No anomalies provided'}

    try:
        message = format_channel_activation_deactivation_alert(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            filename=filename,
            severity=severity,
            drift_metric_current=drift_metric_current,
            anomalies=anomalies,
            timestamp=timestamp,
            environment=environment,
            correlation_id=correlation_id
        )

        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Channel activation/deactivation alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="CHANNEL_ACTIVATION_DEACTIVATION",
            )
            return {'alert_sent': True}
            
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending Channel Activation/Deactivation alert", e, alert_type="CHANNEL_ACTIVATION_DEACTIVATION")
        return {'alert_sent': False, 'reason': str(e)}


def format_spend_mix_reallocation_alert(
    client_id: str,
    brand_name: str,
    retailer_id: str,
    filename: str,
    severity: str,
    drift_metric_current: float,
    anomalies: List[Dict[str, Any]],
    timestamp: str,
    environment: str,
    correlation_id: str = ''
) -> Dict[str, Any]:
    """Format Spend Mix Reallocation (ME-5806b) alert for Slack."""
    anomaly_lines = []
    # Anomalies should have 1 item generally containing overall vector info
    if anomalies:
        info = anomalies[0]
        js_div = info.get('js_divergence', 0.0)
        max_delta = info.get('max_share_delta', 0.0)
        
        anomaly_lines.append(f"• *JS Divergence*: {js_div}")
        anomaly_lines.append(f"• *Max Abs Delta*: {max_delta}")
        
        cdeltas = info.get('channel_deltas', [])
        # Sort by largest delta shift
        sorted_cdeltas = sorted(cdeltas, key=lambda x: x.get('delta', 0.0), reverse=True)
        for i, cinfo in enumerate(sorted_cdeltas[:4], 1):
            ch = cinfo.get('channel', 'unknown')
            c_share = cinfo.get('current_share', 0.0)
            b_share = cinfo.get('baseline_share', 0.0)
            anomaly_lines.append(f"    {i}. `{ch}`: {b_share:.3f} ➔ {c_share:.3f}")

    anomaly_text = "\n".join(anomaly_lines) if anomaly_lines else "None"
    alert_color = "danger" if str(severity).upper() == "RED" else "warning"

    return {
        "attachments": [{
            "color": alert_color,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🚨 *SPEND MIX REALLOCATION DETECTED*"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "<!here> ME-5806b anomaly detected; retraining_required has been set to true."
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Client:*\n`{client_id}`"},
                        {"type": "mrkdwn", "text": f"*Brand / Retailer:*\n{brand_name} / {retailer_id}"},
                        {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                        {"type": "mrkdwn", "text": f"*File:*\n`{filename}`"},
                        {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                        {"type": "mrkdwn", "text": f"*Drift Metric (Normalized):*\n{drift_metric_current}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Distribution Shift:*\n{anomaly_text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"⏱️ {timestamp}"},
                        {"type": "mrkdwn", "text": "📉 Total volume steady, allocation shifted"},
                        {"type": "mrkdwn", "text": f"🔗 correlation_id={correlation_id}" if correlation_id else "🔗 correlation_id=not-provided"}
                    ]
                }
            ]
        }]
    }


def handle_spend_mix_reallocation_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle Spend Mix Reallocation alert event."""
    client_id = event.get('client_id', 'unknown')
    brand_name = event.get('brand_name', 'unknown')
    retailer_id = event.get('retailer_id', 'unknown')
    filename = event.get('filename', 'unknown')
    severity = event.get('severity', 'YELLOW')
    drift_metric_current = event.get('drift_metric_current', 0.0)
    anomalies = event.get('anomalies', [])
    correlation_id = event.get('correlation_id', '')

    if not anomalies:
        _slack_warning(
            status_logger,
            "Spend mix reallocation alert received without anomalies",
            alert_type="SPEND_MIX_REALLOCATION",
        )
        return {'alert_sent': False, 'reason': 'No anomalies provided'}

    try:
        message = format_spend_mix_reallocation_alert(
            client_id=client_id,
            brand_name=brand_name,
            retailer_id=retailer_id,
            filename=filename,
            severity=severity,
            drift_metric_current=drift_metric_current,
            anomalies=anomalies,
            timestamp=timestamp,
            environment=environment,
            correlation_id=correlation_id
        )

        response = http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            _slack_info(
                status_logger,
                f"Spend mix reallocation alert sent for {client_id}/{brand_name}/{retailer_id}",
                alert_type="SPEND_MIX_REALLOCATION",
            )
            return {'alert_sent': True}
            
        return {'alert_sent': False, 'reason': f'HTTP {response.status}'}
    except Exception as e:
        _slack_exception(status_logger, "Error sending Spend Mix Reallocation alert", e, alert_type="SPEND_MIX_REALLOCATION")
        return {'alert_sent': False, 'reason': str(e)}


def handle_stale_alert(
    event: Dict[str, Any],
    timestamp: str,
    environment: str,
    status_logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Handle stale data alert event from Step Function.
    
    Sends a single alert for the stale client in the event.
    Called from Map state in Step Function (once per stale client).
    
    Args:
        event: Event containing stale client data
        timestamp: Current timestamp
        environment: Environment name (dev, qa, stg, prd)
        
    Returns:
        Result dict indicating success/failure
    """
    # The event is a single stale client record from the Map state
    client = event
    
    # Format and send the alert
    message = format_stale_data_alert(client, timestamp, environment)
    success = send_slack_notification(message, status_logger=status_logger)
    
    client_id = client.get('client_id', 'unknown')
    brand_name = client.get('brand_name', 'unknown')
    
    _slack_info(
        status_logger,
        f"Stale alert for {client_id}/{brand_name}: {'sent' if success else 'failed'}",
        alert_type="STALE_DATA",
        client_id=client_id,
        brand_name=brand_name,
        notification_sent=success,
    )
    
    return {
        'alert_sent': success,
        'client_id': client_id,
        'brand_name': brand_name,
        'days_stale': client.get('days_stale', 0)
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Slack notifications.
    
    Receives aggregated results from Step Function and sends formatted
    notification to Slack.
    
    Supports two modes:
    1. Stale data alert: When event contains 'alert_type': 'STALE_DATA' or has 'days_stale'
    2. Data ingestion completion: Standard completion notification
    """
    execution_id = context.aws_request_id if context else 'local-test'
    timestamp = get_current_timestamp()
    
    # Get environment from ENVIRONMENT variable or function name
    environment = get_environment(context)

    # MikAura observability (pipeline-agnostic)
    status_logger = None
    metric_logger = None
    if _MIKAURA_AVAILABLE:
        _config = MikAuraObservabilityConfig(
            context={"pipeline_context": "Data Ingestion Pipeline"},
            environment=environment,
            correlation_id=execution_id,
        )
        status_logger = MikAuraStatusLogger.from_config(
            _config,
            min_level=LOG_LEVEL,
            allowed_statuses=set(SLACK_ALLOWED_STATUSES),
        )
        metric_logger = MikAuraMetricLogger.from_config(_config)

    if status_logger:
        status_logger.log_running(
            "Slack notification Lambda started",
            execution_id=execution_id,
        )
    else:
        _check_logger_required(f"Slack notification Lambda started: {execution_id}")
    _slack_debug(
        status_logger,
        f"Event: {json.dumps(event, default=str)}",
        execution_id=execution_id,
    )
    _slack_debug(status_logger, f"Environment: {environment}", execution_id=execution_id)

    if metric_logger:
        metric_logger.increment("data_ingestion.slack.invocation")
    elif _slack_metrics:
        _slack_metrics.increment(
            "data_ingestion.slack.invocation",
            tags=[f"env:{environment}"],
        )

    try:
        # Check if this is a schema drift alert
        if event.get('alert_type') == 'SCHEMA_DRIFT':
            result = handle_schema_drift_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }
        
        # Check if this is a column drop alert
        if event.get('alert_type') == 'COLUMN_DROP':
            result = handle_column_drop_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }
        
        # Check if this is an invalid sales data alert
        if event.get('alert_type') == 'INVALID_SALES_DATA':
            result = handle_invalid_sales_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        if event.get('alert_type') == 'MISSING_CHANNEL_DATA':
            result = handle_missing_channel_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp,
            }
        
        # Check if this is a spend regime shift alert
        if event.get('alert_type') == 'SPEND_REGIME_SHIFT':
            result = handle_spend_regime_shift_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a KPI behavior break alert
        if event.get('alert_type') == 'KPI_BEHAVIOR_BREAK':
            result = handle_kpi_behavior_break_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a channel activation/deactivation alert
        if event.get('alert_type') == 'CHANNEL_ACTIVATION_DEACTIVATION':
            result = handle_channel_activation_deactivation_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a spend mix reallocation alert
        if event.get('alert_type') == 'SPEND_MIX_REALLOCATION':
            result = handle_spend_mix_reallocation_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a duplicate dates alert
        if event.get('alert_type') == 'DUPLICATE_DATES':
            result = handle_duplicate_dates_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a date gaps alert
        if event.get('alert_type') == 'DATE_GAPS':
            result = handle_date_gaps_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a row count drop alert
        if event.get('alert_type') == 'ROW_COUNT_DROP':
            result = handle_row_count_drop_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }

        # Check if this is a stale data alert (from Map state iterator)
        if event.get('alert_type') == 'STALE_DATA' or 'days_stale' in event:
            result = handle_stale_alert(event, timestamp, environment, status_logger)
            return {
                'statusCode': 200 if result['alert_sent'] else 500,
                **result,
                'timestamp': timestamp
            }
        
        # Standard data ingestion completion notification
        # Extract results from event
        results = extract_results_from_event(event)
        
        # Format Slack message
        message = format_slack_message(
            status=results['status'],
            clients_processed=results['clients_processed'],
            total_files=results['total_files'],
            total_records=results['total_records'],
            errors=results['errors'],
            execution_id=execution_id,
            timestamp=timestamp,
            environment=environment
        )
        
        # Send notification
        success = send_slack_notification(message, status_logger=status_logger)
        
        _slack_info(
            status_logger,
            f"Slack notification {'sent' if success else 'skipped/failed'}",
            operation="ingestion_summary",
            notification_sent=success,
            execution_id=execution_id,
        )
        
        # Return event for Step Function pass-through
        return {
            'statusCode': 200 if success else 500,
            'notification_sent': success,
            'status': results['status'],
            'clients_count': results.get('unique_clients_count', len(results['clients_processed'])),  # Use unique count
            'total_files': results['total_files'],
            'total_records': results['total_records'],
            'timestamp': timestamp,
            # Pass through original event for downstream states
            **event
        }
        
    except Exception as e:
        _slack_exception(status_logger, "Error in Slack notification Lambda", e, execution_id=execution_id)
        
        # Don't fail the pipeline - just log and return
        return {
            'statusCode': 500,
            'notification_sent': False,
            'error': str(e),
            'timestamp': timestamp,
            **event
        }
