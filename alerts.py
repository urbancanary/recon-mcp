"""
alerts.py — Slack alerting for recon-mcp.

Sends notifications to #recon-alerts when:
- GA10 recalc has partial failures (bonds missing after retries)
- Upload parsing fails
- Data quality issues detected (null descriptions, missing par, etc.)
- Any unexpected error in the pipeline

Uses SLACK_BOT_TOKEN from auth-mcp. Fails silently (never blocks the pipeline).
"""

import logging
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)

SLACK_CHANNEL = "recon-alerts"
_bot_token: str | None = None
_token_loaded = False


def _get_token() -> str | None:
    global _bot_token, _token_loaded
    if _token_loaded:
        return _bot_token
    _token_loaded = True
    try:
        from auth_client import get_api_key
        _bot_token = get_api_key("SLACK_BOT_TOKEN")
    except Exception as e:
        logger.warning(f"Could not get SLACK_BOT_TOKEN: {e}")
    return _bot_token


async def send_alert(title: str, message: str, level: str = "warning", fields: dict = None):
    """Send a Slack alert to #recon-alerts.

    level: "info", "warning", "error"
    fields: optional dict of key-value pairs to show as structured fields
    """
    token = _get_token()
    if not token:
        logger.warning(f"Slack alert skipped (no token): {title}: {message}")
        return

    emoji = {"info": "ℹ️", "warning": "⚠️", "error": "🔴"}.get(level, "⚠️")
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{emoji} *{title}*\n{message}"}
        }
    ]

    if fields:
        field_blocks = [
            {"type": "mrkdwn", "text": f"*{k}:*\n{v}"}
            for k, v in list(fields.items())[:6]
        ]
        blocks.append({"type": "section", "fields": field_blocks})

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"recon-mcp • {now}"}]
    })

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://slack.com/api/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={
                    "channel": SLACK_CHANNEL,
                    "text": f"{emoji} {title}: {message}",
                    "blocks": blocks,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if not data.get("ok"):
                    logger.warning(f"Slack API error: {data.get('error')}")
            else:
                logger.warning(f"Slack HTTP error: {resp.status_code}")
    except Exception as e:
        logger.warning(f"Slack alert failed: {e}")


# Convenience wrappers for common alerts

async def alert_ga10_partial_failure(portfolio_id: str, date: str,
                                     sent: int, received: int, missing_isins: list[str]):
    await send_alert(
        title="GA10 Partial Failure",
        message=f"Sent {sent} bonds, got {received} back. {len(missing_isins)} missing after retries.",
        level="error",
        fields={
            "Portfolio": portfolio_id,
            "Date": date,
            "Missing ISINs": ", ".join(missing_isins[:10]) + (f" (+{len(missing_isins)-10} more)" if len(missing_isins) > 10 else ""),
        },
    )


async def alert_upload_failed(source: str, filename: str, error: str, uploaded_by: str = ""):
    await send_alert(
        title=f"{source.upper()} Upload Failed",
        message=error,
        level="error",
        fields={
            "File": filename,
            "Uploaded by": uploaded_by or "unknown",
        },
    )


async def alert_data_quality(issue: str, table: str, count: int, sample_isins: list[str] = None):
    fields = {"Table": table, "Affected rows": str(count)}
    if sample_isins:
        fields["Sample ISINs"] = ", ".join(sample_isins[:5])
    await send_alert(
        title="Data Quality Issue",
        message=issue,
        level="warning",
        fields=fields,
    )


async def alert_upload_success(source: str, portfolio_id: str, date: str,
                                bonds_parsed: int, filename: str):
    await send_alert(
        title=f"{source.upper()} Upload OK",
        message=f"{bonds_parsed} bonds parsed and stored.",
        level="info",
        fields={
            "Portfolio": portfolio_id,
            "Date": date,
            "File": filename,
        },
    )
