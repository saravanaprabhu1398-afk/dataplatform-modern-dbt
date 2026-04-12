"""SLA alert delivery — webhook and email.

Configured via the ``sla`` block in the pipeline YAML:

    sla:
      max_duration_minutes: 30
      alert:
        type: webhook
        webhook_url: https://hooks.slack.com/services/…

    # — or —

    sla:
      max_duration_minutes: 30
      alert:
        type: email
        email: data-team@company.com

Email requires these environment variables:
    ALERT_SMTP_SERVER   (default: smtp.gmail.com)
    ALERT_SMTP_PORT     (default: 587)
    ALERT_SMTP_USER     (sender address)
    ALERT_SMTP_PASSWORD (sender password / app password)
"""
import json
import logging
import os
import smtplib
import urllib.request
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

from dataplatform.core.config import SLAConfig
from dataplatform.core.database import save_sla_violation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delivery functions
# ---------------------------------------------------------------------------

def send_webhook(url: str, payload: Dict[str, Any], timeout: int = 10) -> bool:
    """POST *payload* as JSON to *url*.  Returns True on success."""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=timeout)
        logger.info("Webhook alert sent to %s", url)
        return True
    except Exception as exc:
        logger.error("Webhook alert failed (url=%s): %s", url, exc)
        return False


def send_email(
    to: str,
    subject: str,
    body: str,
    smtp_server: Optional[str] = None,
    smtp_port: Optional[int] = None,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
) -> bool:
    """Send a plain-text email alert.  Returns True on success.

    Credentials are read from environment variables if not passed directly:
        ALERT_SMTP_SERVER, ALERT_SMTP_PORT, ALERT_SMTP_USER, ALERT_SMTP_PASSWORD
    """
    server = smtp_server or os.getenv("ALERT_SMTP_SERVER", "smtp.gmail.com")
    port = smtp_port or int(os.getenv("ALERT_SMTP_PORT", "587"))
    user = smtp_user or os.getenv("ALERT_SMTP_USER")
    password = smtp_password or os.getenv("ALERT_SMTP_PASSWORD")

    if not user or not password:
        logger.warning(
            "Email alert skipped: ALERT_SMTP_USER / ALERT_SMTP_PASSWORD not set. "
            "Configure these environment variables to enable email alerts."
        )
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to

    try:
        with smtplib.SMTP(server, port, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(user, password)
            s.sendmail(user, [to], msg.as_string())
        logger.info("Email alert sent to %s", to)
        return True
    except Exception as exc:
        logger.error("Email alert failed (to=%s): %s", to, exc)
        return False


# ---------------------------------------------------------------------------
# SLA checking
# ---------------------------------------------------------------------------

def check_sla_and_alert(
    pipeline_name: str,
    run_id: str,
    duration_seconds: float,
    sla: SLAConfig,
) -> bool:
    """Check whether *duration_seconds* exceeds the SLA limit.

    If violated:
      - Records the violation in the DB.
      - Sends the configured alert (webhook or email).

    Returns True if the SLA was violated, False otherwise.
    """
    limit_seconds = sla.max_duration_minutes * 60.0

    if duration_seconds <= limit_seconds:
        return False

    logger.warning(
        "SLA VIOLATION — pipeline '%s': ran for %.1fs (limit %.1fs / %.1f min)",
        pipeline_name, duration_seconds, limit_seconds, sla.max_duration_minutes,
    )

    # Attempt to send the alert and note whether it succeeded
    alerted = False
    if sla.alert:
        subject = f"[DataPlatform] SLA Violation: {pipeline_name}"
        body = (
            f"Pipeline '{pipeline_name}' exceeded its SLA.\n\n"
            f"  Duration : {duration_seconds:.1f} seconds ({duration_seconds / 60:.1f} minutes)\n"
            f"  SLA limit: {limit_seconds:.0f} seconds ({sla.max_duration_minutes:.1f} minutes)\n\n"
            f"Run ID: {run_id}"
        )
        payload = {
            "alert": "sla_violation",
            "pipeline": pipeline_name,
            "run_id": run_id,
            "duration_seconds": round(duration_seconds, 2),
            "sla_limit_seconds": limit_seconds,
            "message": body,
        }

        if sla.alert.type == "webhook" and sla.alert.webhook_url:
            alerted = send_webhook(sla.alert.webhook_url, payload)
        elif sla.alert.type == "email" and sla.alert.email:
            alerted = send_email(sla.alert.email, subject, body)

    save_sla_violation(
        run_id=run_id,
        pipeline_name=pipeline_name,
        duration_seconds=duration_seconds,
        limit_seconds=limit_seconds,
        alerted=alerted,
    )

    return True
