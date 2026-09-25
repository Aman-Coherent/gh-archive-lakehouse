"""Failure alerting: sends an actionable email via SMTP when a run fails.

WHY actionable, not just "job failed": an alert that forces the reader to go
dig through logs before they even know what broke is barely better than no
alert. Every email this sends names the hour, the step, the actual error
text, and a direct link to the run — the four things you'd need to start
fixing it without opening a second tab first.
"""

from __future__ import annotations

import smtplib
from email.message import EmailMessage

from pipeline.config import Settings


def send_failure_alert(*, hour: str, step: str, error: str, run_url: str) -> bool:
    """Send an actionable failure alert email to ALERT_EMAIL.

    Returns False (no-op) if ALERT_EMAIL isn't configured — alerting is
    opt-in, not a hard requirement to run the pipeline locally. Returns True
    once the message has been handed off to the SMTP server.
    """
    settings = Settings.load()
    if not settings.alert_email:
        return False

    message = EmailMessage()
    message["Subject"] = f"gh-archive-lakehouse: {step} failed for hour {hour}"
    message["From"] = settings.smtp_username or settings.alert_email
    message["To"] = settings.alert_email
    message.set_content(
        f"Hour:  {hour}\n"
        f"Step:  {step}\n"
        f"Error: {error}\n"
        f"Run:   {run_url}\n"
    )

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username and settings.smtp_password:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)

    return True
