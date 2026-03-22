"""
SMTP email service.

Env vars required:
  SMTP_HOST      — e.g. smtp.gmail.com / smtp.sendgrid.net
  SMTP_PORT      — e.g. 587 (STARTTLS)
  SMTP_USER      — your SMTP username / email
  SMTP_PASSWORD  — your SMTP password / API key
  SMTP_FROM      — sender address, e.g. noreply@socratic.pro
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST     = os.getenv("SMTP_HOST", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM     = os.getenv("SMTP_FROM", "noreply@socratic.pro")


def send_otp_email(to_email: str, otp_code: str) -> None:
    """
    Send an OTP email. Called via asyncio.to_thread() from async routes.
    Raises RuntimeError if SMTP is not configured or sending fails.
    """
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASSWORD]):
        # Dev fallback — just log the OTP so you can test without SMTP
        logger.warning(f"SMTP not configured. OTP for {to_email}: {otp_code}")
        return

    subject = "Your Socratic.pro login code"

    html = f"""
    <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
      <h2 style="color: #1a1a1a;">Your login code</h2>
      <p style="color: #555;">Use the code below to sign in to Socratic.pro.
         It expires in <strong>10 minutes</strong>.</p>
      <div style="font-size: 36px; font-weight: bold; letter-spacing: 8px;
                  text-align: center; padding: 24px; background: #f5f5f5;
                  border-radius: 8px; margin: 24px 0;">
        {otp_code}
      </div>
      <p style="color: #999; font-size: 12px;">
        If you didn't request this, you can safely ignore this email.
      </p>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_FROM
    msg["To"]      = to_email
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, to_email, msg.as_string())
        logger.info(f"OTP email sent to {to_email}")
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send OTP email to {to_email}: {e}")
        raise RuntimeError(f"Email delivery failed: {e}")
