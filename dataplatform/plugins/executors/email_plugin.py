import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)


class EmailExecutor:
    """Email executor for sending email notifications."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Send email.
        
        config = {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "sender_email": "your_email@gmail.com",
            "sender_password": "your_password",
            "recipients": ["user@example.com"],
            "subject": "Pipeline Alert",
            "body": "Pipeline execution completed",
            "html": False,
            "attachments": ["file1.txt", "file2.csv"]
        }
        """
        try:
            smtp_server = config.get("smtp_server", "smtp.gmail.com")
            smtp_port = config.get("smtp_port", 587)
            sender_email = config.get("sender_email")
            sender_password = config.get("sender_password")
            recipients = config.get("recipients", [])
            subject = config.get("subject", "Pipeline Notification")
            body = config.get("body", "")
            is_html = config.get("html", False)
            attachments = config.get("attachments", [])

            if not sender_email or not sender_password or not recipients:
                return False, {"error": "Missing email configuration"}

            # Create message
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ", ".join(recipients)

            # Attach body
            if is_html:
                message.attach(MIMEText(body, "html"))
            else:
                message.attach(MIMEText(body, "plain"))

            # Attach files
            for attachment in attachments:
                try:
                    self._attach_file(message, attachment)
                except Exception as e:
                    logger.warning(f"Failed to attach {attachment}: {e}")

            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipients, message.as_string())

            logger.info(f"✓ Email sent to {', '.join(recipients)}")

            return True, {
                "recipients": recipients,
                "subject": subject,
                "status": "sent"
            }

        except smtplib.SMTPAuthenticationError:
            logger.error("Email authentication failed")
            return False, {"error": "Email authentication failed"}
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False, {"error": str(e)}
        except Exception as e:
            logger.error(f"Email sending error: {e}")
            return False, {"error": str(e)}

    @staticmethod
    def _attach_file(message, file_path):
        """Attach file to email."""
        from email.mime.base import MIMEBase
        from email import encoders
        import os

        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(file_path)}"
        )
        message.attach(part)
