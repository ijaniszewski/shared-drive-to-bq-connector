import smtplib
import ssl
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

MAIL_PORT = ''
MAIL_HOST = ''
MAIL_LOGIN = ''
MAIL_PASSWORD = ''
MAIL_SENDER = ''


@dataclass
class SendMail:
    recipients: List

    def create_message(self, subject, body):
        message = MIMEMultipart()
        message["From"] = MAIL_SENDER
        message["To"] = ", ".join(self.recipients)
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        return message

    def add_attachment(self, message, filename):
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )
        message.attach(part)
        return message

    def send_mail(self, message):
        text = message.as_string()
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT, context=context) as server:
            server.login(MAIL_LOGIN, MAIL_PASSWORD)
            server.sendmail(MAIL_SENDER, self.recipients, text)
