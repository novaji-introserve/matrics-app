import asyncio

from fastapi_mail import ConnectionConfig, FastMail, MessageSchema, MessageType

from config.settings import load_settings


def _get_mail_config(mail_from_name: str | None = None) -> ConnectionConfig:
    settings = load_settings(refresh=True)
    return ConnectionConfig(
        MAIL_SERVER=settings.mail_server,
        MAIL_PORT=settings.mail_port,
        MAIL_USERNAME=settings.mail_username,
        MAIL_PASSWORD=settings.mail_password,
        MAIL_FROM=settings.mail_from,
        MAIL_FROM_NAME=mail_from_name or settings.mail_from_name,
        MAIL_STARTTLS=settings.mail_starttls,
        MAIL_SSL_TLS=settings.mail_ssl_tls,
        USE_CREDENTIALS=bool(settings.mail_username or settings.mail_password),
        VALIDATE_CERTS=settings.mail_validate_certs,
    )


async def send_email_async(
    subject: str,
    body: str,
    recipients: list[str],
    mail_from_name: str | None = None,
) -> None:
    message = MessageSchema(
        subject=subject,
        recipients=recipients,
        body=body,
        subtype=MessageType.html,
    )
    mailer = FastMail(_get_mail_config(mail_from_name=mail_from_name))
    await mailer.send_message(message)


def send_email(
    subject: str,
    body: str,
    recipients: list[str],
    mail_from_name: str | None = None,
) -> None:
    asyncio.run(
        send_email_async(
            subject=subject,
            body=body,
            recipients=recipients,
            mail_from_name=mail_from_name,
        )
    )
