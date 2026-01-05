import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Import centralized logging and configuration
from . import config
from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)


def style_dataframe(df):
    # Inline styles to match the provided table
    base_style = "border-spacing:0px; border-collapse:collapse; vertical-align:middle; text-align:right; padding:4px 8px; border:none;"
    table_style = (
        base_style
        + "color:rgb(56,58,66); font-family:'Segoe WPC', 'Segoe UI', sans-serif; font-size:14px; background-color:rgb(232,232,232);"
    )
    header_style = (
        base_style + "font-weight:bold; background-color:rgba(130,130,130,0.16);"
    )
    cell_style = base_style

    # Convert DataFrame to HTML
    styled_html = df.to_html(index=True, border=0)

    # Replace the opening table tag with styled version
    styled_html = styled_html.replace(
        '<table border="0" class="dataframe">', f'<table style="{table_style}">'
    )

    # Replace the thead and tbody tags with styled versions
    styled_html = styled_html.replace("<thead>", f'<thead style="{header_style}">')
    styled_html = styled_html.replace("<tbody>", f'<tbody style="{cell_style}">')

    # Apply styles to each row and cell
    styled_html = styled_html.replace("<tr>", f'<tr style="{base_style}">')
    styled_html = styled_html.replace("<th>", f'<th style="{header_style}">')
    styled_html = styled_html.replace("<td>", f'<td style="{cell_style}">')

    # Ensure all borders are removed
    styled_html = styled_html.replace('border="1"', 'border="0"')

    return styled_html


def send_email(
    df,
    receiver_email,
    subject,
    sender_email=None,
    email_password=None,
    cc_emails=None,
):
    # Use environment variables if not provided
    if sender_email is None:
        sender_email = config.EMAIL_CONFIG["sender_email"]
    if email_password is None:
        email_password = config.EMAIL_CONFIG["password"]

    # Check global email setting
    from src import settings_manager
    if not settings_manager.get_setting("email_enabled", True):
        logger.warning("[EMAIL] Email sending is disabled in settings. Skipping email.")
        return

    # Apply the style to your dataframe and convert to HTML
    html = style_dataframe(df)

    # Email message setup
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # If CC recipients are provided, add them to the message header
    if cc_emails:
        message["Cc"] = ", ".join(cc_emails)
        receiver_email = [receiver_email] + cc_emails
    else:
        receiver_email = [receiver_email]

    # Attach the HTML part
    html_part = MIMEText(html, "html")
    message.attach(html_part)

    # SMTP server configuration
    try:
        logger.info(f"[EMAIL] Sending email to {receiver_email} with subject: {subject}")
        server = smtplib.SMTP(config.EMAIL_CONFIG["smtp_host"], config.EMAIL_CONFIG["smtp_port"])
        server.starttls()
        server.login(sender_email, email_password)
        server.send_message(message, from_addr=sender_email, to_addrs=receiver_email)
        server.quit()
        logger.info(f"[EMAIL] Email sent successfully to {receiver_email}")
    except Exception as e:
        logger.error(f"[EMAIL] Failed to send email: {str(e)}")
