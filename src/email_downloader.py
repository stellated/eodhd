import imaplib
import email
import re
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
from dotenv import load_dotenv
from email.header import decode_header
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_TARGET_FOLDER = Path("../data/sample_emails")
DEFAULT_SENDER_EMAIL = "reports@stockdataanalytics.com"
IMAP_FOLDER = "Inbox/SDA"
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2

class EmailDownloadError(Exception):
    """Custom exception for email download failures."""
    pass

class IMAPConnectionError(Exception):
    """Custom exception for IMAP connection failures."""
    pass

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_SECONDS, max=10),
    retry=retry_if_exception_type(imaplib.IMAP4.error),
)
def _connect_to_imap(imap_server: str, username: str, password: str) -> imaplib.IMAP4_SSL:
    """Connect to an IMAP server with retry logic."""
    try:
        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(username, password)
        return mail
    except imaplib.IMAP4.error as e:
        logger.warning("IMAP connection failed: %s. Retrying...", e)
        raise IMAPConnectionError(f"Failed to connect to IMAP server: {e}")

def trim_dir(folder: Path) -> str:
    """Trim the directory path for cleaner logging output."""
    return str(folder).replace("/Users/ianatkinson/Library/CloudStorage/OneDrive-Personal", "~")

def sanitize_filename(filename: str) -> str:
    """Sanitize the filename to remove invalid characters."""
    return re.sub(r'[^a-zA-Z0-9\s\-_.]', '', filename)  # Allow letters, numbers, spaces, hyphens, underscores, and dots

def decode_subject(subject: str) -> str:
    """Decode an email subject header, handling MIME-encoded subjects."""
    decoded_parts = decode_header(subject)
    decoded_subject = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            decoded_subject.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            decoded_subject.append(str(part))
    return "".join(decoded_subject)

def get_email_metadata(email_message: email.message.Message) -> Tuple[Optional[datetime], str]:
    """
    Extract date and subject from an email message.
    Tries to parse the date from:
    1. The subject line (e.g., "April 09, 2026").
    2. The standard Date header (unlikely to exist in your case).
    3. The first Received header (fallback).
    """
    subject = email_message["Subject"] or "no_subject"
    decoded_subject = decode_subject(subject)

    # Try to extract date from subject (e.g., "April 09, 2026")
    date_match = re.search(
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b',
        decoded_subject
    )
    if date_match:
        try:
            date_str = date_match.group(0)
            date_obj = datetime.strptime(date_str, "%B %d, %Y")
            return date_obj, decoded_subject
        except ValueError as e:
            logger.warning("Failed to parse date from subject '%s': %s", decoded_subject, e)

    # Try standard Date header (unlikely to exist, but for completeness)
    date_str = email_message["Date"]
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            return date_obj.replace(tzinfo=None), decoded_subject
        except ValueError as e:
            logger.warning("Failed to parse Date header '%s': %s", date_str, e)

    # Try first Received header (fallback)
    received_headers = email_message.get_all("Received", [])
    if received_headers:
        try:
            # Example: "Thu, 9 Apr 2026 16:37:05 +1000"
            first_received = received_headers[0]
            date_match = re.search(
                r'\w{3},\s+\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+[+-]\d{4}',
                first_received
            )
            if date_match:
                date_str = date_match.group(0)
                date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                return date_obj.replace(tzinfo=None), decoded_subject
        except (ValueError, AttributeError) as e:
            logger.warning("Failed to parse Received header: %s", e)

    return None, decoded_subject  # No date found

def process_email(
    email_id: bytes,
    mail: imaplib.IMAP4_SSL,
    target_folder: Path,
    sender_email: str,
    write: bool = True,
) -> Optional[Path]:
    """
    Process a single email: fetch, parse, and save to disk.
    Args:
        email_id: IMAP email ID.
        mail: IMAP connection.
        target_folder: Directory to save the email.
        sender_email: Expected sender email (for validation).
        write: If True, save the email to disk.
    Returns:
        Path to the saved email file, or None if skipped/failed.
    """
    try:
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        if status != "OK":
            logger.warning("Failed to fetch email ID: %s", email_id)
            return None

        # Correctly extract the email bytes from the IMAP response
        # msg_data is a tuple like: (b'RFC822', b'From: ...')
        # The actual email data is in msg_data[1] (the second element of the tuple)
        if isinstance(msg_data, tuple) and len(msg_data) > 1:
            raw_email = msg_data[1]
            if isinstance(raw_email, tuple):
                # If raw_email is a tuple (e.g., (b'RFC822', b'From: ...')), extract the second element
                raw_email = raw_email[1] if len(raw_email) > 1 else raw_email
        else:
            raw_email = msg_data

        email_message = email.message_from_bytes(raw_email)

        # Validate sender
        from_header = email_message.get("From", "")
        if sender_email.lower() not in from_header.lower():
            logger.warning("Skipping email from unexpected sender: %s", from_header)
            return None

        date_obj, subject = get_email_metadata(email_message)
        if date_obj is None:
            logger.warning("Skipping email with no date: %s", subject)
            return None

        date_str = date_obj.strftime("%Y-%m-%d_%H-%M-%S")
        decoded_subject = decode_subject(subject)
        sanitized_subject = sanitize_filename(decoded_subject)

        # Extract "Pick" prefix if present
        pick_ptr = sanitized_subject.find('Pick')
        clean_subject = sanitized_subject[:pick_ptr + 4].strip() if pick_ptr != -1 else sanitized_subject
        filename = f"{date_str}_{clean_subject}.eml"
        filepath = target_folder / filename

        # Skip if file already exists
        if filepath.exists():
            logger.info("Skipping %s (already exists)", filename)
            return None

        # Save the email to a file
        if write:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(raw_email)
            logger.info("Downloaded: %s to %s", filename, trim_dir(target_folder))
            # Mark as read
            mail.store(email_id, "+FLAGS", "\\Seen")
            return filepath
        else:
            logger.info("Not downloading: %s to %s (write=False)", filename, trim_dir(target_folder))
            return None

    except Exception as e:
        logger.error("Failed to process email ID %s: %s", email_id, e)
        raise EmailDownloadError(f"Email processing failed: {e}")

def download_emails(
    imap_server: str,
    username: str,
    password: str,
    target_folder: Path,
    sender_email: str = DEFAULT_SENDER_EMAIL,
    write: bool = True,
) -> List[Path]:
    """
    Download sample_emails from an IMAP server and save them to a target folder.
    Args:
        imap_server: IMAP server address.
        username: IMAP username.
        password: IMAP password.
        target_folder: Directory to save downloaded sample_emails.
        sender_email: Email address to filter messages.
        write: If True, save sample_emails to disk. If False, only log.
    Returns:
        List of paths to downloaded email files.
    """
    downloaded_files = []

    try:
        with _connect_to_imap(imap_server, username, password) as mail:
            mail.select(IMAP_FOLDER)

            # Search for sample_emails from the specified sender
            status, messages = mail.search(None, f'(FROM "{sender_email}")')
            if status != "OK":
                logger.warning("No messages found for sender: %s", sender_email)
                return downloaded_files

            # Correctly extract the message IDs from the IMAP response
            # messages is a tuple like: (b'1 2 3',)
            # The actual message IDs are in messages[0]
            if isinstance(messages, tuple) and len(messages) > 0:
                message_ids = messages[0]
            else:
                message_ids = messages

            if isinstance(message_ids, bytes):
                email_ids = message_ids.split()
            elif isinstance(message_ids, str):
                email_ids = message_ids.split()
            else:
                email_ids = []

            logger.info("Found %d messages from %s", len(email_ids), sender_email)

            for email_id in email_ids:
                try:
                    filepath = process_email(email_id, mail, target_folder, sender_email, write)
                    if filepath:
                        downloaded_files.append(filepath)
                except EmailDownloadError as e:
                    logger.error("Skipping email due to error: %s", e)
                    continue

    except IMAPConnectionError as e:
        logger.error("IMAP connection failed after retries: %s", e)
        raise EmailDownloadError(f"IMAP connection failed: {e}")
    except Exception as e:
        logger.error("Unexpected error during email download: %s", e)
        raise EmailDownloadError(f"Email download failed: {e}")

    return downloaded_files

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_SECONDS, max=10),
    retry=retry_if_exception_type(IMAPConnectionError),
)
def download_emails_with_retry(
    imap_server: str,
    username: str,
    password: str,
    target_folder: Path,
    sender_email: str = DEFAULT_SENDER_EMAIL,
    write: bool = True,
) -> List[Path]:
    """
    Wrapper for download_emails with retry logic for the entire operation.
    """
    return download_emails(imap_server, username, password, target_folder, sender_email, write)

if __name__ == '__main__':
    # Configuration
    load_dotenv()
    IMAP_SERVER = os.getenv("imap_server")
    USERNAME = os.getenv("imap_username")
    PASSWORD = os.getenv("imap_password")

    if not all([IMAP_SERVER, USERNAME, PASSWORD]):
        raise ValueError("Missing required environment variables: imap_server, imap_username, or imap_password")

    TARGET_FOLDER = DEFAULT_TARGET_FOLDER
    if os.getenv("system"):
        if os.getenv("system") == "sirius":
            TARGET_FOLDER = Path(os.getenv("DATA_DIR")) / 'sample_emails'

    # Create target folder if it doesn't exist
    TARGET_FOLDER.mkdir(parents=True, exist_ok=True)
    logger.info("Saving sample_emails to: %s", trim_dir(TARGET_FOLDER))

    # Run the script
    try:
        downloaded_files = download_emails_with_retry(
            IMAP_SERVER, USERNAME, PASSWORD, TARGET_FOLDER
        )
        logger.info("Downloaded %d sample_emails", len(downloaded_files))
    except Exception as e:
        logger.error("Script failed: %s", e)
        raise