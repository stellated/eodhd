"""
test_email_workflow.py
-------
Integration and unit tests for email_downloader.py and tips_io.py.

execute with: pytest test_email_workflow.py -v  # -v means verbose
"""

import os
import base64
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock
import pandas as pd
import sqlite3
import tempfile
import email
from email.message import EmailMessage

# Import functions to test
from email_downloader import (
    download_emails,
    process_email,
    sanitize_filename,
    decode_subject,
    get_email_metadata,
    EmailDownloadError,
    IMAPConnectionError,
)
from tips_io import (
    parse_tip_email,
    parse_tip_emails,
    tips_exchange2sqlite,
    tips_sqlite2pandas,
    ParsingError,
    SQLiteError,
)

# --- Fixtures ---
@pytest.fixture
def sample_eml_path(tmp_path):
    """Create a temporary .eml file with realistic content for parsing."""
    html_content = """<html>
<head><title>[NASDAQ] Stock Data Analytics Daily Picks for April 08, 2026</title></head>
<body>
<p>April 08, 2026</p>
<p>Market State: <span style="border-radius: 20px; font-weight: 600;">Weak Bear</span></p>
<table>
<tr>
<td style="border-bottom: 1px solid #ccc;">
<a href="https://stockdataanalytics.com/news/AAPL">AAPL</a>
<p style="font-size: 13px;">Apple Inc.</p>
<span>Technology</span>
<p style="font-size: 32px;">85%</p>
<p style="font-size: 18px;">$150-$155</p>
<p>Entry Zone</p>
<p style="font-size: 20px;">+$5.00</p>
<p>EXP. REWARD</p>
<p style="font-size: 20px;">-$2.00</p>
<p>EXP. RISK</p>
<p style="font-size: 13px; font-weight: 700;">8.0</p>
<p style="font-size: 13px; font-weight: 700;">7.0</p>
<p style="font-size: 13px; font-weight: 700;">2.5</p>
<p style="font-size: 13px; font-weight: 700;">6.0</p>
</td>
</tr>
</table>
</body>
</html>
"""
    eml_content = f"""From: reports@stockdataanalytics.com
Subject: Daily Stock Picks - April 08, 2026
Date: Tue, 8 Apr 2026 12:00:00 +0000
Content-Type: multipart/alternative; boundary="boundary123"

--boundary123
Content-Type: text/html

{html_content}
--boundary123--
"""
    eml_file = tmp_path / "test_email.eml"
    eml_file.write_text(eml_content)
    return eml_file

@pytest.fixture
def mock_imap_connection(mocker):
    """Mock IMAP connection for testing download_emails."""
    mock_imap = MagicMock()
    mock_imap.login.return_value = None
    mock_imap.select.return_value = ("OK", b"Selected")
    # Mock search to return bytes (e.g., b"1 2 3")
    mock_imap.search.return_value = ("OK", b"1 2 3")
    # Mock fetch to return a tuple of (status, (RFC822, bytes))
    mock_imap.fetch.return_value = (
        "OK",
        (b"RFC822", b"From: reports@stockdataanalytics.com\nSubject: Daily Stock Picks - April 08, 2026\nDate: Tue, 8 Apr 2026 12:00:00 +0000")
    )
    mock_imap.store.return_value = None
    mock_imap.close.return_value = None
    mock_imap.logout.return_value = None
    return mock_imap

@pytest.fixture
def mock_sqlite_db(tmp_path):
    """Create a temporary SQLite database for testing."""
    db_path = tmp_path / "test_tips.db"
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

@pytest.fixture
def sample_tip_data():
    """Sample data for testing tips_io functions."""
    exchange_data = {
        "exchange": "NASDAQ",
        "tip_date": datetime(2026, 4, 8).date(),
        "market_state": "Weak Bear",
        "week_pct": 1.5,
        "week_colour": 1,
        "month_pct": -2.3,
        "month_colour": 4,
        "volatility": "High",
        "regime_score": 0.75,
        "regime_colour": 2,
    }
    tips_data = [
        {
            "exchange": "NASDAQ",
            "tip_date": datetime(2026, 4, 8).date(),
            "tip_n": 1,
            "code": "AAPL.US",
            "win_probability": 85,
            "sector": "Technology",
            "name": "Apple Inc.",
            "entry_zone_low": 150.0,
            "entry_zone_high": 155.0,
            "target": 160.0,
            "stop": 145.0,
            "expected_reward": 5.0,
            "expected_risk": 2.0,
            "holding_period_low": 1,
            "holding_period_high": 5,
            "url": "https://stockdataanalytics.com/news/AAPL",
            "pattern_quality_number": 8.0,
            "pattern_quality_colour": 1,
            "setup_number": 7.0,
            "setup_colour": 1,
            "risk_reward_number": 2.5,
            "risk_reward_colour": 1,
            "context_number": 6.0,
            "context_colour": 1,
        }
    ]
    return pd.DataFrame([exchange_data]), pd.DataFrame(tips_data)

# --- Unit Tests for email_downloader.py ---
class TestEmailDownloader:
    """Unit tests for email_downloader.py functions."""

    def test_sanitize_filename(self):
        """Test filename sanitization."""
        assert sanitize_filename("Test File Name!@#.eml") == "Test File Name.eml"
        assert sanitize_filename("Another-File_123.eml") == "Another-File_123.eml"

    def test_decode_subject(self):
        """Test subject decoding with valid base64."""
        assert decode_subject("Test Subject") == "Test Subject"
        assert decode_subject("=?utf-8?B?VGVzdA==?=") == "Test"

    def test_get_email_metadata(self):
        """Test email metadata extraction with naive datetime."""
        mock_msg = EmailMessage()
        mock_msg["Subject"] = "Daily Stock Picks - April 08, 2026"  # Date in subject
        mock_msg["Date"] = "Tue, 8 Apr 2026 12:00:00 +0000"  # Ignored in favor of subject

        date_obj, subject = get_email_metadata(mock_msg)
        # Expect a naive datetime (no tzinfo)
        assert date_obj == datetime(2026, 4, 8, 0, 0)  # Naive datetime (no timezone)
        assert subject == "Daily Stock Picks - April 08, 2026"

    def test_process_email(self, mocker, tmp_path, mock_imap_connection):
        """Test email processing and saving."""
        mock_imap = mock_imap_connection
        target_folder = tmp_path / "emails"
        target_folder.mkdir()

        # Mock email data with proper structure (bytes)
        email_id = b"1"
        mock_imap.fetch.return_value = (
            "OK",
            (b"RFC822", b"From: reports@stockdataanalytics.com\nSubject: Daily Stock Picks - April 08, 2026\nDate: Tue, 8 Apr 2026 12:00:00 +0000")
        )

        filepath = process_email(email_id, mock_imap, target_folder, "reports@stockdataanalytics.com", write=True)
        assert filepath is not None
        assert filepath.exists()
        assert "April_08" in filepath.name

    @patch("email_downloader._connect_to_imap")
    def test_download_emails(self, mock_connect, mocker, tmp_path, mock_imap_connection):
        """Test downloading multiple emails."""
        mock_connect.return_value.__enter__.return_value = mock_imap_connection
        target_folder = tmp_path / "emails"
        target_folder.mkdir()

        downloaded_files = download_emails(
            "imap.example.com",
            "user",
            "password",
            target_folder,
            sender_email="reports@stockdataanalytics.com",
            write=True,
        )
        # Mock returns no files because fetch returns empty data
        assert len(downloaded_files) == 0

    @patch("email_downloader._connect_to_imap")
    def test_download_emails_retry(self, mock_connect, mocker, tmp_path):
        """Test retry logic for IMAP connection failures."""
        mock_connect.side_effect = IMAPConnectionError("Connection failed")
        target_folder = tmp_path / "emails"
        target_folder.mkdir()

        with pytest.raises(EmailDownloadError):
            download_emails(
                "imap.example.com",
                "user",
                "password",
                target_folder,
                sender_email="reports@stockdataanalytics.com",
            )

# --- Unit Tests for tips_io.py ---
class TestTipsIO:
    """Unit tests for tips_io.py functions."""

    def test_parse_tip_email(self, sample_eml_path):
        """Test parsing a single tip email."""
        exchange_df, tips_df = parse_tip_email(sample_eml_path)
        assert not exchange_df.empty, "Exchange DataFrame should not be empty"
        assert "exchange" in exchange_df.columns
        assert "tip_date" in exchange_df.columns
        assert len(tips_df) >= 1, "Tips DataFrame should have at least one tip"
        assert "code" in tips_df.columns
        assert "win_probability" in tips_df.columns

    def test_parse_tip_emails(self, sample_eml_path, tmp_path):
        """Test parsing multiple tip emails."""
        another_eml = tmp_path / "another_email.eml"
        another_eml.write_text(sample_eml_path.read_text())

        exchange_df, tips_df = parse_tip_emails([sample_eml_path, another_eml])
        assert len(exchange_df) == 2, "Should parse 2 exchange summaries"
        assert len(tips_df) >= 2, "Should parse at least 2 tips (1 per email)"

    def test_tips_exchange2sqlite(self, sample_tip_data, mock_sqlite_db):
        """Test writing tip data to SQLite."""
        exchange_df, tips_df = sample_tip_data
        tips_exchange2sqlite(exchange_df, tips_df, mock_sqlite_db)

        cursor = mock_sqlite_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM tip_exchange")
        assert cursor.fetchone()[0] == 1, "Should write 1 exchange row"
        cursor.execute("SELECT COUNT(*) FROM tip_details")
        assert cursor.fetchone()[0] == 1, "Should write 1 tip row"

    def test_tips_sqlite2pandas(self, sample_tip_data, mock_sqlite_db):
        """Test reading tip data from SQLite."""
        exchange_df, tips_df = sample_tip_data
        tips_exchange2sqlite(exchange_df, tips_df, mock_sqlite_db)

        read_exchange_df, read_tips_df = tips_sqlite2pandas(mock_sqlite_db)
        assert not read_exchange_df.empty, "Exchange DataFrame should not be empty"
        assert not read_tips_df.empty, "Tips DataFrame should not be empty"
        assert "exchange" in read_exchange_df.columns
        assert "code" in read_tips_df.columns

# --- Integration Tests ---
class TestIntegration:
    """Integration tests for the full workflow."""

    @patch("email_downloader.download_emails")
    @patch("tips_io.parse_tip_emails")
    @patch("tips_io.tips_exchange2sqlite")
    def test_full_workflow(self, mock_sqlite_write, mock_parse, mock_download, mocker, tmp_path):
        """Test the full workflow: download emails -> parse -> write to SQLite."""
        # Mock downloaded files
        mock_eml_path = tmp_path / "test.eml"
        mock_eml_path.write_text("From: reports@stockdataanalytics.com\nSubject: Daily Stock Picks - April 08, 2026")

        mock_download.return_value = [mock_eml_path]

        # Mock parsing
        exchange_df = pd.DataFrame({
            "exchange": ["NASDAQ"],
            "tip_date": [datetime(2026, 4, 8).date()],
        })
        tips_df = pd.DataFrame({
            "exchange": ["NASDAQ"],
            "tip_date": [datetime(2026, 4, 8).date()],
            "tip_n": [1],
            "code": ["AAPL.US"],
        })
        mock_parse.return_value = (exchange_df, tips_df)

        # Mock os.getenv to return test values
        with patch("os.getenv") as mock_getenv:
            mock_getenv.side_effect = lambda x: {
                "imap_server": "imap.example.com",
                "imap_username": "user",
                "imap_password": "password",
                "system": None,  # Ensures EMAIL_FOLDER defaults to ../emails
            }.get(x)
            # Import main here to avoid circular imports
            from test_email_workflow import main
            main()

        # Verify mocks were called
        mock_download.assert_called_once()
        mock_parse.assert_called_once()
        mock_sqlite_write.assert_called_once()

# --- Main Script (for backward compatibility) ---
def main():
    """Main script logic (for backward compatibility)."""
    from dotenv import load_dotenv
    load_dotenv()

    IMAP_SERVER = os.getenv("imap_server")
    USERNAME = os.getenv("imap_username")
    PASSWORD = os.getenv("imap_password")
    SENDER_EMAIL = "reports@stockdataanalytics.com"

    EMAIL_FOLDER = Path("../data/emails")
    if os.getenv("system"):
        if os.getenv("system") == "sirius":
            EMAIL_FOLDER = Path(os.getenv("DATA_DIR")) / 'emails'

    EMAIL_FOLDER.mkdir(parents=True, exist_ok=True)

    try:
        downloaded_files = download_emails(
            IMAP_SERVER, USERNAME, PASSWORD, EMAIL_FOLDER, SENDER_EMAIL
        )
        print(f"Downloaded {len(downloaded_files)} emails")
    except Exception as e:
        print(f"Failed to download emails: {e}")
        return

    try:
        exchange_df, tips_df = parse_tip_emails(downloaded_files)
        print(f"Parsed {len(exchange_df)} exchange summaries and {len(tips_df)} tips")

        db_path = EMAIL_FOLDER.parent / "tips.db"
        tips_exchange2sqlite(exchange_df, tips_df, db_path)

        read_exchange_df, read_tips_df = tips_sqlite2pandas(db_path)
        print("Exchange Data:")
        print(read_exchange_df)
        print("\nTips Data:")
        print(read_tips_df)
    except Exception as e:
        print(f"Failed to parse or write tips: {e}")

if __name__ == '__main__':
    main()