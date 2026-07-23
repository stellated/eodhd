"""
test_integration.py
--------------------
Integration tests for email_downloader.py and tips_io.py using real emails from ../emails/.

These tests validate that the code works with actual email data, complementing the unit tests
in test_email_workflow.py which use mocks and temporary files.

execute with: pytest test_integration.py -v  # -v means verbose
"""

import pytest
from pathlib import Path
import pandas as pd

from tips_io import (
    parse_tip_email,
    parse_tip_emails,
    tips_exchange2sqlite,
    tips_sqlite2pandas,
    ParsingError,
)


class TestRealEmails:
    """Integration tests using real emails from ../emails/."""

    @pytest.mark.skipif(not Path("../data/emails").exists(), reason="Real emails directory not found")
    def test_parse_real_emails(self):
        """Test parsing all real emails in ../emails/."""
        emails_dir = Path(__file__).parent.parent / "emails"
        eml_files = list(emails_dir.glob("*.eml"))
        assert len(eml_files) > 0, f"No .eml files found in {emails_dir}"

        for eml_file in eml_files:
            exchange_df, tips_df = parse_tip_email(eml_file)
            assert not exchange_df.empty, f"Failed to parse exchange data from {eml_file.name}"
            assert not tips_df.empty, f"No tips found in {eml_file.name}"
            assert "tip_date" in exchange_df.columns, f"Missing tip_date in {eml_file.name}"
            assert "code" in tips_df.columns, f"Missing code in {eml_file.name}"

    @pytest.mark.skipif(not Path("../data/emails").exists(), reason="Real emails directory not found")
    def test_parse_all_real_emails(self):
        """Test parse_tip_emails on all real emails."""
        emails_dir = Path(__file__).parent.parent / "emails"
        eml_files = list(emails_dir.glob("*.eml"))
        assert len(eml_files) > 0, f"No .eml files found in {emails_dir}"

        exchange_df, tips_df = parse_tip_emails(eml_files)
        assert len(exchange_df) == len(eml_files), f"Expected {len(eml_files)} exchange rows, got {len(exchange_df)}"
        assert len(tips_df) >= len(eml_files), f"Expected at least {len(eml_files)} tips, got {len(tips_df)}"

    @pytest.mark.skipif(not Path("../data/emails").exists(), reason="Real emails directory not found")
    @pytest.mark.slow
    def test_full_workflow_with_real_emails(self, tmp_path):
        """Test the full workflow: parse real emails -> write to SQLite -> read back."""
        emails_dir = Path(__file__).parent.parent / "emails"
        eml_files = list(emails_dir.glob("*.eml"))
        assert len(eml_files) > 0, f"No .eml files found in {emails_dir}"

        # Parse all real emails
        exchange_df, tips_df = parse_tip_emails(eml_files)
        assert not exchange_df.empty, "No exchange data parsed"
        assert not tips_df.empty, "No tips parsed"

        # Write to a temporary SQLite database
        db_path = tmp_path / "test_integration.db"
        tips_exchange2sqlite(exchange_df, tips_df, db_path)

        # Read back and validate
        read_exchange_df, read_tips_df = tips_sqlite2pandas(db_path)
        assert len(read_exchange_df) == len(exchange_df), "Exchange data mismatch after SQLite round-trip"
        assert len(read_tips_df) == len(tips_df), "Tips data mismatch after SQLite round-trip"

        # Validate columns
        assert "exchange" in read_exchange_df.columns, "Missing exchange column"
        assert "tip_date" in read_exchange_df.columns, "Missing tip_date column"
        assert "code" in read_tips_df.columns, "Missing code column"
        assert "win_probability" in read_tips_df.columns, "Missing win_probability column"