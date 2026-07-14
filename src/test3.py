from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
from email_downloader import *
from tips_io import parse_tip_email, parse_tip_emails, tips_exchange2sqlite, tips_sqlite2pandas


load_dotenv()

IMAP_SERVER = os.environ["imap_server"]
USERNAME = os.environ["imap_username"]
PASSWORD = os.environ["imap_password"]
SENDER_EMAIL = "reports@stockdataanalytics.com"

EMAIL_FOLDER = Path("../emails")  # default
print(os.getenv("system"))
if os.getenv("system"):
    if os.getenv("system") == "sirius":
        EMAIL_FOLDER = Path(os.getenv("DATA_DIR")) / 'emails'
else:
    print("os.getenv('system') does not exist")
print("EMAIL_FOLDER", trim_dir(EMAIL_FOLDER))
# Create target folder if it doesn't exist
if EMAIL_FOLDER.is_dir():
    print(f"saving emails to: {trim_dir(EMAIL_FOLDER)}")
else:
    print(f"saving emails to: {trim_dir(EMAIL_FOLDER)}, (which doesn't exist, creating now)")
    EMAIL_FOLDER.mkdir()

# download_emails() is good, moving on to next part
# download_emails(IMAP_SERVER, USERNAME, PASSWORD, EMAIL_FOLDER, SENDER_EMAIL)

# testing extraction of data from emails
for eml_file in sorted(list(EMAIL_FOLDER.glob("*.eml"))):
    print(eml_file.stem.split('_')[0])
    file_date = datetime.strptime(eml_file.stem.split("_")[0], "%Y-%m-%d")
    if datetime.today() - file_date > timedelta(days=30):
        continue
    print('*', eml_file)
    exchange_df, tips_df = parse_tip_email(eml_file)
    print('exchange_df')
    with pd.option_context('display.max_columns', None):
        print(exchange_df)
    print()
    print('tips_df')
    with pd.option_context('display.max_columns', None):
        print(tips_df)
    raise Exception