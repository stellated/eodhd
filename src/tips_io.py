"""
tips_io.py
----------
Parsing and persistence helpers for StockDataAnalytics daily tip emails.

Parses .eml files into two pandas DataFrames:
  exchange_df  -- one row per email with exchange-level summary data
  tips_df      -- one row per tip (up to 20 per email)

Colour fields use an integer traffic-light scale:
  1 = green   (#22c55e)
  2 = yellow  (#eab308 / #ca8a04)
  3 = orange  (#f97316)
  4 = red     (#ef4444)

Public API
----------
parse_tip_email(eml_path)               -> (exchange_df, tips_df)
parse_tip_emails([paths])               -> (exchange_df, tips_df)  concatenated
tips_exchange2sqlite(exc, tips, db, ...) -> None
tips_sqlite2pandas(db, ..., start, end) -> (exchange_df, tips_df)

The tips() function (fetching OHLCV data around each tip date) lives in
eodhd_io.py and imports Database from this project.  tips_io.py has no
dependency on eodhd_io.py.
"""

from __future__ import annotations

import re
import pathlib
import sqlite3
from datetime import date, datetime
from typing import Optional, Union

import email as _email_module
from email import policy as _email_policy

from bs4 import BeautifulSoup
import pandas as pd

# ---------------------------------------------------------------------------
# Colour mapping (hex -> integer)
# ---------------------------------------------------------------------------

_COLOUR_INT: dict[str, int] = {
    "#22c55e": 1,   # green
    "#eab308": 2,   # yellow
    "#ca8a04": 2,   # dark yellow/amber (used for regime score text)
    "#f97316": 3,   # orange
    "#ef4444": 4,   # red
    "#854d0e": 2,   # dark amber (used in some badge backgrounds)
}


def _hex_to_int(hex_colour: Optional[str]) -> Optional[int]:
    """Convert a CSS hex colour to a traffic-light integer (1-4), or None."""
    if not hex_colour:
        return None
    return _COLOUR_INT.get(hex_colour.lower())


def _extract_colour(style: str) -> Optional[int]:
    """Extract the colour integer from a CSS style string (uses colour: property)."""
    m = re.search(r'\bcolour\s*:\s*(#[0-9a-fA-F]{6})', style)
    return _hex_to_int(m.group(1)) if m else None


def _extract_bg_colour(style: str) -> Optional[int]:
    """Extract the colour integer from the background/background-colour property."""
    m = re.search(r'background(?:-colour)?\s*:\s*(#[0-9a-fA-F]{6})', style)
    return _hex_to_int(m.group(1)) if m else None


def _clean(text: str) -> str:
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# HTML extraction from .eml
# ---------------------------------------------------------------------------

def _get_html(eml_path: pathlib.Path) -> bytes:
    """Extract the HTML body bytes from an .eml file."""
    with open(eml_path, "rb") as f:
        msg = _email_module.message_from_binary_file(f, policy=_email_policy.default)
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            return part.get_payload(decode=True)
    raise ValueError(f"No text/html part found in {eml_path}")


# ---------------------------------------------------------------------------
# Exchange summary parsing
# ---------------------------------------------------------------------------

def _parse_exchange(soup: BeautifulSoup, eml_path: pathlib.Path) -> dict:
    """Parse the exchange-level summary from the soup of one email."""

    # Exchange from <title>: "[NASDAQ] Stock Data Analytics..."
    title = soup.find("title")
    title_text = title.get_text(strip=True) if title else ""
    exchange_m = re.search(r'\[(NASDAQ|NYSE)\]', title_text)
    exchange = exchange_m.group(1) if exchange_m else None

    # Tip date from header paragraph "April 08, 2026"
    date_tag = soup.find("p", string=re.compile(
        r'(January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+\d{1,2},\s+\d{4}'
    ))
    tip_date = None
    if date_tag:
        try:
            tip_date = datetime.strptime(
                _clean(date_tag.get_text()), "%B %d, %Y"
            ).date()
        except ValueError:
            pass

    # Market state: the rounded badge span (e.g. "Weak Bear")
    market_state = None
    for span in soup.find_all("span", style=True):
        style = span.get("style", "")
        if "border-radius: 20px" in style and "font-weight: 600" in style:
            txt = _clean(span.get_text())
            if txt and txt not in ("Premium Tier",):
                market_state = txt
                break

    # Week percent, month percent, regime score — all in font-size: 16px spans
    week_pct = month_pct = vol_str = regime_score = None
    week_colour = month_colour = regime_colour = None

    spans_16 = [
        s for s in soup.find_all("span", style=True)
        if "16px" in s.get("style", "")
    ]
    for s in spans_16:
        txt = _clean(s.get_text())
        style = s.get("style", "")
        if re.match(r'^[+-]?\d+\.?\d*%$', txt):
            val = float(txt.replace('%', '').replace('+', ''))
            col = _extract_colour(style)
            if week_pct is None:
                week_pct = val
                week_colour = col
            elif month_pct is None:
                month_pct = val
                month_colour = col
        elif txt in ("Elevated", "Low", "Moderate", "High"):
            vol_str = txt
        elif re.match(r'^-?\d+\.\d+$', txt) and regime_score is None:
            regime_score = float(txt)
            regime_colour = _extract_colour(style)

    return {
        "exchange":      exchange,
        "tip_date":      tip_date,
        "market_state":  market_state,
        "week_pct":      week_pct,
        "week_colour":   week_colour,
        "month_pct":     month_pct,
        "month_colour":  month_colour,
        "volatility":    vol_str,
        "regime_score":  regime_score,
        "regime_colour": regime_colour,
    }


# ---------------------------------------------------------------------------
# Individual tip card parsing
# ---------------------------------------------------------------------------
def _parse_tip_card(card_td, tip_n: int) -> dict:
    """Parse one tip card <td> into a dict, handling two different HTML formats."""

    # --- Common parsing for both formats ---
    # Ticker + URL: the <a> pointing to stockdataanalytics.com/news/
    ticker_a = card_td.find("a", href=re.compile(r'stockdataanalytics\.com/news/'))
    ticker = _clean(ticker_a.get_text()) if ticker_a else None
    url = ticker_a["href"] if ticker_a else None
    code = f"{ticker}.US" if ticker else None

    # Initialize all fields as None
    result = {
        "tip_n": tip_n,
        "code": code,
        "win_probability": None,
        "sector": None,
        "name": None,
        "entry_zone_low": None,
        "entry_zone_high": None,
        "target": None,
        "stop": None,
        "expected_reward": None,
        "expected_risk": None,
        "holding_period_low": None,
        "holding_period_high": None,
        "url": url,
        "pattern_quality_number": None,
        "pattern_quality_colour": None,
        "setup_number": None,
        "setup_colour": None,
        "risk_reward_number": None,
        "risk_reward_colour": None,
        "context_number": None,
        "context_colour": None,
    }

    if tip_n <= 3:
        # --- Original format (first 3 tips) ---
        # Company name: <p> with font-size: 13px directly after ticker
        ticker_p = ticker_a.find_parent("p") if ticker_a else None
        if ticker_p:
            sib = ticker_p.find_next_sibling("p")
            if sib:
                result["name"] = _clean(sib.get_text())

        # Sector: inline-block span after name
        if ticker_p:
            span = ticker_p.find_next("span")
            if span:
                result["sector"] = _clean(span.get_text())

        # Win probability: large font-size: 32px paragraph
        for p in card_td.find_all("p", style=True):
            if "32px" in p.get("style", ""):
                txt = _clean(p.get_text()).replace("%", "")
                if txt.isdigit():
                    result["win_probability"] = int(txt)
                    break

        # Trade levels: Entry Zone / Target / Stop Loss
        for p in card_td.find_all("p", style=True):
            if "18px" in p.get("style", ""):
                sib = p.find_next_sibling("p")
                label = _clean(sib.get_text()) if sib else ""
                txt = _clean(p.get_text())
                if label == "Entry Zone":
                    m = re.match(r'\$?([\d.]+)-\$?([\d.]+)', txt)
                    if m:
                        result["entry_zone_low"] = float(m.group(1))
                        result["entry_zone_high"] = float(m.group(2))
                elif label == "Target":
                    entry = re.sub(r'[^\d.]', '', txt)
                    result["target"] = float(entry) if entry else None
                elif label == "Stop Loss":
                    entry = re.sub(r'[^\d.]', '', txt)
                    result["stop"] = float(entry) if entry else None

        # Exp. Reward / Exp. Risk / Hold Period: font-size: 20px paragraphs
        for p in card_td.find_all("p", style=True):
            if "20px" in p.get("style", ""):
                sib = p.find_next_sibling("p")
                label = _clean(sib.get_text()) if sib else ""
                txt = _clean(p.get_text())
                if "EXP. REWARD" in label.upper():
                    entry = re.sub(r'[^\d.]', '', txt)
                    result["expected_reward"] = float(entry) if entry else None
                elif "EXP. RISK" in label.upper():
                    entry = re.sub(r'[^\d.]', '', txt)
                    result["expected_risk"] = float(entry) if entry else None
                elif "HOLD PERIOD" in label.upper():
                    m = re.match(r'(\d+)-(\d+)', txt)
                    if m:
                        result["holding_period_low"] = int(m.group(1))
                        result["holding_period_high"] = int(m.group(2))

        # Score bars: four <p> tags with font-size: 13px font-weight: 700
        score_ps = [
            p for p in card_td.find_all("p", style=True)
            if "13px" in p.get("style", "") and "700" in p.get("style", "")
        ]
        if len(score_ps) >= 4:
            def _score_val(p):
                txt = _clean(p.get_text())
                try:
                    return float(txt)
                except:
                    return None

            result["pattern_quality_number"] = _score_val(score_ps[0])
            result["pattern_quality_colour"] = _extract_colour(score_ps[0]["style"])
            result["setup_number"] = _score_val(score_ps[1])
            result["setup_colour"] = _extract_colour(score_ps[1]["style"])
            result["risk_reward_number"] = _score_val(score_ps[2])
            result["risk_reward_colour"] = _extract_colour(score_ps[2]["style"])
            result["context_number"] = _score_val(score_ps[3])
            result["context_colour"] = _extract_colour(score_ps[3]["style"])

    else:
        # --- New format (tips 4 and above) ---

        # Win probability: in a coloured circle <span> with "Win" below it
        win_span = card_td.find("span", string=re.compile(r'\d+%'))
        if not win_span:
            # Look for the win probability in the gradient circle
            win_table = card_td.find("table", style=re.compile(r'background: linear-gradient'))
            if win_table:
                win_span = win_table.find("span", string=re.compile(r'\d+%'))

        if win_span:
            txt = _clean(win_span.get_text()).replace("%", "")
            if txt.isdigit():
                result["win_probability"] = int(txt)

        # Sector: inline as small text next to ticker
        if ticker_a:
            # Look for text nodes near the ticker link
            parent = ticker_a.parent
            if parent:
                for sibling in parent.next_siblings:
                    if hasattr(sibling, 'name'):
                        if sibling.name == "span":
                            text = _clean(sibling.get_text())
                            if text and len(text) < 50:
                                result["sector"] = text
                                break
                    elif hasattr(sibling, 'strip'):
                        text = sibling.strip()
                        if text and len(text) < 50:
                            result["sector"] = text
                            break

        # Reward/risk: shown as +$X.XX reward / -$X.XX risk in a single <p>
        for p in card_td.find_all("p"):
            txt = _clean(p.get_text())
            if "reward" in txt.lower() and "risk" in txt.lower():
                reward_match = re.search(r'\+\$([\d.]+)', txt)
                risk_match = re.search(r'-\$([\d.]+)', txt)
                if reward_match:
                    result["expected_reward"] = float(reward_match.group(1))
                if risk_match:
                    result["expected_risk"] = float(risk_match.group(1))
                break

        # Holding period: just 1-9d in blue inline
        for string in card_td.stripped_strings:
            if re.match(r'\d+-\d+d', string):
                m = re.match(r'(\d+)-(\d+)d', string)
                if m:
                    result["holding_period_low"] = int(m.group(1))
                    result["holding_period_high"] = int(m.group(2))
                    break

        # Entry zone, target, stop: in <p> tags with specific formats
        for p in card_td.find_all("p"):
            txt = _clean(p.get_text())

            # Entry Zone: $X-$Y
            entry_match = re.match(r'\$([\d.]+)-\$([\d.]+)', txt)
            if entry_match and result["entry_zone_low"] is None:
                result["entry_zone_low"] = float(entry_match.group(1))
                result["entry_zone_high"] = float(entry_match.group(2))

            # Target: Target: $Z
            target_match = re.match(r'Target:\s*\$([\d.]+)', txt)
            if target_match:
                result["target"] = float(target_match.group(1))

            # Stop: Stop: $Z
            stop_match = re.match(r'Stop:\s*\$([\d.]+)', txt)
            if stop_match:
                result["stop"] = float(stop_match.group(1))

        # For tips 4+, extract colors from the mini score bar tables
        # The structure is: <td><table><tr><td style="background: #color..."></td></tr></table><p>Label</p></td>
        score_labels = ["PQ", "Set", "R:R", "Ctx"]
        colour_mapping = {
            "PQ": "pattern_quality_colour",
            "Set": "setup_colour",
            "R:R": "risk_reward_colour",
            "Ctx": "context_colour"
        }

        # Find the mini score bars table
        score_table = card_td.find("table", {"width": "100%"}, recursive=False)
        if score_table:
            # Find all td elements that contain score bars
            score_tds = score_table.find_all("td", align="center", width="20")
            for score_td in score_tds:
                # Find the label (PQ, Set, R:R, Ctx) in the <p> tag
                label_p = score_td.find("p")
                if label_p:
                    label_text = _clean(label_p.get_text())
                    if label_text in score_labels:
                        # Find the inner table with the colored background
                        inner_table = score_td.find("table")
                        if inner_table:
                            # Find the td with the colored background (the second tr)
                            trs = inner_table.find_all("tr")
                            if len(trs) >= 2:
                                colored_td = trs[1].find("td")
                                if colored_td and colored_td.has_attr("style"):
                                    style = colored_td["style"]
                                    # Extract background color
                                    bg_match = re.search(r'background:\s*(#[0-9a-fA-F]{6})', style)
                                    if bg_match:
                                        colour = _hex_to_int(bg_match.group(1))
                                        if colour is not None:
                                            result[colour_mapping[label_text]] = colour

    return result
# def _parse_tip_card(card_td, tip_n: int) -> dict:
#     """Parse one tip card <td> into a dict."""
#
#     # Ticker + URL: the <a> pointing to stockdataanalytics.com/news/
#     ticker_a = card_td.find(
#         "a", href=re.compile(r'stockdataanalytics\.com/news/')
#     )
#     ticker = _clean(ticker_a.get_text()) if ticker_a else None
#     url    = ticker_a["href"] if ticker_a else None
#     code   = f"{ticker}.US" if ticker else None
#
#     # Company name: <p> with font-size: 13px directly after ticker
#     name = None
#     ticker_p = ticker_a.find_parent("p") if ticker_a else None
#     if ticker_p:
#         sib = ticker_p.find_next_sibling("p")
#         if sib:
#             name = _clean(sib.get_text())
#
#     # Sector: inline-block span after name
#     sector = None
#     if ticker_p:
#         span = ticker_p.find_next("span")
#         if span:
#             sector = _clean(span.get_text())
#
#     # Win probability: large font-size: 32px paragraph
#     win_prob = None
#     for p in card_td.find_all("p", style=True):
#         if "32px" in p.get("style", ""):
#             txt = _clean(p.get_text()).replace("%", "")
#             if txt.isdigit():
#                 win_prob = int(txt)
#                 break
#
#     # Trade levels: Entry Zone / Target / Stop Loss
#     entry_low = entry_high = target = stop = None
#     for p in card_td.find_all("p", style=True):
#         if "18px" in p.get("style", ""):
#             sib = p.find_next_sibling("p")
#             label = _clean(sib.get_text()) if sib else ""
#             txt   = _clean(p.get_text())
#             if label == "Entry Zone":
#                 m = re.match(r'\$?([\d.]+)-\$?([\d.]+)', txt)
#                 if m:
#                     entry_low  = float(m.group(1))
#                     entry_high = float(m.group(2))
#             elif label == "Target":
#                 entry = re.sub(r'[^\d.]', '', txt)
#                 target = float(entry) if entry else None
#             elif label == "Stop Loss":
#                 entry = re.sub(r'[^\d.]', '', txt)
#                 stop = float(entry) if entry else None
#
#     # Exp. Reward / Exp. Risk / Hold Period: font-size: 20px paragraphs
#     exp_reward = exp_risk = hold_low = hold_high = None
#     for p in card_td.find_all("p", style=True):
#         if "20px" in p.get("style", ""):
#             sib = p.find_next_sibling("p")
#             label = _clean(sib.get_text()) if sib else ""
#             txt   = _clean(p.get_text())
#             if "EXP. REWARD" in label.upper():
#                 entry = re.sub(r'[^\d.]', '', txt)
#                 exp_reward = float(entry) if entry else None
#             elif "EXP. RISK" in label.upper():
#                 entry = re.sub(r'[^\d.]', '', txt)
#                 exp_risk = float(entry) if entry else None
#             elif "HOLD PERIOD" in label.upper():
#                 m = re.match(r'(\d+)-(\d+)', txt)
#                 if m:
#                     hold_low  = int(m.group(1))
#                     hold_high = int(m.group(2))
#
#     # Score bars: four <p> tags with font-size: 13px font-weight: 700
#     # Order: Pattern Quality, Setup, Risk/Reward, Context
#     score_ps = [
#         p for p in card_td.find_all("p", style=True)
#         if "13px" in p.get("style", "") and "700" in p.get("style", "")
#     ]
#     pq_num = pq_col = setup_num = setup_col = None
#     rr_num = rr_col = ctx_num  = ctx_col   = None
#
#     if len(score_ps) >= 4:
#         def _score_val(p):
#             txt = _clean(p.get_text())
#             try:    return float(txt)
#             except: return None
#
#         pq_num,    pq_col    = _score_val(score_ps[0]), _extract_colour(score_ps[0]["style"])
#         setup_num, setup_col = _score_val(score_ps[1]), _extract_colour(score_ps[1]["style"])
#         rr_num,    rr_col    = _score_val(score_ps[2]), _extract_colour(score_ps[2]["style"])
#         ctx_num,   ctx_col   = _score_val(score_ps[3]), _extract_colour(score_ps[3]["style"])
#
#     return {
#         "tip_n":                  tip_n,
#         "code":                   code,
#         "win_probability":        win_prob,
#         "sector":                 sector,
#         "name":                   name,
#         "entry_zone_low":         entry_low,
#         "entry_zone_high":        entry_high,
#         "target":                 target,
#         "stop":                   stop,
#         "expected_reward":        exp_reward,
#         "expected_risk":          exp_risk,
#         "holding_period_low":     hold_low,
#         "holding_period_high":    hold_high,
#         "url":                    url,
#         "pattern_quality_number": pq_num,
#         "pattern_quality_colour": pq_col,
#         "setup_number":           setup_num,
#         "setup_colour":           setup_col,
#         "risk_reward_number":     rr_num,
#         "risk_reward_colour":     rr_col,
#         "context_number":         ctx_num,
#         "context_colour":         ctx_col,
#     }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_tip_email(
    eml_path: pathlib.Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse a single StockDataAnalytics tip email (.eml file).

    Returns
    -------
    (exchange_df, tips_df)
    exchange_df : 1-row DataFrame with exchange-level summary columns
    tips_df     : one row per tip (up to 20)

    Colour fields are integers: 1=green, 2=yellow, 3=orange, 4=red.
    All .US suffix is appended to ticker codes automatically.
    tip_n is 1-based position within the email.
    """
    html  = _get_html(eml_path)
    soup  = BeautifulSoup(html, "lxml")
    # for human testing
    open('../soup/soup.html', 'w').write(str(soup))


    # Exchange summary
    exc   = _parse_exchange(soup, eml_path)
    exchange_df = pd.DataFrame([exc])
    exchange_df["tip_date"] = pd.to_datetime(exchange_df["tip_date"]).dt.date

    # Tip cards: each is the outermost <td> that contains exactly one
    # unique stockdataanalytics detail link, found via the border-bottom style.
    def _find_card_td(a_tag):
        p = a_tag.parent
        best = None
        while p and p.name != "[document]":
            if p.name == "td" and "border-bottom" in p.get("style", ""):
                best = p
            p = p.parent
        return best

    all_links = soup.find_all(
        "a", href=re.compile(r'stockdataanalytics\.com/news/')
    )
    seen = set()
    tip_rows = []
    for a in all_links:
        href_key = a["href"].split("?")[0]
        if href_key in seen:
            continue
        seen.add(href_key)
        card_td = _find_card_td(a)
        if card_td is None:
            continue
        tip_n = len(tip_rows) + 1
        tip_rows.append(_parse_tip_card(card_td, tip_n))

    tips_df = pd.DataFrame(tip_rows)

    # Add tip_date and exchange so tips can be joined to exchange_df
    tips_df.insert(0, "tip_date", exc["tip_date"])
    tips_df.insert(0, "exchange", exc["exchange"])

    # Type coercions
    for col in ["win_probability", "holding_period_low", "holding_period_high",
                "pattern_quality_colour", "setup_colour",
                "risk_reward_colour", "context_colour"]:
        if col in tips_df.columns:
            tips_df[col] = pd.to_numeric(tips_df[col], errors="coerce") \
                             .astype("Int64")   # nullable int

    for col in ["entry_zone_low", "entry_zone_high", "target", "stop",
                "expected_reward", "expected_risk",
                "pattern_quality_number", "setup_number",
                "risk_reward_number", "context_number"]:
        if col in tips_df.columns:
            tips_df[col] = pd.to_numeric(tips_df[col], errors="coerce")

    return exchange_df, tips_df


def parse_tip_emails(
    eml_paths: list[pathlib.Path],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse multiple .eml files and concatenate results.

    Returns (exchange_df, tips_df) with rows from all emails.
    """
    exc_frames, tip_frames = [], []
    for path in eml_paths:
        e, t = parse_tip_email(path)
        exc_frames.append(e)
        tip_frames.append(t)
    exchange_df = pd.concat(exc_frames, ignore_index=True)
    tips_df     = pd.concat(tip_frames, ignore_index=True)
    return exchange_df, tips_df


# ---------------------------------------------------------------------------
# Tip email SQLite persistence
# ---------------------------------------------------------------------------

_DDL_TIP_EXCHANGE = """
CREATE TABLE IF NOT EXISTS {tablename} (
    exchange      TEXT    NOT NULL,
    tip_date      TEXT    NOT NULL,
    market_state  TEXT,
    week_pct      REAL,
    week_colour   INTEGER,
    month_pct     REAL,
    month_colour  INTEGER,
    volatility    TEXT,
    regime_score  REAL,
    regime_colour INTEGER,
    PRIMARY KEY (exchange, tip_date)
);
"""

_DDL_TIP_DETAILS = """
CREATE TABLE IF NOT EXISTS {tablename} (
    exchange                TEXT    NOT NULL,
    tip_date                TEXT    NOT NULL,
    tip_n                   INTEGER NOT NULL,
    code                    TEXT,
    win_probability         INTEGER,
    sector                  TEXT,
    name                    TEXT,
    entry_zone_low          REAL,
    entry_zone_high         REAL,
    target                  REAL,
    stop                    REAL,
    expected_reward         REAL,
    expected_risk           REAL,
    holding_period_low      INTEGER,
    holding_period_high     INTEGER,
    url                     TEXT,
    pattern_quality_number  REAL,
    pattern_quality_colour  INTEGER,
    setup_number            REAL,
    setup_colour            INTEGER,
    risk_reward_number      REAL,
    risk_reward_colour      INTEGER,
    context_number          REAL,
    context_colour          INTEGER,
    PRIMARY KEY (exchange, tip_date, tip_n)
);
"""

_INSERT_TIP_EXCHANGE = """
INSERT OR REPLACE INTO {tablename}
    (exchange, tip_date, market_state, week_pct, week_colour,
     month_pct, month_colour, volatility, regime_score, regime_colour)
VALUES
    (:exchange, :tip_date, :market_state, :week_pct, :week_colour,
     :month_pct, :month_colour, :volatility, :regime_score, :regime_colour);
"""

_INSERT_TIP_DETAILS = """
INSERT OR REPLACE INTO {tablename}
    (exchange, tip_date, tip_n, code, win_probability, sector, name,
     entry_zone_low, entry_zone_high, target, stop,
     expected_reward, expected_risk,
     holding_period_low, holding_period_high, url,
     pattern_quality_number, pattern_quality_colour,
     setup_number, setup_colour,
     risk_reward_number, risk_reward_colour,
     context_number, context_colour)
VALUES
    (:exchange, :tip_date, :tip_n, :code, :win_probability, :sector, :name,
     :entry_zone_low, :entry_zone_high, :target, :stop,
     :expected_reward, :expected_risk,
     :holding_period_low, :holding_period_high, :url,
     :pattern_quality_number, :pattern_quality_colour,
     :setup_number, :setup_colour,
     :risk_reward_number, :risk_reward_colour,
     :context_number, :context_colour);
"""


def tips_exchange2sqlite(
    exchange_df: pd.DataFrame,
    tips_df:     pd.DataFrame,
    db:          Union[sqlite3.Connection, str, pathlib.Path],
    exchange_tablename: str = "tip_exchange",
    tips_tablename:     str = "tip_details",
) -> None:
    """Write exchange_df and tips_df to SQLite.

    INSERT OR REPLACE — idempotent; safe to call after each email import.
    Primary key on exchange table: (exchange, tip_date).
    Primary key on tips table:     (exchange, tip_date, tip_n).
    """
    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        conn.execute(_DDL_TIP_EXCHANGE.format(tablename=exchange_tablename))
        conn.execute(_DDL_TIP_DETAILS.format(tablename=tips_tablename))

        for row in exchange_df.itertuples(index=False):
            d = dict(row._asdict())
            d["tip_date"] = row.tip_date.isoformat() \
                            if isinstance(row.tip_date, date) else str(row.tip_date)
            conn.execute(
                _INSERT_TIP_EXCHANGE.format(tablename=exchange_tablename), d
            )

        for row in tips_df.itertuples(index=False):
            d = dict(row._asdict())
            d["tip_date"] = row.tip_date.isoformat() \
                            if isinstance(row.tip_date, date) else str(row.tip_date)
            # Convert pandas NA / numpy int types to plain Python for sqlite3
            for k, v in d.items():
                try:
                    if pd.isna(v):
                        d[k] = None
                        continue
                except (TypeError, ValueError):
                    pass
                if hasattr(v, "item"):   # numpy/pandas scalar → Python native
                    d[k] = v.item()
            conn.execute(
                _INSERT_TIP_DETAILS.format(tablename=tips_tablename), d
            )

        conn.commit()
    finally:
        if _own:
            conn.close()


def tips_sqlite2pandas(
    db:                 Union[sqlite3.Connection, str, pathlib.Path],
    exchange_tablename: str = "tip_exchange",
    tips_tablename:     str = "tip_details",
    start:              Optional[date] = None,
    end:                Optional[date] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read tip tables back from SQLite into pandas DataFrames.

    Parameters
    ----------
    start / end : optional date range filter on tip_date (inclusive).
    """
    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        conditions, params = [], []
        if start:
            conditions.append("tip_date >= ?")
            params.append(start.isoformat())
        if end:
            conditions.append("tip_date <= ?")
            params.append(end.isoformat())
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        exchange_df = pd.read_sql(
            f"SELECT * FROM {exchange_tablename} {where}", conn, params=params
        )
        tips_df = pd.read_sql(
            f"SELECT * FROM {tips_tablename} {where}", conn, params=params
        )
    finally:
        if _own:
            conn.close()

    # Restore date type
    exchange_df["tip_date"] = pd.to_datetime(exchange_df["tip_date"]).dt.date
    tips_df["tip_date"]     = pd.to_datetime(tips_df["tip_date"]).dt.date

    # Restore nullable ints for colour columns
    int_cols = ["week_colour", "month_colour", "regime_colour"]
    for col in int_cols:
        if col in exchange_df.columns:
            exchange_df[col] = exchange_df[col].astype("Int64")

    tip_int_cols = ["win_probability", "holding_period_low", "holding_period_high",
                    "pattern_quality_colour", "setup_colour",
                    "risk_reward_colour", "context_colour"]
    for col in tip_int_cols:
        if col in tips_df.columns:
            tips_df[col] = tips_df[col].astype("Int64")

    return exchange_df, tips_df
