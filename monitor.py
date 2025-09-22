#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
monitor.py — USAspending Award Watcher

- Scrapes the Transactions table from a set of award pages
- Stores one CSV per award name (state/<NAME>.csv)
- Diffs current run vs. last stored version (by Modification Number)
- If diffs exist, sends a Gmail SMTP digest to configured recipients
- Designed for GitHub Actions (headless Playwright)

Key hardening:
- Robust detection of the Transactions table (not tied to a single container class)
- Waits for real rows, not just a visible container
- Bounded, safe "Read more" expansion (no infinite loops)
- Retries + diagnostics (HTML + screenshot on last failure)
- Atomic writes; never overwrite snapshots with empty scrapes
"""

import os
import re
import sys
import json
import csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --------------------
# Configuration
# --------------------

SITES: Dict[str, str] = {
    "EBANGA":   "https://www.usaspending.gov/award/CONT_AWD_75A50123C00037_7505_-NONE-_-NONE-",
    "TEMBEXA":  "https://www.usaspending.gov/award/CONT_AWD_75A50122C00047_7505_-NONE-_-NONE-",
    "BAT":      "https://www.usaspending.gov/award/CONT_AWD_75A50119C00075_7505_-NONE-_-NONE-",
    "VIGIV":    "https://www.usaspending.gov/award/CONT_AWD_75A50119C00037_7505_-NONE-_-NONE-",
    "ACAM2000": "https://www.usaspending.gov/award/CONT_AWD_75A50119C00071_7505_-NONE-_-NONE-",
    "CYFENDUS": "https://www.usaspending.gov/award/CONT_AWD_HHSO100201600030C_7505_-NONE-_-NONE-",
}

# Primary container used on USAspending (we'll also fall back to header-based table detection)
TABLE_CONTAINER_XPATH = '//div[@class="results-table-content"]'

STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Email / SMTP settings (set via GitHub Secrets)
GMAIL_USERNAME       = os.environ.get("GMAIL_USERNAME", "").strip()
GMAIL_APP_PASSWORD   = os.environ.get("GMAIL_APP_PASSWORD", "").strip()  # App Password required
EMAIL_RECIPIENTS     = [x.strip() for x in os.environ.get("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
EMAIL_SENDER_NAME    = os.environ.get("EMAIL_SENDER_NAME", "USAspending Watcher").strip()
EMAIL_SUBJECT_PREFIX = os.environ.get("EMAIL_SUBJECT_PREFIX", "[Award Watch]").strip()

# Optional: set to "1" to skip sending email (useful for first run/tests)
DRY_RUN = os.environ.get("DRY_RUN", "0").strip() == "1"

# Playwright timeouts (ms)
NAV_TIMEOUT_MS   = 15_000
TABLE_TIMEOUT_MS = 15_000
ROW_TIMEOUT_MS   = 35_000  # wait for real rows

# Scrape retries
MAX_SCRAPE_RETRIES = 3


# --------------------
# Helpers
# --------------------

def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def normalize_amount(s: str) -> str:
    """Normalize currency strings for comparison (keep sign and digits; handle parentheses negatives)."""
    if s is None:
        return ""
    s = s.strip()
    neg = s.startswith("-") or ("(" in s and ")" in s)
    digits = re.sub(r"[^\d.]", "", s)
    if digits == "":
        return "0"
    return f"-{digits}" if neg else digits

def canonicalize_row(row: Dict[str, str], amount_cols=("Amount",)) -> Dict[str, str]:
    out = {}
    for k, v in row.items():
        vs = normalize_space(str(v))
        if k in amount_cols:
            vs = normalize_amount(vs)
        out[k] = vs
    return out

def read_csv_if_exists(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]

def write_csv_atomic(path: Path, rows: List[Dict[str, str]]) -> None:
    """Always write atomically to avoid partial/blank files."""
    if not rows:
        # Caller must guard empties
        return
    headers = list(rows[0].keys())
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)

def detect_changes(
    old_rows: List[Dict[str, str]],
    new_rows: List[Dict[str, str]],
    key_col: str = "Modification Number"
) -> Tuple[List[Dict[str, str]], List[Tuple[str, Dict[str, str], Dict[str, str]]]]:
    old_norm = [canonicalize_row(r) for r in old_rows]
