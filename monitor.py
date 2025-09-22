#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
monitor.py — USAspending Award Watcher (API-first, no browser)

- Fetches Transactions for each award via USAspending API
- Stores one CSV per award name under ./state
- Diffs current run vs. prior snapshot (only new/changed rows)
- Sends a Gmail SMTP digest when diffs are found
- Safe for GitHub Actions (idempotent, atomic writes, timeouts)

Requires: requests (pip install requests)

Docs:
- Transactions table: POST /api/v2/transactions/ (powers award Transaction History) 
- Count: GET /api/v2/awards/count/transaction/<AWARD_ID>/
"""

import os
import re
import sys
import csv
import json
import time
import ssl
import smtplib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --------------------
# Configuration
# --------------------

# Award label -> generated award id (from the USAspending award URL)
SITES: Dict[str, str] = {
    "EBANGA":   "CONT_AWD_75A50123C00037_7505_-NONE-_-NONE-",
    "TEMBEXA":  "CONT_AWD_75A50122C00047_7505_-NONE-_-NONE-",
    "BAT":      "CONT_AWD_75A50119C00075_7505_-NONE-_-NONE-",
    "VIGIV":    "CONT_AWD_75A50119C00037_7505_-NONE-_-NONE-",
    "ACAM2000": "CONT_AWD_75A50119C00071_7505_-NONE-_-NONE-",
    "CYFENDUS": "CONT_AWD_HHSO100201600030C_7505_-NONE-_-NONE-",
}

API_BASE = "https://api.usaspending.gov"
TXN_ENDPOINT = "/api/v2/transactions/"
COUNT_ENDPOINT_TEMPLATE = "/api/v2/awards/count/transaction/{award_id}/"

STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Email / SMTP (set in env / GitHub Secrets)
GMAIL_USERNAME      = os.environ.get("GMAIL_USERNAME", "").strip()
GMAIL_APP_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
EMAIL_RECIPIENTS    = [x.strip() for x in os.environ.get("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
EMAIL_SENDER_NAME   = os.environ.get("EMAIL_SENDER_NAME", "USAspending Watcher").strip()
EMAIL_SUBJECT_PREFIX = os.environ.get("EMAIL_SUBJECT_PREFIX", "[Award Watch]").strip()

# Optional dry-run (no email)
DRY_RUN = os.environ.get("DRY_RUN", "0").strip() == "1"

# HTTP behavior
HTTP_TIMEOUT = 20           # per request seconds
MAX_RETRIES = 4             # API retry attempts
RETRY_BACKOFF = 1.5         # exponential backoff base

# Diff behavior
# Use composite key to avoid collisions where Mod # alone repeats.
DIFF_KEY_COLS = ("Modification Number", "Action Date", "Amount")

# CSV columns (stable order)
CSV_HEADERS = ["Modification Number", "Action Date", "Amount", "Action Type", "Transaction Description"]

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# --------------------
# Helpers
# --------------------

def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def normalize_amount(s: str) -> str:
    """Normalize currency strings for comparison (keep sign and digits)."""
    if s is None:
        return ""
    s = s.strip()
    neg = s.startswith("-")
    digits = re.sub(r"[^\d.]", "", s)
    if digits == "":
        return "0"
    return f"-{digits}" if neg else digits

def canonicalize_row(row: Dict[str, str], amount_cols=("Amount",)) -> Dict[str, str]:
    """Return a normalized copy of a table row for stable comparisons."""
    out = {}
    for k, v in row.items():
        vs = normalize_space(str(v))
        if k in amount_cols:
            vs = normalize_amount(vs)
        out[k] = vs
    return out

def row_key_from_cols(row: Dict[str, str], key_cols=DIFF_KEY_COLS) -> str:
    vals = [normalize_space(str(row.get(k, ""))) for k in key_cols]
    return "||".join(vals)

def read_csv_if_exists(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]

def write_csv_atomic(path: Path, rows: List[Dict[str, str]]):
    """Write CSV atomically to avoid partial files in CI."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for r in rows:
            writer.writerow({h: r.get(h, "") for h in CSV_HEADERS})
    tmp.replace(path)  # atomic on POSIX

def detect_changes(
    old_rows: List[Dict[str, str]],
    new_rows: List[Dict[str, str]],
    key_cols: Tuple[str, ...] = DIFF_KEY_COLS
) -> Tuple[List[Dict[str, str]], List[Tuple[str, Dict[str, str], Dict[str, str]]]]:
    """
    Compare old vs new using composite key.
    Returns:
      - new_entries: newly added rows (by key)
      - updated_entries: list of (key, old_row, new_row) where key exists but content differs
    """
    old_norm = [canonicalize_row(r) for r in old_rows]
    new_norm = [canonicalize_row(r) for r in new_rows]

    def rk(r: Dict[str, str]) -> str:
        key = row_key_from_cols(r, key_cols)
        return key if key.strip("|") else json.dumps(r, sort_keys=True, ensure_ascii=False)

    old_map = {rk(r): r for r in old_norm}
    new_map = {rk(r): r for r in new_norm}

    # New entries (keys not seen before)
    new_keys = [k for k in new_map.keys() if k not in old_map]
    new_entries: List[Dict[str, str]] = []
    for k in new_keys:
        # find original un-normalized row
        idx = new_norm.index(new_map[k])
        new_entries.append(new_rows[idx])

    # Updated entries
    updated: List[Tuple[str, Dict[str, str], Dict[str, str]]] = []
    for k in new_map.keys():
        if k in old_map and new_map[k] != old_map[k]:
            old_orig = old_rows[old_norm.index(old_map[k])]
            new_orig = new_rows[new_norm.index(new_map[k])]
            updated.append((k, old_orig, new_orig))

    return new_entries, updated

def format_change_lines(
    headers: List[str],
    new_entries: List[Dict[str, str]],
    updated_entries: List[Tuple[str, Dict[str, str], Dict[str, str]]],
) -> List[str]:
    """
    Create human-friendly bullet lines describing only the new/updated items.
    """
    lines: List[str] = []

    # New rows
    for row in new_entries:
        parts = []
        if row.get("Modification Number"):
            parts.append(f"New entry (Mod #{row['Modification Number']})")
        else:
            parts.append("New entry")
        for col in ["Action Date", "Amount", "Action Type", "Transaction Description"]:
            if row.get(col):
                parts.append(f"{col}: {row[col]}")
        lines.append(" - " + " | ".join(parts))

    # Updated rows: show only changed fields
    for key, old_r, new_r in updated_entries:
        changed_cols = []
        old_norm = canonicalize_row(old_r)
        new_norm = canonicalize_row(new_r)
        for col in headers:
            if old_norm.get(col, "") != new_norm.get(col, ""):
                changed_cols.append(col)
        if changed_cols:
            details = []
            for col in changed_cols:
                details.append(f"{col}: {old_r.get(col, '')} → {new_r.get(col, '')}")
            header = f"Updated (Key {key}):"
            lines.append(" - " + header + " " + "; ".join(details))

    return lines

def send_email_digest(
    subject_prefix: str,
    sender_name: str,
    gmail_username: str,
    gmail_app_password: str,
    recipients: List[str],
    per_name_lines: Dict[str, List[str]]
):
    if not recipients:
        logging.info("[email] No recipients configured; skipping send.")
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject_prefix} USAspending Award Changes — {date_str}"

    html_parts: List[str] = []
    text_parts: List[str] = []
    html_parts.append(f"<p>Detected changes at {now_utc_iso()}:</p>")
    for name, lines in per_name_lines.items():
        if not lines:
            continue
        html_parts.append(f"<p><strong>{name}</strong></p><ul>")
        for ln in lines:
            html_parts.append(f"<li>{ln[3:] if ln.startswith(' - ') else ln}</li>")
        html_parts.append("</ul>")

        text_parts.append(f"{name}")
        text_parts.extend(lines)
        text_parts.append("")

    html_body = "\n".join(html_parts)
    text_body = "\n".join(text_parts) if text_parts else "Changes detected."

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{gmail_username}>"
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=60) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(gmail_username, gmail_app_password)
        server.sendmail(gmail_username, recipients, msg.as_string())
        logging.info(f"[email] Sent digest to {len(recipients)} recipient(s).")


# --------------------
# USAspending API
# --------------------

def http_post_json(url: str, payload: dict, session: Optional[requests.Session] = None) -> dict:
    sess = session or requests.Session()
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.post(url, json=payload, timeout=HTTP_TIMEOUT)
            if r.status_code >= 500:
                raise requests.HTTPError(f"Server {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            sleep = RETRY_BACKOFF ** attempt
            logging.warning(f"[api] POST {url} failed (attempt {attempt}/{MAX_RETRIES}): {e}; backoff {sleep:.1f}s")
            time.sleep(sleep)
    raise RuntimeError(f"POST {url} failed after {MAX_RETRIES} attempts: {last}")

def http_get_json(url: str, session: Optional[requests.Session] = None) -> dict:
    sess = session or requests.Session()
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code >= 500:
                raise requests.HTTPError(f"Server {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            sleep = RETRY_BACKOFF ** attempt
            logging.warning(f"[api] GET {url} failed (attempt {attempt}/{MAX_RETRIES}): {e}; backoff {sleep:.1f}s")
            time.sleep(sleep)
    raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} attempts: {last}")

def fetch_transactions(award_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all transactions for an award via POST /api/v2/transactions/
    Returns API rows as dictionaries.
    """
    url = f"{API_BASE}{TXN_ENDPOINT}"
    session = requests.Session()

    # Optional: discover count to pre-size pagination
    try:
        count_url = f"{API_BASE}{COUNT_ENDPOINT_TEMPLATE.format(award_id=award_id)}"
        count_json = http_get_json(count_url, session=session)
        total = int(count_json.get("results", 0) or count_json.get("count", 0) or 0)
    except Exception:
        total = 0  # not critical; we can paginate until empty

    limit = 500  # safe chunk (max is 5000 per docs)
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        payload = {
            "award_id": award_id,
            "limit": limit,
            "page": page,
            "sort": "action_date",
            "order": "desc",
        }
        data = http_post_json(url, payload, session=session)
        rows = data.get("results") or data.get("transactions") or data.get("data") or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < limit:
            break
        if total and len(out) >= total:
            break
        page += 1

    return out

def shape_transactions(api_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Map API fields to our CSV schema. Fields cited in API docs:
    modification_number, action_date, federal_action_obligation,
    action_type_description, description.
    """
    shaped: List[Dict[str, str]] = []
    for r in api_rows:
        mod = r.get("modification_number") or r.get("award_modification_amendment_number") or ""
        action_date = r.get("action_date") or ""
        amt = r.get("federal_action_obligation") or r.get("transaction_obligated_amount") or 0
        try:
            # format as something human-friendly for emails/CSVs
            amount_str = f"{float(amt):,.2f}"
            if str(amt).startswith("-") and not amount_str.startswith("-"):
                amount_str = "-" + amount_str
        except Exception:
            amount_str = str(amt)
        action_type = r.get("action_type_description") or r.get("action_type") or ""
        desc = r.get("description") or r.get("transaction_description") or ""

        shaped.append({
            "Modification Number": str(mod),
            "Action Date": str(action_date),
            "Amount": amount_str,
            "Action Type": str(action_type),
            "Transaction Description": str(desc),
        })

    # Sort deterministically (newest first)
    shaped.sort(key=lambda x: (x.get("Action Date", ""), x.get("Modification Number", "")), reverse=True)
    return shaped


# --------------------
# Main
# --------------------

def main() -> int:
    logging.info(f"[start] USAspending watcher at {now_utc_iso()}")

    any_changes = False
    digest_by_name: Dict[str, List[str]] = {}

    for name, award_id in SITES.items():
        logging.info(f"[fetch] {name}: {award_id}")
        try:
            api_rows = fetch_transactions(award_id)
            rows = shape_transactions(api_rows)
            logging.info(f"[fetch] {name}: {len(rows)} transaction(s)")

            csv_path = STATE_DIR / f"{name}.csv"
            old_rows = read_csv_if_exists(csv_path)

            if not old_rows:
                if rows:
                    logging.info(f"[state] Initializing snapshot for {name} -> {csv_path}")
                    write_csv_atomic(csv_path, rows)
                else:
                    logging.warning(f"[state][WARN] {name}: API returned 0 rows; snapshot not created.")
                continue

            if not rows:
                logging.warning(f"[warn] {name}: API returned 0 rows; keeping previous snapshot and skipping diff.")
                continue

            new_entries, updated_entries = detect_changes(old_rows, rows, key_cols=DIFF_KEY_COLS)

            if new_entries or updated_entries:
                any_changes = True
                lines = format_change_lines(CSV_HEADERS, new_entries, updated_entries)
                digest_by_name[name] = lines
                write_csv_atomic(csv_path, rows)
                logging.info(f"[diff] {name}: {len(new_entries)} new, {len(updated_entries)} updated")
            else:
                logging.info(f"[diff] {name}: no changes")

        except Exception as e:
            logging.exception(f"[ERROR] {name}: {e}")

    if any_changes and digest_by_name:
        if DRY_RUN:
            logging.info("[email] DRY_RUN=1, not sending email. Digest preview:")
            for k, v in digest_by_name.items():
                print(f"\n** {k} **")
                for ln in v:
                    print(ln)
        else:
            send_email_digest(
                subject_prefix=EMAIL_SUBJECT_PREFIX,
                sender_name=EMAIL_SENDER_NAME,
                gmail_username=GMAIL_USERNAME,
                gmail_app_password=GMAIL_APP_PASSWORD,
                recipients=EMAIL_RECIPIENTS,
                per_name_lines=digest_by_name,
            )
    else:
        logging.info("[email] No diffs detected; no email sent.")

    logging.info("[end] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
