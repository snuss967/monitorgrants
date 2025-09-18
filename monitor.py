#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USAspending Award Watcher
- Scrapes the Transactions table from a set of award pages
- Stores one CSV per award name
- Diffs current run vs. last stored version
- If diffs exist, sends a Gmail SMTP digest to configured recipients
- Designed to run under GitHub Actions on a schedule

Author: you
"""

import os
import re
import sys
import time
import json
import csv
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

# 3rd party
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# --------------------
# Configuration
# --------------------

# Award names -> URLs (you can add more here)
SITES: Dict[str, str] = {
    "EBANGA":   "https://www.usaspending.gov/award/CONT_AWD_75A50123C00037_7505_-NONE-_-NONE-",
    "TEMBEXA":  "https://www.usaspending.gov/award/CONT_AWD_75A50122C00047_7505_-NONE-_-NONE-",
    "BAT":      "https://www.usaspending.gov/award/CONT_AWD_75A50119C00075_7505_-NONE-_-NONE-",
    "VIGIV":    "https://www.usaspending.gov/award/CONT_AWD_75A50119C00037_7505_-NONE-_-NONE-",
    "ACAM2000": "https://www.usaspending.gov/award/CONT_AWD_75A50119C00071_7505_-NONE-_-NONE-",
    "CYFENDUS": "https://www.usaspending.gov/award/CONT_AWD_HHSO100201600030C_7505_-NONE-_-NONE-",
}

# The XPath you specified
TABLE_XPATH = '//div[@class="results-table-content"]'

# Where to store state (CSV snapshots)
STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Email / SMTP settings come from environment variables (set in GitHub Secrets)
GMAIL_USERNAME      = os.environ.get("GMAIL_USERNAME", "").strip()
GMAIL_APP_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "").strip()  # App Password required
EMAIL_RECIPIENTS    = [x.strip() for x in os.environ.get("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
EMAIL_SENDER_NAME   = os.environ.get("EMAIL_SENDER_NAME", "USAspending Watcher").strip()
EMAIL_SUBJECT_PREFIX = os.environ.get("EMAIL_SUBJECT_PREFIX", "[Award Watch]").strip()

# Optional: set to "1" to skip sending email (useful for first run/tests)
DRY_RUN = os.environ.get("DRY_RUN", "0").strip() == "1"

# Playwright timeouts (ms)
NAV_TIMEOUT_MS = 60_000
TABLE_TIMEOUT_MS = 60_000


# --------------------
# Helpers
# --------------------

def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()


def normalize_amount(s: str) -> str:
    """Normalize currency strings for comparison (keep sign and digits)."""
    if s is None:
        return ""
    s = s.strip()
    # Keep a leading '-' if present; remove everything except digits and dot
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


def read_csv_if_exists(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
    return rows


def write_csv(path: Path, rows: List[Dict[str, str]]):
    if not rows:
        # If no rows, write an empty file with known headers
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("")  # keep empty; or write headers if desired
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def detect_changes(
    old_rows: List[Dict[str, str]],
    new_rows: List[Dict[str, str]],
    key_col: str = "Modification Number"
) -> Tuple[List[Dict[str, str]], List[Tuple[str, Dict[str, str], Dict[str, str]]]]:
    """
    Compare old vs new.
    Returns:
      - new_entries: list of newly added rows (not present before)
      - updated_entries: list of (key, old_row, new_row) where row exists but changed
    Ignores deletions (per your instruction to include only new changes).
    """

    # Canonicalize both sides for comparison
    old_norm = [canonicalize_row(r) for r in old_rows]
    new_norm = [canonicalize_row(r) for r in new_rows]

    # Index by key if available; if not, fallback to full row signature
    def row_key(r: Dict[str, str]) -> str:
        if key_col in r and r[key_col]:
            return r[key_col]
        # fallback signature
        return json.dumps(r, sort_keys=True, ensure_ascii=False)

    old_map = {row_key(r): r for r in old_norm}
    new_map = {row_key(r): r for r in new_norm}

    # New entries
    new_entries_keys = [k for k in new_map.keys() if k not in old_map]
    new_entries = [new_rows[new_norm.index(new_map[k])] for k in new_entries_keys]  # original (un-normalized)

    # Updated entries (present in both but content differs)
    updated: List[Tuple[str, Dict[str, str], Dict[str, str]]] = []
    for k in new_map.keys():
        if k in old_map:
            if new_map[k] != old_map[k]:
                # Recover original (un-normalized) rows for reporting
                old_orig = old_rows[old_norm.index(old_map[k])]
                new_orig = new_rows[new_norm.index(new_map[k])]
                updated.append((k, old_orig, new_orig))

    return new_entries, updated


def format_change_lines(
    name: str,
    headers: List[str],
    new_entries: List[Dict[str, str]],
    updated_entries: List[Tuple[str, Dict[str, str], Dict[str, str]]],
    key_col: str = "Modification Number"
) -> List[str]:
    """
    Create human-friendly bullet lines describing only the new/updated items.
    """
    lines: List[str] = []

    # New rows
    for row in new_entries:
        parts = []
        if key_col in row and row[key_col]:
            parts.append(f"New entry (Mod #{row[key_col]})")
        else:
            parts.append("New entry")
        # Display common fields if present
        for col in ["Action Date", "Amount", "Action Type", "Transaction Description"]:
            if col in row and row[col]:
                parts.append(f"{col}: {row[col]}")
        lines.append(" - " + " | ".join(parts))

    # Updated rows: show only fields that changed
    for key, old_r, new_r in updated_entries:
        changed_cols = []
        # Compare after normalization to avoid noise
        old_norm = canonicalize_row(old_r)
        new_norm = canonicalize_row(new_r)
        for col in headers:
            if old_norm.get(col, "") != new_norm.get(col, ""):
                changed_cols.append(col)

        header = f"Updated (Mod #{new_r.get(key_col, key)}):"
        details = []
        for col in changed_cols:
            old_val = old_r.get(col, "")
            new_val = new_r.get(col, "")
            details.append(f"{col}: {old_val} → {new_val}")
        if details:
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
    """
    Send a single HTML+text email with grouped sections:
    <strong>NAME</strong> followed by bullet points of new/updated items.
    """

    if not recipients:
        print("[email] No recipients configured; skipping send.")
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject_prefix} USAspending Award Changes — {date_str}"

    # Build HTML body
    html_parts: List[str] = []
    text_parts: List[str] = []
    html_parts.append(f"<p>Detected changes at {now_utc_iso()}:</p>")
    for name, lines in per_name_lines.items():
        if not lines:
            continue
        html_parts.append(f"<p><strong>{name}</strong></p>")
        html_parts.append("<ul>")
        for ln in lines:
            html_parts.append(f"<li>{ln[3:] if ln.startswith(' - ') else ln}</li>")
        html_parts.append("</ul>")

        text_parts.append(f"{name}")
        for ln in lines:
            text_parts.append(ln)
        text_parts.append("")

    html_body = "\n".join(html_parts)
    text_body = "\n".join(text_parts) if text_parts else "Changes detected."

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{gmail_username}>"
    msg["To"] = ", ".join(recipients)

    part1 = MIMEText(text_body, "plain", "utf-8")
    part2 = MIMEText(html_body, "html", "utf-8")
    msg.attach(part1)
    msg.attach(part2)

    context = ssl.create_default_context()
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(gmail_username, gmail_app_password)
        server.sendmail(gmail_username, recipients, msg.as_string())
        print(f"[email] Sent digest to {len(recipients)} recipient(s).")


# --------------------
# Scraping
# --------------------

def scrape_table_rows(page, url: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Navigate to the page, wait for table at the given XPath, expand 'Read More' buttons,
    then return (headers, rows_as_dict_list).
    """
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)

    # Some pages display cookie banners; attempt to dismiss common ones (best-effort, ignore failures)
    for sel in [
        'button:has-text("Accept")',
        'button:has-text("Accept All")',
        'button:has-text("I Agree")',
        'button:has-text("Got it")',
    ]:
        try:
            if page.is_visible(sel, timeout=1500):
                page.click(sel)
        except Exception:
            pass

    # Wait for the results table container
    container = page.wait_for_selector(TABLE_XPATH, state="visible", timeout=TABLE_TIMEOUT_MS)

    # Expand all 'Read More' buttons inside the container (to get full text)
    try:
        # Click until none left (some tables paginate or lazily render)
        while True:
            buttons = page.query_selector_all(f"{TABLE_XPATH}//button[contains(@class,'read-more-button')]")
            clicked = 0
            for b in buttons:
                try:
                    b.click()
                    clicked += 1
                except Exception:
                    pass
            if clicked == 0:
                break
            time.sleep(0.3)
    except Exception:
        pass

    # Grab the table element (first table under the container)
    table = page.query_selector(f"{TABLE_XPATH}//table")
    if table is None:
        # Some pages render with data table virtualization; try inner HTML fallback
        html = container.inner_html()
        raise RuntimeError("Could not find <table> under results-table-content; page layout may have changed.")

    # Extract headers robustly: get header label text only (exclude sort icons)
    headers: List[str] = table.evaluate(
        """(el) => {
            // Collect header labels; target the label container if present
            const hs = [];
            const ths = el.querySelectorAll('thead th');
            for (const th of ths) {
                let label = '';
                const lbl = th.querySelector('.table-header__label');
                if (lbl) {
                    // Often like "Modification Number" with nested sort buttons
                    // Grab only the first text node content
                    for (const node of lbl.childNodes) {
                        if (node.nodeType === Node.TEXT_NODE) {
                            label = (node.textContent || '').trim();
                            if (label) break;
                        }
                    }
                    if (!label) label = (lbl.textContent || '').trim();
                } else {
                    label = (th.textContent || '').trim();
                }
                label = label.replace(/\\s+/g, ' ').trim();
                if (label) hs.push(label);
            }
            return hs;
        }"""
    )

    # Extract body rows (visible text)
    rows: List[List[str]] = table.evaluate(
        """(el) => {
            const out = [];
            const trs = el.querySelectorAll('tbody tr');
            for (const tr of trs) {
                const tds = tr.querySelectorAll('td');
                const row = [];
                for (const td of tds) {
                    // innerText returns visible text (after 'Read More' expanded)
                    let t = td.innerText || '';
                    t = t.replace(/\\s+/g, ' ').trim();
                    row.push(t);
                }
                if (row.length > 0) out.push(row);
            }
            return out;
        }"""
    )

    # Zip into dicts
    records: List[Dict[str, str]] = []
    for r in rows:
        if len(r) != len(headers):
            # If column count mismatch, pad/truncate conservatively
            if len(r) < len(headers):
                r = r + [""] * (len(headers) - len(r))
            else:
                r = r[:len(headers)]
        records.append({h: v for h, v in zip(headers, r)})

    return headers, records


def scrape_all_sites() -> Dict[str, Dict[str, Any]]:
    """
    Scrape every site and return {name: {"headers": [...], "rows": [...]}} for successful scrapes.
    """
    results: Dict[str, Dict[str, Any]] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()

        for name, url in SITES.items():
            print(f"[scrape] {name}: {url}")
            try:
                headers, rows = scrape_table_rows(page, url)
                print(f"[scrape] {name}: {len(rows)} row(s)")
                results[name] = {"headers": headers, "rows": rows}
            except PlaywrightTimeoutError:
                print(f"[scrape][ERROR] Timeout while loading table for {name}")
            except Exception as e:
                print(f"[scrape][ERROR] {name}: {e}")

        context.close()
        browser.close()
    return results


# --------------------
# Main flow
# --------------------

def main() -> int:
    print(f"[start] USAspending watcher at {now_utc_iso()}")

    scraped = scrape_all_sites()
    if not scraped:
        print("[end] No sites scraped successfully.")
        return 0

    any_changes = False
    digest_by_name: Dict[str, List[str]] = {}

    for name, payload in scraped.items():
        headers = payload["headers"]
        new_rows = payload["rows"]

        # Path for this award's CSV snapshot
        csv_path = STATE_DIR / f"{name}.csv"

        # Load old rows (if any)
        old_rows = read_csv_if_exists(csv_path)

        if not old_rows:
            # First time: create the file, do not email (per your spec)
            print(f"[state] Initializing snapshot for {name} -> {csv_path}")
            write_csv(csv_path, new_rows)
            continue

        # Detect changes
        new_entries, updated_entries = detect_changes(old_rows, new_rows, key_col="Modification Number")

        if new_entries or updated_entries:
            any_changes = True
            lines = format_change_lines(name, headers, new_entries, updated_entries, key_col="Modification Number")
            digest_by_name[name] = lines

            # Update snapshot immediately so repo gets the latest
            write_csv(csv_path, new_rows)
            print(f"[diff] {name}: {len(new_entries)} new, {len(updated_entries)} updated")
        else:
            print(f"[diff] {name}: no changes")

    # Send email only if there were changes anywhere
    if any_changes and digest_by_name:
        if DRY_RUN:
            print("[email] DRY_RUN=1, not sending email. Digest preview:")
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
        print("[email] No diffs detected; no email sent.")

    print("[end] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
