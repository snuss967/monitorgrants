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
import json
import csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

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

# The table container on USAspending
TABLE_XPATH = '//div[@class="results-table-content"]'

# Where to store state (CSV snapshots)
STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Email / SMTP settings come from environment variables (set in GitHub Secrets)
GMAIL_USERNAME       = os.environ.get("GMAIL_USERNAME", "").strip()
GMAIL_APP_PASSWORD   = os.environ.get("GMAIL_APP_PASSWORD", "").strip()  # App Password required
EMAIL_RECIPIENTS     = [x.strip() for x in os.environ.get("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
EMAIL_SENDER_NAME    = os.environ.get("EMAIL_SENDER_NAME", "USAspending Watcher").strip()
EMAIL_SUBJECT_PREFIX = os.environ.get("EMAIL_SUBJECT_PREFIX", "[Award Watch]").strip()

# Optional: set to "1" to skip sending email (useful for first run/tests)
DRY_RUN = os.environ.get("DRY_RUN", "0").strip() == "1"

# Playwright timeouts (ms)
NAV_TIMEOUT_MS   = 10_000
TABLE_TIMEOUT_MS = 10_000
ROW_TIMEOUT_MS   = 30_000  # wait for real rows

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
    """Normalize currency strings for comparison (keep sign and digits, handle parentheses negatives)."""
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
        # Caller should guard empties; do nothing to avoid clobbering good state.
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
    new_norm = [canonicalize_row(r) for r in new_rows]

    def row_key(r: Dict[str, str]) -> str:
        v = r.get(key_col, "")
        return v if v else json.dumps(r, sort_keys=True, ensure_ascii=False)

    old_map = {row_key(r): r for r in old_norm}
    new_map = {row_key(r): r for r in new_norm}

    # New entries
    new_entries = []
    for k, norm in new_map.items():
        if k not in old_map:
            # find original un-normalized by index in new_norm
            idx = new_norm.index(norm)
            new_entries.append(new_rows[idx])

    # Updated entries
    updated: List[Tuple[str, Dict[str, str], Dict[str, str]]] = []
    for k, new_norm_row in new_map.items():
        if k in old_map and new_norm_row != old_map[k]:
            old_idx = old_norm.index(old_map[k])
            new_idx = new_norm.index(new_norm_row)
            updated.append((k, old_rows[old_idx], new_rows[new_idx]))

    return new_entries, updated

def format_change_lines(
    name: str,
    headers: List[str],
    new_entries: List[Dict[str, str]],
    updated_entries: List[Tuple[str, Dict[str, str], Dict[str, str]]],
    key_col: str = "Modification Number"
) -> List[str]:
    lines: List[str] = []

    for row in new_entries:
        parts = []
        mod = row.get(key_col, "")
        parts.append(f"New entry (Mod #{mod})" if mod else "New entry")
        for col in ["Action Date", "Amount", "Action Type", "Transaction Description"]:
            if row.get(col):
                parts.append(f"{col}: {row[col]}")
        lines.append(" - " + " | ".join(parts))

    for key, old_r, new_r in updated_entries:
        changed_cols = []
        old_norm = canonicalize_row(old_r)
        new_norm = canonicalize_row(new_r)
        for col in headers:
            if old_norm.get(col, "") != new_norm.get(col, ""):
                changed_cols.append(col)

        if changed_cols:
            header = f"Updated (Mod #{new_r.get(key_col, key)}):"
            details = []
            for col in changed_cols:
                details.append(f"{col}: {old_r.get(col, '')} → {new_r.get(col, '')}")
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
        print("[email] No recipients configured; skipping send.")
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject_prefix} USAspending Award Changes — {date_str}"

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

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=60) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(gmail_username, gmail_app_password)
        server.sendmail(gmail_username, recipients, msg.as_string())
        print(f"[email] Sent digest to {len(recipients)} recipient(s).")

# --------------------
# Scraping
# --------------------

def wait_for_table_rows(page, container_xpath: str, min_rows: int = 1, timeout_ms: int = ROW_TIMEOUT_MS):
    """Wait until the table under container_xpath has at least min_rows rows in <tbody>."""
    page.wait_for_selector(f"{container_xpath}//table", state="attached", timeout=timeout_ms)
    # Ensure scrolled into view (helps some virtualized tables)
    try:
        page.locator(f"xpath={container_xpath}").scroll_into_view_if_needed(timeout=1000)
    except Exception:
        pass
    page.wait_for_function(
        """
        (containerXPath, minRows) => {
            const el = document.evaluate(containerXPath + "//table", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            if (!el) return false;
            const rows = el.querySelectorAll("tbody tr");
            return rows && rows.length >= minRows;
        }
        """,
        arg=[container_xpath, min_rows],
        timeout=timeout_ms,
    )

def expand_read_more_safely(page, container_xpath: str, max_passes: int = 8, per_pass_cap: int = 200):
    """
    Expand collapsed 'Read more' toggles without getting stuck.
    Click only 'read more' (not 'read less'); cap passes and clicks.
    """
    prev_count = None
    for _ in range(max_passes):
        sel = (
            f"{container_xpath}//button"
            "[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'read more')"
            " or @aria-expanded='false']"
        )
        loc = page.locator(f"xpath={sel}")
        try:
            count = loc.count()
        except Exception:
            break
        if count == 0 or count == prev_count:
            break
        prev_count = count

        for i in range(min(count, per_pass_cap)):
            try:
                loc.nth(i).click(timeout=1000)
            except Exception:
                pass

        page.wait_for_timeout(250)  # let DOM settle

def scrape_table_rows(page, url: str) -> Tuple[List[str], List[Dict[str, str]]]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_SCRAPE_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            # Let XHRs settle a bit
            try:
                page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

            # Try to activate the "Transactions" tab if present
            try:
                tab = page.get_by_role("tab", name=re.compile(r"^Transactions", re.I))
                if tab.is_visible():
                    tab.click(timeout=2000)
            except Exception:
                pass

            # Wait for container + real rows
            page.wait_for_selector(TABLE_XPATH, state="visible", timeout=TABLE_TIMEOUT_MS)
            wait_for_table_rows(page, TABLE_XPATH, min_rows=1, timeout_ms=ROW_TIMEOUT_MS)

            # Expand details safely
            expand_read_more_safely(page, TABLE_XPATH)

            # Extract
            table = page.query_selector(f"{TABLE_XPATH}//table")
            if table is None:
                raise RuntimeError("Results table not found under container.")

            headers: List[str] = table.evaluate(
                """(el) => {
                    const hs = [];
                    for (const th of el.querySelectorAll('thead th')) {
                        let t = '';
                        const lbl = th.querySelector('.table-header__label');
                        if (lbl) {
                            for (const node of lbl.childNodes) {
                                if (node.nodeType === Node.TEXT_NODE) {
                                    t = (node.textContent || '').trim();
                                    if (t) break;
                                }
                            }
                            if (!t) t = (lbl.textContent || '').trim();
                        } else {
                            t = (th.textContent || '').trim();
                        }
                        t = t.replace(/\\s+/g, ' ').trim();
                        if (t) hs.push(t);
                    }
                    return hs;
                }"""
            )

            rows: List[List[str]] = table.evaluate(
                """(el) => {
                    const out = [];
                    for (const tr of el.querySelectorAll('tbody tr')) {
                        const row = [];
                        for (const td of tr.querySelectorAll('td')) {
                            let t = td.innerText || '';
                            t = t.replace(/\\s+/g, ' ').trim();
                            row.push(t);
                        }
                        if (row.length) out.push(row);
                    }
                    return out;
                }"""
            )

            records: List[Dict[str, str]] = []
            for r in rows:
                if len(r) < len(headers):
                    r = r + [""] * (len(headers) - len(r))
                elif len(r) > len(headers):
                    r = r[:len(headers)]
                records.append({h: v for h, v in zip(headers, r)})

            if not records:
                raise RuntimeError("Table has 0 rows after wait; treating as transient load failure.")

            return headers, records

        except Exception as e:
            last_err = e
            if attempt == MAX_SCRAPE_RETRIES:
                ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                try:
                    STATE_DIR.mkdir(exist_ok=True)
                    page.screenshot(path=str(STATE_DIR / f"{ts}_fail.png"), full_page=True)
                    (STATE_DIR / f"{ts}_fail.html").write_text(page.content(), encoding="utf-8")
                    print(f"[scrape][DIAG] Saved {STATE_DIR}/{ts}_fail.png and {STATE_DIR}/{ts}_fail.html")
                except Exception:
                    pass
                break
            # Backoff + reload for another try
            page.wait_for_timeout(500 * attempt)
            try:
                page.reload(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            except Exception:
                pass

    raise last_err if last_err else RuntimeError("Unknown scrape failure")

def scrape_all_sites() -> Dict[str, Dict[str, Any]]:
    """
    Scrape every site and return {name: {"headers": [...], "rows": [...]}} for successful scrapes.
    """
    results: Dict[str, Dict[str, Any]] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
        )

        page = context.new_page()
        page.set_default_timeout(15_000)
        page.set_default_navigation_timeout(15_000)

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

        csv_path = STATE_DIR / f"{name}.csv"
        old_rows = read_csv_if_exists(csv_path)

        # First-time snapshot: only write if we actually have rows.
        if not old_rows:
            if new_rows:
                print(f"[state] Initializing snapshot for {name} -> {csv_path}")
                write_csv_atomic(csv_path, new_rows)
            else:
                print(f"[state][WARN] {name}: initial scrape returned 0 rows; snapshot not created.")
            continue

        # Subsequent runs: skip entirely if scrape is empty (never clobber with empties)
        if not new_rows:
            print(f"[warn] {name}: scrape returned 0 rows; keeping previous snapshot and skipping diff.")
            continue

        # Detect changes
        new_entries, updated_entries = detect_changes(old_rows, new_rows, key_col="Modification Number")

        if new_entries or updated_entries:
            any_changes = True
            lines = format_change_lines(name, headers, new_entries, updated_entries, key_col="Modification Number")
            digest_by_name[name] = lines

            # Update snapshot atomically
            write_csv_atomic(csv_path, new_rows)
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
