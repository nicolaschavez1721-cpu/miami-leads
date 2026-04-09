"""
Miami-Dade County Motivated Seller Lead Scraper
Uses Playwright with stealth mode to execute JavaScript in the portal.
"""

import asyncio
import json
import csv
import io
import re
import time
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LOOKBACK_DAYS = 7
MAX_RETRIES   = 3
RETRY_DELAY   = 5

ROOT_DIR      = Path(__file__).parent.parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
DATA_DIR      = ROOT_DIR / "data"

DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("miami_scraper")

# ─────────────────────────────────────────────
# DOCUMENT TYPE MAP
# ─────────────────────────────────────────────
DOC_TYPES = {
    "LP":       ("Lis Pendens",             "pre-foreclosure"),
    "NOFC":     ("Notice of Foreclosure",   "pre-foreclosure"),
    "TAXDEED":  ("Tax Deed",                "tax-distressed"),
    "JUD":      ("Judgment",                "judgment"),
    "CCJ":      ("Certified Judgment",      "judgment"),
    "DRJUD":    ("Domestic Judgment",       "judgment"),
    "LNCORPTX": ("Corp Tax Lien",          "tax-lien"),
    "LNIRS":    ("IRS Lien",               "tax-lien"),
    "LNFED":    ("Federal Lien",           "tax-lien"),
    "LN":       ("Lien",                   "lien"),
    "LNMECH":   ("Mechanic Lien",          "lien"),
    "LNHOA":    ("HOA Lien",              "lien"),
    "MEDLN":    ("Medicaid Lien",          "lien"),
    "PRO":      ("Probate Document",       "probate"),
    "NOC":      ("Notice of Commencement", "notice"),
    "RELLP":    ("Release of Lis Pendens", "release"),
}

CAT_LABELS = {
    "pre-foreclosure": "Pre-Foreclosure",
    "tax-distressed":  "Tax Distressed",
    "judgment":        "Judgment / CCJ",
    "tax-lien":        "Tax / IRS / Fed Lien",
    "lien":            "Lien",
    "probate":         "Probate / Estate",
    "notice":          "Notice",
    "release":         "Release",
}

# ─────────────────────────────────────────────
# SCORE ENGINE
# ─────────────────────────────────────────────
def compute_score_and_flags(record: dict) -> tuple:
    flags = []
    score = 30
    doc_type = record.get("doc_type", "")
    cat      = record.get("cat", "")
    amount   = record.get("amount") or 0
    filed    = record.get("filed", "")
    owner    = record.get("owner", "") or ""

    if doc_type in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if cat == "pre-foreclosure" or doc_type in ("LP", "NOFC"):
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat == "tax-lien" or doc_type == "TAXDEED":
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bCORP\b|\bINC\b|\bLTD\b|\bLLP\b", owner, re.I):
        flags.append("LLC / corp owner")
    try:
        filed_dt = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_dt).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount and float(amount) > 100000:
        score += 15
    elif amount and float(amount) > 50000:
        score += 10
    if "New this week" in flags:
        score += 5
    if record.get("prop_address"):
        score += 5

    return min(score, 100), list(dict.fromkeys(flags))


# ─────────────────────────────────────────────
# STEALTH JS — injected to avoid bot detection
# ─────────────────────────────────────────────
STEALTH_JS = """
// Override navigator properties to appear as real browser
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
window.chrome = { runtime: {} };
Object.defineProperty(navigator, 'permissions', {
  get: () => ({
    query: () => Promise.resolve({ state: 'granted' })
  })
});
// Fix iframe contentWindow
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
  parameters.name === 'notifications' ?
    Promise.resolve({ state: Notification.permission }) :
    originalQuery(parameters)
);
"""


# ─────────────────────────────────────────────
# CLERK SCRAPER
# ─────────────────────────────────────────────
class ClerkScraper:
    PORTAL_URL = "https://onlineservices.miamidadeclerk.gov/officialrecords/StandardSearch.aspx"

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")

    async def _human_type(self, page, selector, text):
        """Type like a human with random delays."""
        await page.click(selector)
        await page.fill(selector, "")
        for ch in text:
            await page.type(selector, ch, delay=random.randint(50, 150))

    async def _wait_and_log(self, page, label):
        """Wait for page to settle and log current state."""
        await asyncio.sleep(random.uniform(1.5, 3.0))
        url = page.url
        html = await page.content()
        text = await page.evaluate("() => document.body ? document.body.innerText.slice(0, 300) : ''")
        inputs = await page.evaluate("""() => {
            const els = document.querySelectorAll('input, select, button');
            return Array.from(els).slice(0, 15).map(e => ({
                tag: e.tagName, id: e.id, name: e.name,
                type: e.type, placeholder: e.placeholder, value: e.value.slice(0,30)
            }));
        }""")
        log.info(f"[{label}] URL: {url}")
        log.info(f"[{label}] Page text: {text[:200].replace(chr(10),' ')}")
        log.info(f"[{label}] Inputs: {inputs}")
        return html, inputs

    async def _try_fill_form(self, page, doc_code: str) -> bool:
        """Try all known approaches to fill the search form."""

        # Approach 1: Find inputs by various strategies
        strategies = [
            # By placeholder text
            ("placeholder~=date", "date"),
            # By aria-label
            ("aria-label~=date", "date"),
            # By data attributes
            ("[data-field*=date]", "date"),
            # By common ID patterns
            ("#startDate,#start-date,#dateFrom,#date-from,#fromDate,#beginDate,#recordedDateFrom", "date_start"),
            ("#endDate,#end-date,#dateTo,#date-to,#toDate,#throughDate,#recordedDateTo", "date_end"),
            ("#docType,#doc-type,#documentType,#instrumentType,#recordType", "doctype"),
        ]

        # Try to find all form elements via JS
        form_info = await page.evaluate("""() => {
            const result = {inputs: [], selects: [], buttons: []};
            document.querySelectorAll('input').forEach(el => {
                result.inputs.push({
                    id: el.id, name: el.name, type: el.type,
                    placeholder: el.placeholder, className: el.className.slice(0,50),
                    'aria-label': el.getAttribute('aria-label') || '',
                    'data-bind': el.getAttribute('data-bind') || '',
                    visible: el.offsetParent !== null
                });
            });
            document.querySelectorAll('select').forEach(el => {
                const opts = Array.from(el.options).map(o => o.value + ':' + o.text).slice(0,10);
                result.selects.push({id: el.id, name: el.name, options: opts});
            });
            document.querySelectorAll('button, input[type=submit]').forEach(el => {
                result.buttons.push({id: el.id, text: el.innerText || el.value, type: el.type});
            });
            return result;
        }""")

        log.info(f"Form elements found: inputs={len(form_info['inputs'])}, selects={len(form_info['selects'])}, buttons={len(form_info['buttons'])}")
        log.info(f"Inputs detail: {form_info['inputs']}")
        log.info(f"Selects detail: {form_info['selects']}")
        log.info(f"Buttons: {form_info['buttons']}")

        if not form_info['inputs'] and not form_info['selects']:
            return False

        # Try to fill date fields
        filled_start = False
        filled_end   = False
        filled_type  = False

        for inp in form_info['inputs']:
            iid   = (inp.get('id') or '').lower()
            iname = (inp.get('name') or '').lower()
            iph   = (inp.get('placeholder') or '').lower()
            ibind = (inp.get('data-bind') or '').lower()
            iaria = (inp.get('aria-label') or '').lower()
            combined = iid + iname + iph + ibind + iaria

            sel = None
            if inp.get('id'):
                sel = f"#{inp['id']}"
            elif inp.get('name'):
                sel = f"input[name='{inp['name']}']"

            if not sel:
                continue

            is_start = any(x in combined for x in ['startdate','start_date','datefrom','date_from','begindate','fromdate','recordedstart','datebegin'])
            is_end   = any(x in combined for x in ['enddate','end_date','dateto','date_to','throughdate','todate','recordedend','dateend'])
            is_type  = any(x in combined for x in ['doctype','doc_type','documenttype','instrumenttype','recordtype'])

            try:
                if is_start and not filled_start:
                    await page.fill(sel, self.date_from)
                    filled_start = True
                    log.info(f"Filled start date: {sel} = {self.date_from}")
                elif is_end and not filled_end:
                    await page.fill(sel, self.date_to)
                    filled_end = True
                    log.info(f"Filled end date: {sel} = {self.date_to}")
                elif is_type and not filled_type:
                    await page.fill(sel, doc_code)
                    filled_type = True
                    log.info(f"Filled doc type: {sel} = {doc_code}")
            except Exception as e:
                log.debug(f"Fill error {sel}: {e}")

        # Try selects for doc type
        for sel_el in form_info['selects']:
            sid = (sel_el.get('id') or sel_el.get('name') or '').lower()
            if any(x in sid for x in ['doctype','doc_type','documenttype','instrumenttype']):
                sel_selector = f"#{sel_el['id']}" if sel_el.get('id') else f"select[name='{sel_el['name']}']"
                try:
                    await page.select_option(sel_selector, value=doc_code)
                    filled_type = True
                    log.info(f"Selected doc type: {sel_selector} = {doc_code}")
                except Exception:
                    try:
                        await page.select_option(sel_selector, label=doc_code)
                        filled_type = True
                    except Exception as e:
                        log.debug(f"Select error: {e}")

        log.info(f"Fill results: start={filled_start}, end={filled_end}, type={filled_type}")

        # Submit form
        submitted = False
        for btn in form_info['buttons']:
            btn_text = (btn.get('text') or '').lower()
            btn_id   = (btn.get('id') or '').lower()
            if any(x in btn_text + btn_id for x in ['search', 'find', 'submit', 'go']):
                try:
                    if btn.get('id'):
                        await page.click(f"#{btn['id']}")
                    else:
                        await page.get_by_text(btn.get('text', ''), exact=False).first.click()
                    submitted = True
                    log.info(f"Clicked submit: {btn}")
                    break
                except Exception as e:
                    log.debug(f"Submit click error: {e}")

        if not submitted:
            for sel in ["input[type='submit']", "button[type='submit']", "button:has-text('Search')"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.click()
                        submitted = True
                        log.info(f"Submitted via fallback: {sel}")
                        break
                except Exception:
                    pass

        if not submitted:
            log.warning("No submit button found — pressing Enter")
            await page.keyboard.press("Enter")

        return filled_start or filled_end

    async def _parse_results(self, page, doc_code: str, cat: str, doc_label: str) -> list[dict]:
        """Extract records from the results page using JS execution."""
        records = []

        # Wait for results to load
        await asyncio.sleep(3)

        # Try to extract table data via JS
        table_data = await page.evaluate("""() => {
            const results = [];
            // Try various table selectors
            const tables = document.querySelectorAll('table, [class*=result], [class*=grid], [class*=record]');
            tables.forEach(table => {
                const rows = table.querySelectorAll('tr');
                if (rows.length < 2) return;
                const headers = Array.from(rows[0].querySelectorAll('th,td')).map(c => c.innerText.trim().toLowerCase());
                for (let i = 1; i < rows.length; i++) {
                    const cells = Array.from(rows[i].querySelectorAll('td'));
                    if (cells.length < 2) continue;
                    const row = {_headers: headers};
                    cells.forEach((c, idx) => {
                        row['col_' + idx] = c.innerText.trim();
                        const link = c.querySelector('a');
                        if (link) row['link_' + idx] = link.href;
                    });
                    results.push(row);
                }
            });
            return results;
        }""")

        log.info(f"Table rows found: {len(table_data)}")

        for row in table_data:
            try:
                headers = row.get('_headers', [])

                def hcol(frag):
                    for i, h in enumerate(headers):
                        if frag in h:
                            return row.get(f'col_{i}', '')
                    return ''

                def col(i):
                    return row.get(f'col_{i}', '')

                def link(i):
                    return row.get(f'link_{i}', '')

                doc_num  = hcol('cfn') or hcol('doc') or hcol('instrument') or col(0)
                filed    = hcol('date') or hcol('record') or col(1)
                grantor  = hcol('grantor') or hcol('owner') or hcol('party') or col(2)
                grantee  = hcol('grantee') or col(3)
                legal    = hcol('legal') or col(4)
                amount_s = hcol('amount') or hcol('consider') or col(5)

                if not doc_num or len(doc_num) < 2:
                    continue

                # Parse date
                filed_clean = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
                    try:
                        filed_clean = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                # Parse amount
                amount = None
                if amount_s:
                    try:
                        amount = float(re.sub(r"[^\d.]", "", amount_s))
                    except Exception:
                        pass

                # Find clerk URL
                clerk_url = ""
                for i in range(10):
                    lnk = link(i)
                    if lnk and 'miamidadeclerk' in lnk:
                        clerk_url = lnk
                        break
                if not clerk_url:
                    clerk_url = f"https://www2.miamidadeclerk.gov/ocs/Search.aspx?doctype={doc_code}"

                records.append({
                    "doc_num":      doc_num.strip(),
                    "doc_type":     doc_code,
                    "filed":        filed_clean or filed,
                    "cat":          cat,
                    "cat_label":    CAT_LABELS.get(cat, cat),
                    "owner":        grantor.strip(),
                    "grantee":      grantee.strip(),
                    "amount":       amount,
                    "legal":        legal.strip(),
                    "prop_address": "",
                    "prop_city":    "",
                    "prop_state":   "FL",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "",
                    "mail_zip":     "",
                    "clerk_url":    clerk_url,
                    "flags":        [],
                    "score":        0,
                })
            except Exception as e:
                log.debug(f"Row parse error: {e}")

        # Also try React/Angular rendered list items
        if not records:
            list_data = await page.evaluate("""() => {
                const results = [];
                const items = document.querySelectorAll('[class*=row], [class*=item], [class*=result], [class*=record]');
                items.forEach(item => {
                    const text = item.innerText.trim();
                    if (text.length > 20 && text.length < 2000) {
                        const link = item.querySelector('a');
                        results.push({text: text, href: link ? link.href : ''});
                    }
                });
                return results.slice(0, 200);
            }""")
            log.info(f"List items found: {len(list_data)}")
            for item in list_data[:5]:
                log.info(f"  Sample item: {item['text'][:100]}")

        return records

    async def _fetch_doc_type(self, page, doc_code: str) -> list[dict]:
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        log.info(f"Fetching {doc_code} ({doc_label})...")
        records = []

        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(self.PORTAL_URL, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(random.uniform(2, 4))

                html, inputs = await self._wait_and_log(page, f"{doc_code} attempt {attempt+1}")

                if not inputs:
                    log.warning(f"No inputs found on attempt {attempt+1}, waiting longer...")
                    await asyncio.sleep(5)
                    html, inputs = await self._wait_and_log(page, f"{doc_code} retry")

                filled = await self._try_fill_form(page, doc_code)

                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(random.uniform(2, 4))

                html2, inputs2 = await self._wait_and_log(page, f"{doc_code} results")

                recs = await self._parse_results(page, doc_code, cat, doc_label)
                records.extend(recs)
                log.info(f"  {doc_code}: {len(recs)} records found")
                break

            except PlaywrightTimeout as e:
                log.warning(f"Timeout on {doc_code} attempt {attempt+1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                log.warning(f"Error on {doc_code} attempt {attempt+1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)

        return records

    async def run(self) -> list[dict]:
        all_records = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--allow-running-insecure-content",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--window-size=1366,768",
                ]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"macOS"',
                }
            )

            # Inject stealth JS on every page
            await context.add_init_script(STEALTH_JS)

            page = await context.new_page()

            # Visit Google first to build realistic browser history
            log.info("Warming up browser session...")
            try:
                await page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1, 2))
            except Exception:
                pass

            # Now visit the portal
            log.info(f"Navigating to clerk portal: {self.PORTAL_URL}")
            try:
                await page.goto(self.PORTAL_URL, wait_until="networkidle", timeout=45000)
            except Exception as e:
                log.warning(f"Initial navigation warning: {e}")

            await asyncio.sleep(3)

            # Log initial page state
            await self._wait_and_log(page, "INITIAL")

            # Screenshot for debugging (saves to data dir)
            try:
                screenshot_path = DATA_DIR / "debug_screenshot.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                log.info(f"Debug screenshot saved: {screenshot_path}")
            except Exception as e:
                log.debug(f"Screenshot error: {e}")

            # Fetch each doc type
            for doc_code in DOC_TYPES:
                try:
                    recs = await self._fetch_doc_type(page, doc_code)
                    all_records.extend(recs)
                except Exception as e:
                    log.error(f"Failed {doc_code}: {e}")

            await browser.close()

        log.info(f"Total raw records: {len(all_records)}")
        return all_records


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def build_output(records: list[dict]) -> dict:
    enriched = []
    for rec in records:
        try:
            score, flags = compute_score_and_flags(rec)
            rec["score"] = score
            rec["flags"] = flags
            enriched.append(rec)
        except Exception as e:
            log.debug(f"Score error: {e}")
            enriched.append(rec)

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Miami-Dade Clerk of Courts Official Records",
        "date_range": {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d"),
            "to":   datetime.now().strftime("%Y-%m-%d"),
        },
        "total":        len(enriched),
        "with_address": sum(1 for r in enriched if r.get("prop_address")),
        "records":      enriched,
    }


def save_ghl_csv(records: list[dict], output_path: Path):
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]

    def split_name(full):
        parts = full.strip().split()
        if len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        return full, ""

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            fn, ln = split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":             fn,
                "Last Name":              ln,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", ""),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "FL"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          DOC_TYPES.get(r.get("doc_type",""), (r.get("doc_type",""),))[0],
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", ""),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                 "Miami-Dade Clerk Official Records",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved: {output_path}")


def main():
    log.info("=" * 60)
    log.info("Miami-Dade Motivated Seller Scraper — Starting")
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info("=" * 60)

    scraper  = ClerkScraper(lookback_days=LOOKBACK_DAYS)
    records  = asyncio.run(scraper.run())
    output   = build_output(records)

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        log.info(f"Saved: {path}")

    today = datetime.now().strftime("%Y%m%d")
    save_ghl_csv(output["records"], DATA_DIR / f"ghl_export_{today}.csv")
    log.info(f"Done. Total: {output['total']} | With address: {output['with_address']}")


if __name__ == "__main__":
    main()
