"""
Miami-Dade County Motivated Seller Lead Scraper
Logs in to the Clerk portal to bypass reCAPTCHA, then searches by doc type and date range.
"""

import asyncio
import json
import csv
import re
import os
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LOOKBACK_DAYS  = 7
MAX_RETRIES    = 3
RETRY_DELAY    = 5

CLERK_EMAIL    = os.environ.get("CLERK_EMAIL", "")
CLERK_PASSWORD = os.environ.get("CLERK_PASSWORD", "")

PORTAL_HOME    = "https://onlineservices.miamidadeclerk.gov/officialrecords"
LOGIN_URL      = "https://www2.miamidadeclerk.gov/PremierServices/login.aspx"
SEARCH_URL     = f"{PORTAL_HOME}/StandardSearch.aspx"
NAME_DOC_URL   = f"{PORTAL_HOME}/SearchName.aspx"

ROOT_DIR       = Path(__file__).parent.parent
DASHBOARD_DIR  = ROOT_DIR / "dashboard"
DATA_DIR       = ROOT_DIR / "data"

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
# SCRAPER
# ─────────────────────────────────────────────
class ClerkScraper:

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")
        self.logged_in = False

    async def _screenshot(self, page, name):
        try:
            path = DATA_DIR / f"debug_{name}.png"
            await page.screenshot(path=str(path), full_page=True)
            log.info(f"Screenshot: {path}")
        except Exception as e:
            log.debug(f"Screenshot error: {e}")

    async def _login(self, page) -> bool:
        """Inject session cookie to bypass login and reCAPTCHA."""
        clerk_session = os.environ.get("CLERK_SESSION", "")

        if clerk_session:
            log.info("Injecting session cookie...")
            try:
                # First visit the portal to establish context
                await page.goto(PORTAL_HOME, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)

                # Inject the PremierID session cookie
                await page.context.add_cookies([
                    {
                        "name": ".PremierID",
                        "value": clerk_session,
                        "domain": "onlineservices.miamidadeclerk.gov",
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "name": ".PremierID",
                        "value": clerk_session,
                        "domain": "www2.miamidadeclerk.gov",
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    }
                ])
                log.info("Session cookie injected")

                # Reload to apply cookie
                await page.goto(PORTAL_HOME, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)
                await self._screenshot(page, "after_cookie_inject")

                content = await page.content()
                text = await page.evaluate("() => document.body ? document.body.innerText.slice(0,300) : ''")
                log.info(f"Portal after cookie: {text[:200].replace(chr(10),' ')}")

                self.logged_in = True
                return True
            except Exception as e:
                log.error(f"Cookie injection error: {e}")

        # Fall back to form login
        if not CLERK_EMAIL or not CLERK_PASSWORD:
            log.warning("No credentials provided — will attempt without login")
            return False

        log.info(f"Logging in as {CLERK_EMAIL}...")
        try:
            await page.goto(PORTAL_HOME, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Click Register/Login link
            for sel in ["text=Register/Login", "text=Login", "a[href*='login']", "a[href*='Login']"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.click()
                        await asyncio.sleep(2)
                        log.info(f"Clicked login via: {sel}")
                        break
                except Exception:
                    pass

            await self._screenshot(page, "login_page")

            # The login page has two sections:
            # Left: "Registered User" with User ID/Email + Password + LOGIN button
            # Right: "New Users" registration
            # We need to fill the LEFT side fields

            # Find all inputs on the page
            all_inputs = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('input')).map((el, i) => ({
                    index: i,
                    id: el.id, name: el.name, type: el.type,
                    placeholder: el.placeholder,
                    visible: el.offsetParent !== null
                }));
            }""")
            log.info(f"Login page inputs: {all_inputs}")

            # Fill by index - first text input is email, first password is password
            # Use nth-of-type or position-based filling
            text_inputs = [i for i in all_inputs if i['type'] in ('text', 'email', '') and i['visible']]
            pass_inputs = [i for i in all_inputs if i['type'] == 'password' and i['visible']]

            log.info(f"Text inputs: {text_inputs}")
            log.info(f"Pass inputs: {pass_inputs}")

            # Fill email - try by label proximity, then by position
            filled_email = False
            for strategy in [
                "input[type='text']:first-of-type",
                "input:not([type='password']):not([type='submit']):not([type='hidden'])",
                "//label[contains(text(),'User') or contains(text(),'Email')]/following-sibling::input",
                "//div[contains(@class,'register') or contains(text(),'Registered')]//input[@type='text']",
            ]:
                try:
                    if strategy.startswith("//"):
                        el = await page.query_selector(f"xpath={strategy}")
                    else:
                        # Get first matching element in left column
                        els = await page.query_selector_all(strategy)
                        el = els[0] if els else None
                    if el and await el.is_visible():
                        await el.fill(CLERK_EMAIL)
                        log.info(f"Filled email via: {strategy}")
                        filled_email = True
                        break
                except Exception as e:
                    log.debug(f"Email fill try {strategy}: {e}")

            if not filled_email:
                # Last resort: click on first visible text input and type
                try:
                    await page.click("input[type='text']")
                    await page.keyboard.type(CLERK_EMAIL)
                    log.info("Filled email by keyboard")
                    filled_email = True
                except Exception as e:
                    log.warning(f"Could not fill email: {e}")

            # Fill password
            filled_pass = False
            for strategy in ["input[type='password']"]:
                try:
                    els = await page.query_selector_all(strategy)
                    el = els[0] if els else None
                    if el and await el.is_visible():
                        await el.fill(CLERK_PASSWORD)
                        log.info(f"Filled password via: {strategy}")
                        filled_pass = True
                        break
                except Exception as e:
                    log.debug(f"Password fill try: {e}")

            log.info(f"Fill status: email={filled_email}, password={filled_pass}")

            # Click LOGIN button (not REGISTER)
            submitted = False
            for strategy in [
                "//button[normalize-space(text())='LOGIN']",
                "//input[@value='LOGIN' or @value='Login']",
                "button:has-text('LOGIN')",
                "input[value='LOGIN']",
            ]:
                try:
                    if strategy.startswith("//"):
                        el = await page.query_selector(f"xpath={strategy}")
                    else:
                        el = await page.query_selector(strategy)
                    if el and await el.is_visible():
                        await el.click()
                        log.info(f"Clicked LOGIN via: {strategy}")
                        submitted = True
                        break
                except Exception as e:
                    log.debug(f"Submit try {strategy}: {e}")

            if not submitted:
                await page.keyboard.press("Enter")
                log.info("Submitted via Enter key")

            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(2)
            await self._screenshot(page, "after_login")

            # Check if logged in
            url = page.url
            content = await page.content()
            if any(x in content.lower() for x in ["my account", "logout", "welcome", "nicholas", "nicolas"]):
                log.info("Login successful!")
                self.logged_in = True
                return True
            else:
                log.warning(f"Login may have failed. URL: {url}")
                log.warning(f"Page snippet: {content[:300]}")
                return False

        except Exception as e:
            log.error(f"Login error: {e}")
            return False

    async def _search_doc_type(self, page, doc_code: str) -> list[dict]:
        """Navigate to Name/Document search and search by doc type + date range."""
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        records = []

        log.info(f"Searching {doc_code} ({doc_label}) from {self.date_from} to {self.date_to}")

        try:
            # Go to the search page
            await page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            await self._screenshot(page, f"search_{doc_code}_start")

            # Log all inputs
            form_info = await page.evaluate("""() => {
                const r = {inputs: [], selects: [], links: []};
                document.querySelectorAll('input').forEach(e => r.inputs.push({
                    id: e.id, name: e.name, type: e.type,
                    placeholder: e.placeholder, value: e.value.slice(0,30)
                }));
                document.querySelectorAll('select').forEach(e => r.selects.push({
                    id: e.id, name: e.name,
                    options: Array.from(e.options).map(o => o.value + ':' + o.text).slice(0,20)
                }));
                document.querySelectorAll('a').forEach(e => {
                    if (e.innerText.trim()) r.links.push({text: e.innerText.trim(), href: e.href});
                });
                return r;
            }""")

            log.info(f"Inputs: {form_info['inputs']}")
            log.info(f"Selects: {form_info['selects']}")
            log.info(f"Nav links: {[l for l in form_info['links'] if any(x in l['text'].lower() for x in ['search','name','doc','record'])]}")

            # Try to find and click "Name/Document" search link in sidebar
            for link in form_info['links']:
                if any(x in link['text'].lower() for x in ['name/doc', 'name doc', 'document']):
                    try:
                        await page.goto(link['href'], wait_until="networkidle", timeout=20000)
                        await asyncio.sleep(2)
                        log.info(f"Navigated to: {link['href']}")
                        break
                    except Exception:
                        pass

            await self._screenshot(page, f"search_{doc_code}_form")

            # Re-read form after navigation
            form_info2 = await page.evaluate("""() => {
                const r = {inputs: [], selects: []};
                document.querySelectorAll('input').forEach(e => r.inputs.push({
                    id: e.id, name: e.name, type: e.type,
                    placeholder: e.placeholder, value: e.value.slice(0,30),
                    label: ''
                }));
                document.querySelectorAll('select').forEach(e => r.selects.push({
                    id: e.id, name: e.name,
                    options: Array.from(e.options).map(o => o.value + ':' + o.text).slice(0,30)
                }));
                return r;
            }""")
            log.info(f"Form inputs after nav: {form_info2['inputs']}")
            log.info(f"Form selects after nav: {form_info2['selects']}")

            # Fill date range fields
            filled = 0
            for inp in form_info2['inputs']:
                iid   = (inp.get('id') or '').lower()
                iname = (inp.get('name') or '').lower()
                iph   = (inp.get('placeholder') or '').lower()
                key   = iid + iname + iph

                sel = f"#{inp['id']}" if inp.get('id') else f"input[name='{inp['name']}']" if inp.get('name') else None
                if not sel:
                    continue

                if any(x in key for x in ['startdate','start_date','datefrom','date_from','begindate','fromdate','datebegin','recordstart']):
                    try:
                        await page.fill(sel, self.date_from)
                        log.info(f"Filled start date {sel} = {self.date_from}")
                        filled += 1
                    except Exception as e:
                        log.debug(f"Fill error: {e}")

                elif any(x in key for x in ['enddate','end_date','dateto','date_to','throughdate','todate','dateend','recordend']):
                    try:
                        await page.fill(sel, self.date_to)
                        log.info(f"Filled end date {sel} = {self.date_to}")
                        filled += 1
                    except Exception as e:
                        log.debug(f"Fill error: {e}")

                elif any(x in key for x in ['doctype','doc_type','documenttype','instrumenttype','recordtype']):
                    try:
                        await page.fill(sel, doc_code)
                        log.info(f"Filled doc type {sel} = {doc_code}")
                        filled += 1
                    except Exception as e:
                        log.debug(f"Fill error: {e}")

            # Fill doc type selects
            for sel_el in form_info2['selects']:
                sid = (sel_el.get('id') or sel_el.get('name') or '').lower()
                if any(x in sid for x in ['doctype','doc_type','documenttype','instrumenttype']):
                    sel = f"#{sel_el['id']}" if sel_el.get('id') else f"select[name='{sel_el['name']}']"
                    try:
                        await page.select_option(sel, value=doc_code)
                        log.info(f"Selected doc type {sel} = {doc_code}")
                        filled += 1
                    except Exception:
                        try:
                            await page.select_option(sel, label=doc_code)
                            filled += 1
                        except Exception as e:
                            log.debug(f"Select error: {e}")

            log.info(f"Filled {filled} fields")

            # Submit
            submitted = False
            for btn_sel in [
                "input[type='submit']",
                "button[type='submit']",
                "button:has-text('Search')",
                "input[value*='Search']",
                "button:has-text('Find')",
            ]:
                try:
                    el = await page.query_selector(btn_sel)
                    if el:
                        await el.click()
                        submitted = True
                        log.info(f"Submitted via {btn_sel}")
                        break
                except Exception:
                    pass

            if not submitted:
                await page.keyboard.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=30000)
            await asyncio.sleep(3)
            await self._screenshot(page, f"results_{doc_code}")

            # Parse results
            records = await self._parse_results(page, doc_code, cat, doc_label)
            log.info(f"  {doc_code}: {len(records)} records")

        except Exception as e:
            log.error(f"Search error for {doc_code}: {e}")

        return records

    async def _parse_results(self, page, doc_code, cat, doc_label) -> list[dict]:
        records = []

        # Log page text for debugging
        page_text = await page.evaluate("() => document.body ? document.body.innerText.slice(0, 500) : ''")
        log.info(f"Results page text: {page_text[:300].replace(chr(10), ' ')}")

        # Extract all card/table data via JS
        data = await page.evaluate("""() => {
            const results = [];

            // Try table rows
            document.querySelectorAll('table').forEach(table => {
                const rows = table.querySelectorAll('tr');
                if (rows.length < 2) return;
                const headers = Array.from(rows[0].querySelectorAll('th,td')).map(c => c.innerText.trim().toLowerCase());
                for (let i = 1; i < rows.length; i++) {
                    const cells = Array.from(rows[i].querySelectorAll('td'));
                    if (cells.length < 2) continue;
                    const row = {_type: 'table', _headers: headers};
                    cells.forEach((c, idx) => {
                        row['col_' + idx] = c.innerText.trim();
                        const a = c.querySelector('a');
                        if (a) row['link_' + idx] = a.href;
                    });
                    results.push(row);
                }
            });

            // Try card format (new portal)
            document.querySelectorAll('[class*="card"], [class*="result"], [class*="record"], [class*="item"]').forEach(card => {
                const text = card.innerText.trim();
                if (text.length < 10 || text.length > 3000) return;
                const links = Array.from(card.querySelectorAll('a')).map(a => a.href);
                results.push({_type: 'card', text: text, links: links});
            });

            return results;
        }""")

        log.info(f"Raw data items: {len(data)}")

        for item in data:
            try:
                if item.get('_type') == 'table':
                    headers = item.get('_headers', [])

                    def hcol(frag):
                        for i, h in enumerate(headers):
                            if frag in h:
                                return item.get(f'col_{i}', '')
                        return ''

                    def col(i):
                        return item.get(f'col_{i}', '')

                    doc_num  = hcol('cfn') or hcol('doc') or hcol('file') or hcol('instrument') or col(0)
                    filed    = hcol('date') or hcol('record') or col(1)
                    grantor  = hcol('grantor') or hcol('owner') or hcol('party') or col(2)
                    grantee  = hcol('grantee') or col(3)
                    legal    = hcol('legal') or col(4)
                    amount_s = hcol('amount') or hcol('consider') or col(5)

                    clerk_url = ""
                    for i in range(8):
                        lnk = item.get(f'link_{i}', '')
                        if lnk and 'clerk' in lnk.lower():
                            clerk_url = lnk
                            break

                elif item.get('_type') == 'card':
                    # Parse card text
                    text = item.get('text', '')
                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    doc_num  = lines[0] if lines else ''
                    filed    = ''
                    grantor  = ''
                    grantee  = ''
                    legal    = ''
                    amount_s = ''
                    clerk_url = item.get('links', [''])[0] if item.get('links') else ''

                    # Try to extract fields from card text
                    for line in lines:
                        ll = line.lower()
                        if 'recorded' in ll or 'filed' in ll or re.search(r'\d{1,2}/\d{1,2}/\d{4}', line):
                            dates = re.findall(r'\d{1,2}/\d{1,2}/\d{4}', line)
                            if dates:
                                filed = dates[0]
                        if 'grantor' in ll or 'owner' in ll:
                            grantor = line.split(':', 1)[-1].strip()
                        if 'grantee' in ll:
                            grantee = line.split(':', 1)[-1].strip()
                        if '$' in line or 'amount' in ll:
                            amount_s = line
                else:
                    continue

                if not doc_num or len(str(doc_num).strip()) < 2:
                    continue

                # Parse date
                filed_clean = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
                    try:
                        filed_clean = datetime.strptime(str(filed)[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                # Parse amount
                amount = None
                if amount_s:
                    try:
                        amount = float(re.sub(r"[^\d.]", "", str(amount_s)))
                        if amount == 0:
                            amount = None
                    except Exception:
                        pass

                if not clerk_url:
                    clerk_url = f"https://onlineservices.miamidadeclerk.gov/officialrecords/DocumentDetail.aspx?cfn={doc_num}"

                records.append({
                    "doc_num":      str(doc_num).strip(),
                    "doc_type":     doc_code,
                    "filed":        filed_clean or str(filed),
                    "cat":          cat,
                    "cat_label":    CAT_LABELS.get(cat, cat),
                    "owner":        str(grantor).strip(),
                    "grantee":      str(grantee).strip(),
                    "amount":       amount,
                    "legal":        str(legal).strip(),
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
                log.debug(f"Parse error: {e}")

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
                    "--window-size=1366,768",
                ]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/New_York",
            )

            # Stealth: hide webdriver
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            page = await context.new_page()

            # Login first
            logged_in = await self._login(page)
            log.info(f"Login status: {logged_in}")

            # Search each doc type
            for doc_code in DOC_TYPES:
                try:
                    recs = await self._search_doc_type(page, doc_code)
                    all_records.extend(recs)
                    await asyncio.sleep(random.uniform(1, 2))
                except Exception as e:
                    log.error(f"Failed {doc_code}: {e}")

            # Save final screenshot
            await self._screenshot(page, "final")
            await browser.close()

        log.info(f"Total raw records: {len(all_records)}")
        return all_records


# ─────────────────────────────────────────────
# PIPELINE
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
        return (parts[0], " ".join(parts[1:])) if len(parts) >= 2 else (full, "")

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
    log.info("Miami-Dade Motivated Seller Scraper")
    log.info(f"Lookback: {LOOKBACK_DAYS} days | Login: {'yes' if CLERK_EMAIL else 'no'}")
    log.info("=" * 60)

    scraper = ClerkScraper(lookback_days=LOOKBACK_DAYS)
    records = asyncio.run(scraper.run())
    output  = build_output(records)

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        log.info(f"Saved: {path}")

    today = datetime.now().strftime("%Y%m%d")
    save_ghl_csv(output["records"], DATA_DIR / f"ghl_export_{today}.csv")
    log.info(f"Done. Total: {output['total']} | With address: {output['with_address']}")


if __name__ == "__main__":
    main()
