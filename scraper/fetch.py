"""
Miami-Dade County Motivated Seller Lead Scraper
Calls the portal's internal REST API directly using session cookies.
"""

import json
import csv
import re
import os
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LOOKBACK_DAYS  = 7

CLERK_SESSION  = os.environ.get("CLERK_SESSION", "")
CLERK_NSC      = os.environ.get("CLERK_NSC", "")
CLERK_EMAIL    = os.environ.get("CLERK_EMAIL", "")
CLERK_PASSWORD = os.environ.get("CLERK_PASSWORD", "")

API_BASE = "https://onlineservices.miamidadeclerk.gov/officialrecords/api"

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
    "LIS":  ("Lis Pendens",              "pre-foreclosure"),
    "JUD":  ("Judgement",                "judgment"),
    "LIE":  ("Lien",                     "lien"),
    "FTL":  ("Federal Tax Lien",         "tax-lien"),
    "NCO":  ("Notice of Commencement",   "notice"),
    "PAD":  ("Probate & Administration", "probate"),
    "PRO":  ("Probate Order of Dist.",   "probate"),
    "REL":  ("Release",                  "release"),
    "NTL":  ("Notice of Tax Lien",       "tax-lien"),
    "NCT":  ("Notice of Contest of Lien","lien"),
    "SJU":  ("Satisfaction of Judgment", "judgment"),
    "CLP":  ("Cancellation Lis Pendens", "release"),
}

PORTAL_DOC_NAMES = {
    "LIS":  "LIS PENDENS - LIS",
    "JUD":  "JUDGEMENT - JUD",
    "LIE":  "LIEN - LIE",
    "FTL":  "FEDERAL TAX LIEN - FTL",
    "NCO":  "NOTICE OF COMMENCEMENT - NCO",
    "PAD":  "PROBATE & ADMINISTRATION - PAD",
    "PRO":  "PROBATE ORDER OF DISTRIBUTION - PRO",
    "REL":  "RELEASE - REL",
    "NTL":  "NOTICE OF TAX LIEN - NTL",
    "NCT":  "NOTICE OF CONTEST OF LIEN - NCT",
    "SJU":  "SATISFACTION OF JUDGMENT - SJU",
    "CLP":  "CANCELLATION OF LIS PENDENS - CLP",
}

CAT_LABELS = {
    "pre-foreclosure": "Pre-Foreclosure",
    "tax-distressed":  "Tax Distressed",
    "judgment":        "Judgment",
    "tax-lien":        "Tax / Fed Lien",
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

    if doc_type == "LIS" or cat == "pre-foreclosure":
        flags.append("Lis pendens")
        flags.append("Pre-foreclosure")
    if doc_type in ("JUD", "SJU") or cat == "judgment":
        flags.append("Judgment lien")
    if doc_type in ("FTL", "NTL") or cat == "tax-lien":
        flags.append("Tax lien")
    if doc_type == "LIE" or cat == "lien":
        flags.append("Mechanic lien")
    if doc_type in ("PAD", "PRO") or cat == "probate":
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
# PROPERTY APPRAISER LOOKUP
# ─────────────────────────────────────────────
class PALookup:
    """Looks up property and mailing address from Miami-Dade PA by folio number.
    Scrapes the HTML property search page since the PA uses server-side rendering.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.cache = {}

    def _format_folio(self, folio: str) -> str:
        """Format folio to 13 digits no dashes for URL."""
        return re.sub(r"[^0-9]", "", str(folio)).zfill(13)

    def lookup(self, folio: str) -> dict:
        if not folio or str(folio).strip() in ("", "None", "null", "0"):
            return {}

        folio_clean = self._format_folio(folio)
        if len(folio_clean) < 8:
            return {}

        if folio_clean in self.cache:
            return self.cache[folio_clean]

        result = {}

        # Try the PA property search HTML page
        urls = [
            f"https://appsmiamidadepa.gov/PropertySearch/?folio={folio_clean}",
            f"https://www.miamidade.gov/Apps/PA/propertysearch/?folio={folio_clean}",
        ]

        for url in urls:
            try:
                r = self.session.get(url, timeout=15, allow_redirects=True)
                if r.status_code == 200 and len(r.text) > 500:
                    result = self._parse_pa_html(r.text)
                    if result.get("prop_address"):
                        log.info(f"PA HTML hit for {folio_clean}: {result['prop_address']}")
                        break
            except Exception as e:
                log.debug(f"PA HTML error {url}: {e}")

        self.cache[folio_clean] = result
        if not result.get("prop_address"):
            log.debug(f"PA no result for folio: {folio_clean}")
        return result

    def _parse_pa_html(self, html: str) -> dict:
        """Parse property address and mailing address from PA HTML page."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        def find_field(labels):
            for label in labels:
                # Look for label text then get sibling/next value
                el = soup.find(string=re.compile(label, re.I))
                if el:
                    parent = el.find_parent()
                    if parent:
                        # Try next sibling
                        nxt = parent.find_next_sibling()
                        if nxt:
                            val = nxt.get_text(strip=True)
                            if val and len(val) > 2:
                                return val
                        # Try parent's next sibling
                        nxt2 = parent.find_parent()
                        if nxt2:
                            nxt3 = nxt2.find_next_sibling()
                            if nxt3:
                                val = nxt3.get_text(strip=True)
                                if val and len(val) > 2:
                                    return val
            return ""

        # PA page structure: label divs followed by value divs
        prop_addr = ""
        mail_addr = ""
        mail_city_state_zip = ""

        # Find all text blocks that look like addresses
        all_text = [el.get_text(strip=True) for el in soup.find_all(["div", "p", "span", "td"]) if el.get_text(strip=True)]

        for i, text in enumerate(all_text):
            tl = text.lower()
            if "property address" in tl and i + 1 < len(all_text):
                prop_addr = all_text[i + 1]
            elif "mailing address" in tl and i + 1 < len(all_text):
                mail_addr = all_text[i + 1]
                if i + 2 < len(all_text):
                    mail_city_state_zip = all_text[i + 2]

        # Parse city/state/zip from mail address line
        mail_city, mail_state, mail_zip = "", "FL", ""
        if mail_city_state_zip:
            # Format: "MIAMI SHORES, FL 33138-2923"
            m = re.match(r"^(.+?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", mail_city_state_zip)
            if m:
                mail_city  = m.group(1).strip()
                mail_state = m.group(2)
                mail_zip   = m.group(3)

        # Parse city from prop_addr if it has multiple lines
        prop_city = ""
        if "\n" in prop_addr or len(prop_addr.split()) > 5:
            parts = prop_addr.split("\n")
            if len(parts) >= 2:
                prop_addr = parts[0].strip()
                prop_city = parts[1].strip()

        return {
            "prop_address": prop_addr,
            "prop_city":    prop_city,
            "prop_state":   "FL",
            "prop_zip":     "",
            "mail_address": mail_addr,
            "mail_city":    mail_city,
            "mail_state":   mail_state,
            "mail_zip":     mail_zip,
        }

    # Keep old API methods as fallback
    def _parse_pa_response(self, data: dict) -> dict:
        return {}

    def _parse_arcgis_response(self, data: dict) -> dict:
        return {}


class ClerkAPIScraper:

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")
        self.session   = requests.Session()
        self.pa        = PALookup()
        self._setup_session()

    def _setup_session(self):
        """Configure session with cookies and headers."""
        # Build cookie string
        cookie_parts = []
        if CLERK_SESSION:
            cookie_parts.append(f".PremierIDDade={CLERK_SESSION}")
        if CLERK_NSC:
            cookie_parts.append(f"NSC_JOeqtbnye4rqvqae52yysbdjdcwntcw={CLERK_NSC}")

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://onlineservices.miamidadeclerk.gov/officialrecords/StandardSearch.aspx",
            "Origin": "https://onlineservices.miamidadeclerk.gov",
            "Sec-Ch-Ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        })

        if cookie_parts:
            self.session.headers["Cookie"] = "; ".join(cookie_parts)

        log.info(f"Session configured with {len(cookie_parts)} auth cookies")

    def _login_api(self) -> bool:
        """Try to login via API endpoint."""
        if not CLERK_EMAIL or not CLERK_PASSWORD:
            return False

        try:
            # Try the UMS login endpoint
            login_url = "https://www2.miamidadeclerk.gov/PremierServices/api/Account/login"
            payload = {
                "userName": CLERK_EMAIL,
                "password": CLERK_PASSWORD,
            }
            r = self.session.post(login_url, json=payload, timeout=20)
            log.info(f"Login API response: {r.status_code} - {r.text[:200]}")

            if r.status_code == 200:
                data = r.json()
                token = data.get("token") or data.get("access_token") or data.get("sessionId")
                if token:
                    self.session.headers["Authorization"] = f"Bearer {token}"
                    log.info("Got bearer token from login API")
                    return True

            # Try form-based login
            login_url2 = "https://www2.miamidadeclerk.gov/PremierServices/login.aspx"
            r2 = self.session.get(login_url2, timeout=20)
            # Get the page to grab any form tokens
            log.info(f"Login page: {r2.status_code}")

        except Exception as e:
            log.warning(f"Login API error: {e}")

        return False

    def _check_login_status(self) -> bool:
        """Check if we're logged in via the isLoggedIn API."""
        try:
            r = self.session.get(
                f"{API_BASE}/Environment/isLoggedIn",
                timeout=15
            )
            log.info(f"isLoggedIn: {r.status_code} - {r.text[:200]}")
            if r.status_code == 200:
                data = r.json()
                is_logged = data.get("isLoggedIn") or data.get("loggedIn") or data.get("authenticated")
                log.info(f"Logged in status: {is_logged}")
                return bool(is_logged)
        except Exception as e:
            log.warning(f"Login check error: {e}")
        return False

    def _discover_search_api(self):
        """Try to find the search API endpoint by probing common patterns."""
        # First get the status to see what APIs are available
        try:
            r = self.session.get(f"{API_BASE}/Environment/getStatus", timeout=15)
            log.info(f"getStatus: {r.status_code} - {r.text[:300]}")
        except Exception as e:
            log.warning(f"getStatus error: {e}")

        # Try GetDate to verify API is working
        try:
            r = self.session.get(f"{API_BASE}/Environment/GetDate", timeout=15)
            log.info(f"GetDate: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            log.warning(f"GetDate error: {e}")

        # Try to find search endpoints
        search_endpoints = [
            f"{API_BASE}/Search/StandardSearch",
            f"{API_BASE}/Search/NameDocumentSearch",
            f"{API_BASE}/OfficialRecords/Search",
            f"{API_BASE}/Search",
            f"{API_BASE}/Records/Search",
            f"{API_BASE}/Document/Search",
        ]

        for ep in search_endpoints:
            try:
                # Try GET first
                r = self.session.get(ep, timeout=10)
                log.info(f"Probe GET {ep}: {r.status_code} - {r.text[:100]}")

                # Try POST
                r2 = self.session.post(ep, json={}, timeout=10)
                log.info(f"Probe POST {ep}: {r2.status_code} - {r2.text[:100]}")
            except Exception as e:
                log.debug(f"Probe {ep}: {e}")

    def _search_by_doctype(self, doc_code: str) -> list[dict]:
        """Search using the exact API flow discovered from browser Network tab."""
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        records = []

        import urllib.parse

        # Step 1: POST to standardsearch to get a qs token
        # Use the full portal display name
        portal_name = PORTAL_DOC_NAMES.get(doc_code, doc_code)

        # Match exactly what the browser sends - empty dates, doc type only
        # Date format: YYYY-MM-DD, From=oldest, To=newest
        date_from = (datetime.now() - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        date_to   = datetime.now().strftime("%Y-%m-%d")

        search_url = (
            f"{API_BASE}/home/standardsearch"
            f"?partyName="
            f"&dateRangeFrom={date_from}"
            f"&dateRangeTo={date_to}"
            f"&documentType={urllib.parse.quote(portal_name)}"
            f"&searchT={urllib.parse.quote(portal_name)}"
            f"&firstQuery=y"
            f"&searchtype=Name/Document"
        )

        log.info(f"POST standardsearch for {doc_code}...")
        try:
            r = self.session.post(
                search_url,
                headers={"Content-Length": "0", "Content-Type": "application/json; charset=utf-8"},
                timeout=30
            )
            log.info(f"standardsearch {doc_code}: {r.status_code} - {r.text[:300]}")

            if r.status_code != 200:
                log.warning(f"standardsearch failed: {r.status_code}")
                return records

            # Parse the qs token from response
            try:
                data = r.json()
                log.info(f"standardsearch response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                qs = None
                if isinstance(data, dict):
                    qs = data.get("qs") or data.get("queryString") or data.get("token") or data.get("key")
                    # Sometimes it's nested
                    if not qs:
                        for v in data.values():
                            if isinstance(v, str) and len(v) > 20:
                                qs = v
                                break
                elif isinstance(data, str) and len(data) > 10:
                    qs = data

                log.info(f"qs token: {str(qs)[:100] if qs else 'NOT FOUND'}")

                if not qs:
                    log.warning(f"No qs token in response for {doc_code}")
                    return records

            except Exception as e:
                log.warning(f"Could not parse standardsearch response: {e} - {r.text[:200]}")
                return records

        except Exception as e:
            log.error(f"standardsearch request error: {e}")
            return records

        # Step 2: GET getStandardRecords with the qs token
        import time
        time.sleep(0.5)

        # Add date range to the records fetch
        records_url = (
            f"{API_BASE}/SearchResults/getStandardRecords"
            f"?qs={urllib.parse.quote(str(qs))}"
            f"&dateRangeFrom={urllib.parse.quote(self.date_from)}"
            f"&dateRangeTo={urllib.parse.quote(self.date_to)}"
        )
        log.info(f"GET getStandardRecords for {doc_code}...")

        try:
            r2 = self.session.get(records_url, timeout=30)
            log.info(f"getStandardRecords {doc_code}: {r2.status_code} - {r2.text[:300]}")

            if r2.status_code == 200:
                parsed = self._parse_api_response(r2, doc_code, cat, doc_label, self.lookback_days)
                records.extend(parsed)
                log.info(f"Parsed {len(parsed)} records for {doc_code}")
            else:
                log.warning(f"getStandardRecords failed: {r2.status_code}")

        except Exception as e:
            log.error(f"getStandardRecords error: {e}")

        return records

    def _parse_api_response(self, response, doc_code, cat, doc_label, lookback_days=7) -> list[dict]:
        """Parse API JSON response into records."""
        records = []
        try:
            data = response.json()
            log.info(f"API response type: {type(data)} keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")

            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # The portal returns recordingModels as the key
                for key in ["recordingModels", "records", "results", "data", "items", "documents", "officialRecords"]:
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        log.info(f"Found {len(items)} items under key '{key}'")
                        break
                # Log first item structure for debugging
                if items:
                    log.info(f"First item keys: {list(items[0].keys()) if isinstance(items[0], dict) else items[0]}")
                else:
                    log.info(f"No items found. Response keys: {list(data.keys())}")
                    # Log the full response for first doc type to understand structure
                    log.info(f"Full response sample: {str(data)[:500]}")

            log.info(f"Items found: {len(items)}")

            for item in items:
                try:
                    # Log raw item for debugging first record
                    if len(records) == 0:
                        log.info(f"Raw item sample: {str(item)[:400]}")

                    # Use exact field names from API response
                    raw_date = item.get("reC_DATE") or item.get("doC_DATE") or ""
                    filed = ""
                    if raw_date:
                        for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
                            try:
                                filed = datetime.strptime(str(raw_date)[:20].strip(), fmt).strftime("%Y-%m-%d")
                                break
                            except Exception:
                                pass

                    # DATE FILTER - only keep records from last N days
                    if not filed:
                        continue
                    try:
                        rec_dt = datetime.strptime(filed, "%Y-%m-%d")
                        cutoff_dt = datetime.now() - timedelta(days=lookback_days)
                        if rec_dt < cutoff_dt:
                            continue
                    except Exception:
                        continue

                    amount = None
                    raw_amt = item.get("consideratioN_1") or item.get("consideratioN_2")
                    if raw_amt:
                        try:
                            amount = float(str(raw_amt))
                            if amount == 0:
                                amount = None
                        except Exception:
                            pass

                    cfn_year = str(item.get("cfN_YEAR") or "")
                    cfn_seq  = str(item.get("cfN_SEQ") or "")
                    doc_num  = item.get("clerk_File") or (f"{cfn_year} R {cfn_seq}" if cfn_year and cfn_seq else cfn_seq)

                    # Build clerk URL using the record's qs token for direct link
                    rec_qs = item.get("qs", "")
                    if rec_qs:
                        import urllib.parse as _up
                        clerk_url = f"https://onlineservices.miamidadeclerk.gov/officialrecords/DocumentDetail.aspx?qs={_up.quote(rec_qs)}"
                    else:
                        clerk_url = f"https://onlineservices.miamidadeclerk.gov/officialrecords/StandardSearch.aspx"

                    # Start with address from clerk record
                    prop_addr_raw = str(item.get("address") or item.get("addressnounit") or "").strip()
                    folio_raw = item.get("foliO_NUMBER") or item.get("folioNumber") or item.get("folio") or ""
                    # Log first 3 records to see folio values
                    if len(records) < 3:
                        log.info(f"FOLIO DEBUG: folio_raw={folio_raw!r} addr={prop_addr_raw!r}")
                        log.info(f"FOLIO DEBUG full item folio key: foliO_NUMBER={item.get('foliO_NUMBER')!r}")

                    # Enrich with PA data using folio number
                    folio = str(folio_raw).strip()
                    if folio and folio not in ("", "None"):
                        log.info(f"PA lookup for folio: {folio}")
                        pa_data = self.pa.lookup(folio)
                    else:
                        pa_data = {}

                    records.append({
                        "doc_num":      doc_num,
                        "doc_type":     doc_code,
                        "filed":        filed,
                        "cat":          cat,
                        "cat_label":    CAT_LABELS.get(cat, cat),
                        "owner":        str(item.get("firsT_PARTY") or "").strip(),
                        "grantee":      str(item.get("seconD_PARTY") or "").strip(),
                        "amount":       amount,
                        "legal":        str(item.get("legaL_DESCRIPTION") or "").strip(),
                        "prop_address": pa_data.get("prop_address") or prop_addr_raw,
                        "prop_city":    pa_data.get("prop_city", ""),
                        "prop_state":   "FL",
                        "prop_zip":     pa_data.get("prop_zip", ""),
                        "mail_address": pa_data.get("mail_address", ""),
                        "mail_city":    pa_data.get("mail_city", ""),
                        "mail_state":   pa_data.get("mail_state", "FL"),
                        "mail_zip":     pa_data.get("mail_zip", ""),
                        "clerk_url":    clerk_url,
                        "flags":        [],
                        "score":        0,
                    })
                except Exception as e:
                    log.debug(f"Item parse error: {e}")

        except Exception as e:
            log.warning(f"Response parse error: {e} - {response.text[:200]}")

        return records

    def run(self) -> list[dict]:
        all_records = []

        # Try API login first
        self._login_api()

        # Check login status
        self._check_login_status()

        # Discover available API endpoints
        self._discover_search_api()

        # Search each doc type
        for doc_code in DOC_TYPES:
            try:
                recs = self._search_by_doctype(doc_code)
                log.info(f"{doc_code}: {len(recs)} records")
                all_records.extend(recs)
            except Exception as e:
                log.error(f"Failed {doc_code}: {e}")

        log.info(f"Total: {len(all_records)}")
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
        except Exception:
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
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info(f"Auth cookies: session={'yes' if CLERK_SESSION else 'no'}, nsc={'yes' if CLERK_NSC else 'no'}")
    log.info("=" * 60)

    scraper = ClerkAPIScraper(lookback_days=LOOKBACK_DAYS)
    records = scraper.run()
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
