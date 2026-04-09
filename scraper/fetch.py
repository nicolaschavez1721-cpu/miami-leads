"""
Miami-Dade County Motivated Seller Lead Scraper
Fetches foreclosure, lien, probate, and related records from the Clerk portal.
Enriches with property appraiser bulk parcel data.
"""

import asyncio
import json
import csv
import io
import os
import re
import time
import logging
import zipfile
import struct
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CLERK_BASE_URL   = "https://www.miamidadeclerk.gov/clerk/home.page"
CLERK_SEARCH_URL = "https://www2.miami-dadeclerk.com/officialrecords/Search.aspx"
PA_BULK_URL      = "https://www.miamidade.gov/pa/download.asp"

LOOKBACK_DAYS    = 7
MAX_RETRIES      = 3
RETRY_DELAY      = 3   # seconds

ROOT_DIR         = Path(__file__).parent.parent
DASHBOARD_DIR    = ROOT_DIR / "dashboard"
DATA_DIR         = ROOT_DIR / "data"

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
    "LP":       ("Lis Pendens",                 "pre-foreclosure"),
    "NOFC":     ("Notice of Foreclosure",        "pre-foreclosure"),
    "TAXDEED":  ("Tax Deed",                     "tax-distressed"),
    "JUD":      ("Judgment",                     "judgment"),
    "CCJ":      ("Certified Judgment",           "judgment"),
    "DRJUD":    ("Domestic Judgment",            "judgment"),
    "LNCORPTX": ("Corp Tax Lien",               "tax-lien"),
    "LNIRS":    ("IRS Lien",                    "tax-lien"),
    "LNFED":    ("Federal Lien",                "tax-lien"),
    "LN":       ("Lien",                        "lien"),
    "LNMECH":   ("Mechanic Lien",               "lien"),
    "LNHOA":    ("HOA Lien",                    "lien"),
    "MEDLN":    ("Medicaid Lien",               "lien"),
    "PRO":      ("Probate Document",            "probate"),
    "NOC":      ("Notice of Commencement",      "notice"),
    "RELLP":    ("Release of Lis Pendens",      "release"),
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
def compute_score_and_flags(record: dict) -> tuple[int, list[str]]:
    flags = []
    score = 30  # base

    doc_type = record.get("doc_type", "")
    cat      = record.get("cat", "")
    amount   = record.get("amount") or 0
    filed    = record.get("filed", "")
    owner    = record.get("owner", "") or ""

    # flag detection
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

    # score additions
    score += len(flags) * 10

    # LP + FC combo bonus
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    if amount and float(amount) > 100_000:
        score += 15
    elif amount and float(amount) > 50_000:
        score += 10

    if "New this week" in flags:
        score += 5

    if record.get("prop_address"):
        score += 5

    return min(score, 100), list(dict.fromkeys(flags))  # dedup, cap at 100


# ─────────────────────────────────────────────
# PROPERTY APPRAISER LOOKUP
# ─────────────────────────────────────────────
class ParcelLookup:
    """Downloads and parses Miami-Dade PA bulk DBF parcel file."""

    def __init__(self):
        self.by_name:   dict[str, dict] = {}
        self.by_parcel: dict[str, dict] = {}
        self._loaded = False

    def _try_download_pa(self) -> Optional[bytes]:
        """Try several known PA bulk data download endpoints."""
        urls = [
            "https://www.miamidade.gov/pa/download_files/NAL.zip",
            "https://www.miamidade.gov/pa/download_files/Nal.zip",
            "https://opendata.miamidade.gov/api/views/4bkp-rqzg/rows.csv?accessType=DOWNLOAD",
        ]
        for url in urls:
            for attempt in range(MAX_RETRIES):
                try:
                    log.info(f"PA download attempt {attempt+1}: {url}")
                    r = requests.get(url, timeout=120, stream=True)
                    if r.status_code == 200:
                        log.info(f"PA download success from {url}")
                        return r.content
                    log.warning(f"PA download HTTP {r.status_code} from {url}")
                except Exception as e:
                    log.warning(f"PA download error: {e}")
                    time.sleep(RETRY_DELAY)
        return None

    def _parse_dbf_bytes(self, data: bytes) -> list[dict]:
        """Minimal pure-Python DBF parser (no external lib needed)."""
        if len(data) < 32:
            return []
        try:
            num_records = struct.unpack_from("<I", data, 4)[0]
            header_size = struct.unpack_from("<H", data, 8)[0]
            record_size = struct.unpack_from("<H", data, 10)[0]

            fields = []
            offset = 32
            while offset < header_size - 1:
                if data[offset] == 0x0D:
                    break
                name = data[offset:offset+11].split(b"\x00")[0].decode("ascii", errors="replace").strip()
                ftype = chr(data[offset+11])
                flen  = data[offset+16]
                fields.append((name, ftype, flen))
                offset += 32

            records = []
            rec_start = header_size
            for _ in range(num_records):
                if rec_start + record_size > len(data):
                    break
                row_bytes = data[rec_start:rec_start + record_size]
                if row_bytes[0] == 0x2A:  # deleted
                    rec_start += record_size
                    continue
                row = {}
                col_off = 1
                for (fname, ftype, flen) in fields:
                    val = row_bytes[col_off:col_off+flen].decode("latin-1", errors="replace").strip()
                    row[fname] = val
                    col_off += flen
                records.append(row)
                rec_start += record_size
            return records
        except Exception as e:
            log.error(f"DBF parse error: {e}")
            return []

    def _normalize(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _name_variants(self, raw: str) -> list[str]:
        raw = self._normalize(raw)
        raw_nc = raw.replace(",", "").strip()
        parts = raw_nc.split()
        variants = [raw, raw_nc]
        if len(parts) >= 2:
            # LAST FIRST → FIRST LAST
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            # LAST, FIRST
            variants.append(f"{parts[0]}, {' '.join(parts[1:])}")
        return list(dict.fromkeys(variants))

    def _row_to_parcel(self, row: dict) -> dict:
        def g(*keys):
            for k in keys:
                v = row.get(k, "").strip()
                if v:
                    return v
            return ""

        return {
            "prop_address": g("SITE_ADDR", "SITEADDR", "SITE_ADDRESS"),
            "prop_city":    g("SITE_CITY", "SITECITY"),
            "prop_state":   "FL",
            "prop_zip":     g("SITE_ZIP", "SITEZIP"),
            "mail_address": g("ADDR_1", "MAILADR1", "MAIL_ADDR1"),
            "mail_city":    g("CITY", "MAILCITY", "MAIL_CITY"),
            "mail_state":   g("STATE", "MAILSTATE", "MAIL_STATE") or "FL",
            "mail_zip":     g("ZIP", "MAILZIP", "MAIL_ZIP"),
        }

    def load(self):
        if self._loaded:
            return
        log.info("Loading PA parcel data...")
        raw = self._try_download_pa()
        if not raw:
            log.warning("Could not download PA data — address enrichment disabled")
            self._loaded = True
            return

        # Handle ZIP containing DBF
        if raw[:2] == b"PK":
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
                    csv_names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
                    if dbf_names:
                        raw = zf.read(dbf_names[0])
                    elif csv_names:
                        raw = zf.read(csv_names[0])
                        self._load_csv(raw)
                        self._loaded = True
                        return
            except Exception as e:
                log.error(f"ZIP extract error: {e}")
                self._loaded = True
                return

        # CSV fallback (opendata)
        if raw[:3] in (b"ï»¿", b"\xef\xbb\xbf") or raw[0:1] == b'"' or raw[0:1].isalpha():
            self._load_csv(raw)
            self._loaded = True
            return

        rows = self._parse_dbf_bytes(raw)
        log.info(f"Parsed {len(rows)} parcel records")
        self._index_rows(rows)
        self._loaded = True

    def _load_csv(self, raw: bytes):
        text = raw.decode("latin-1", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        log.info(f"Loaded {len(rows)} parcel records from CSV")
        self._index_rows(rows)

    def _index_rows(self, rows: list[dict]):
        for row in rows:
            def g(*keys):
                for k in keys:
                    v = row.get(k, "").strip()
                    if v:
                        return v
                return ""
            owner = g("OWN1", "OWNER", "OWNER1", "OWN_NAME")
            parcel = g("PARCEL", "PARCELID", "FOLIO")
            parcel_data = self._row_to_parcel(row)
            if parcel:
                self.by_parcel[parcel] = parcel_data
            if owner:
                for v in self._name_variants(owner):
                    self.by_name[v] = parcel_data
        log.info(f"Indexed {len(self.by_name)} name entries, {len(self.by_parcel)} parcels")

    def lookup(self, owner_name: str) -> Optional[dict]:
        if not owner_name:
            return None
        for v in self._name_variants(owner_name):
            if v in self.by_name:
                return self.by_name[v]
        return None


# ─────────────────────────────────────────────
# CLERK PORTAL SCRAPER (Playwright)
# ─────────────────────────────────────────────
class ClerkScraper:
    BASE = "https://www2.miami-dadeclerk.com/officialrecords"
    SEARCH = f"{BASE}/Search.aspx"

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")

    async def _safe_fill(self, page, selector, value):
        try:
            await page.wait_for_selector(selector, timeout=8000)
            await page.fill(selector, value)
        except Exception:
            pass

    async def _fetch_doc_type(self, page, doc_code: str) -> list[dict]:
        records = []
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        log.info(f"Fetching {doc_code} ({doc_label}) from {self.date_from} to {self.date_to}")

        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(self.SEARCH, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(1.5)

                # Fill date range
                await self._safe_fill(page, "#ctl00_bodyPlaceHolder_txtDocumentStartDate", self.date_from)
                await self._safe_fill(page, "#ctl00_bodyPlaceHolder_txtDocumentEndDate",   self.date_to)

                # Fill doc type
                await self._safe_fill(page, "#ctl00_bodyPlaceHolder_txtDocType", doc_code)

                # Submit
                try:
                    await page.click("#ctl00_bodyPlaceHolder_btnNameSearch", timeout=5000)
                except Exception:
                    try:
                        await page.click("input[type='submit']", timeout=5000)
                    except Exception:
                        await page.keyboard.press("Enter")

                await page.wait_for_load_state("networkidle", timeout=20000)
                await asyncio.sleep(1)

                # Pagination loop
                page_num = 1
                while True:
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    rows = self._parse_results_table(soup, doc_code, cat, doc_label)
                    records.extend(rows)
                    log.info(f"  {doc_code} page {page_num}: {len(rows)} rows")

                    # Try next page
                    next_btn = soup.find("a", string=re.compile(r"Next|>", re.I))
                    if not next_btn:
                        break
                    try:
                        href = next_btn.get("href", "")
                        if "__doPostBack" in href:
                            js = href.replace("javascript:", "")
                            await page.evaluate(js)
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        else:
                            await page.click(f"a:has-text('Next')", timeout=5000)
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        page_num += 1
                        await asyncio.sleep(1)
                    except Exception as e:
                        log.debug(f"Pagination ended: {e}")
                        break

                break  # success, exit retry loop

            except PlaywrightTimeout as e:
                log.warning(f"Timeout on {doc_code} attempt {attempt+1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            except Exception as e:
                log.warning(f"Error on {doc_code} attempt {attempt+1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)

        return records

    def _parse_results_table(self, soup: BeautifulSoup, doc_code: str, cat: str, doc_label: str) -> list[dict]:
        records = []
        table = soup.find("table", {"id": re.compile(r"grid|result|record", re.I)})
        if not table:
            tables = soup.find_all("table")
            table = next((t for t in tables if t.find("tr") and len(t.find_all("tr")) > 1), None)
        if not table:
            return records

        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        for row in table.find_all("tr")[1:]:
            try:
                cells = row.find_all("td")
                if not cells or len(cells) < 3:
                    continue

                def cell(idx: int) -> str:
                    if idx < len(cells):
                        return cells[idx].get_text(strip=True)
                    return ""

                def hcell(name_fragment: str) -> str:
                    """Find cell by header name fragment."""
                    for i, h in enumerate(headers):
                        if name_fragment in h:
                            return cell(i)
                    return ""

                # Try named columns first, fall back to positional
                doc_num  = hcell("doc") or hcell("instrument") or cell(0)
                filed    = hcell("date") or hcell("record") or cell(1)
                grantor  = hcell("grantor") or hcell("owner") or hcell("name") or cell(2)
                grantee  = hcell("grantee") or cell(3)
                legal    = hcell("legal") or cell(4)
                amount_s = hcell("amount") or hcell("consider") or cell(5)

                # Parse date
                filed_clean = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
                    try:
                        filed_clean = datetime.strptime(filed, fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass
                if not filed_clean:
                    filed_clean = filed

                # Parse amount
                amount = None
                if amount_s:
                    try:
                        amount = float(re.sub(r"[^\d.]", "", amount_s))
                    except Exception:
                        pass

                # Build clerk URL
                link_tag = row.find("a", href=True)
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else f"{self.BASE}/{href.lstrip('/')}"
                else:
                    clerk_url = f"{self.SEARCH}?doctype={doc_code}&docnum={doc_num}"

                if not doc_num:
                    continue

                records.append({
                    "doc_num":   doc_num.strip(),
                    "doc_type":  doc_code,
                    "filed":     filed_clean,
                    "cat":       cat,
                    "cat_label": CAT_LABELS.get(cat, cat),
                    "owner":     grantor.strip(),
                    "grantee":   grantee.strip(),
                    "amount":    amount,
                    "legal":     legal.strip(),
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
                continue
        return records

    async def run(self) -> list[dict]:
        all_records = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()
            page.set_default_timeout(30000)

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
def build_output(records: list[dict], parcel: ParcelLookup) -> dict:
    enriched = []
    for rec in records:
        try:
            # Enrich with parcel data
            pdata = parcel.lookup(rec.get("owner", ""))
            if pdata:
                rec.update({k: v for k, v in pdata.items() if not rec.get(k)})

            # Score & flags
            score, flags = compute_score_and_flags(rec)
            rec["score"] = score
            rec["flags"] = flags
            enriched.append(rec)
        except Exception as e:
            log.debug(f"Enrich error: {e}")
            enriched.append(rec)

    # Sort by score desc
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    with_address = sum(1 for r in enriched if r.get("prop_address"))

    return {
        "fetched_at":    datetime.now().isoformat(),
        "source":        "Miami-Dade Clerk of Courts Official Records",
        "date_range":    {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d"),
            "to":   datetime.now().strftime("%Y-%m-%d"),
        },
        "total":         len(enriched),
        "with_address":  with_address,
        "records":       enriched,
    }


def save_ghl_csv(records: list[dict], output_path: Path):
    """GoHighLevel-compatible CSV export."""
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]

    def split_name(full: str):
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
                "First Name":            fn,
                "Last Name":             ln,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", "FL"),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         DOC_TYPES.get(r.get("doc_type", ""), (r.get("doc_type",""),))[0],
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", ""),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                "Miami-Dade Clerk Official Records",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved: {output_path}")


async def main():
    log.info("=" * 60)
    log.info("Miami-Dade Motivated Seller Scraper — Starting")
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    parcel.load()

    # 2. Scrape clerk records
    scraper = ClerkScraper(lookback_days=LOOKBACK_DAYS)
    records  = await scraper.run()

    # 3. Build enriched output
    output = build_output(records, parcel)

    # 4. Save JSON to both locations
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        log.info(f"Saved: {path}")

    # 5. GHL CSV
    today = datetime.now().strftime("%Y%m%d")
    save_ghl_csv(output["records"], DATA_DIR / f"ghl_export_{today}.csv")

    log.info(f"Done. Total: {output['total']} | With address: {output['with_address']}")


if __name__ == "__main__":
    asyncio.run(main())
