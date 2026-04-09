"""
Miami-Dade County Motivated Seller Lead Scraper
Uses the internal API that powers the official records portal.
"""

import asyncio
import json
import csv
import io
import re
import time
import logging
import zipfile
import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LOOKBACK_DAYS = 7
MAX_RETRIES   = 3
RETRY_DELAY   = 3

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
# CLERK SCRAPER - Direct HTTP approach
# ─────────────────────────────────────────────
class ClerkScraper:
    # The internal API endpoint the new portal uses
    API_BASE = "https://onlineservices.miamidadeclerk.gov/officialrecords"

    # Headers that mimic a real browser session
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://onlineservices.miamidadeclerk.gov/officialrecords/StandardSearch.aspx",
        "Origin": "https://onlineservices.miamidadeclerk.gov",
    }

    def __init__(self, lookback_days: int = 7):
        self.lookback_days = lookback_days
        self.date_from = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")
        self.session   = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _init_session(self):
        """Load the portal page to get cookies/tokens."""
        try:
            r = self.session.get(
                f"{self.API_BASE}/StandardSearch.aspx",
                timeout=30,
                allow_redirects=True
            )
            log.info(f"Portal init: HTTP {r.status_code}, URL: {r.url}")
            # Also try the main page
            self.session.get(f"{self.API_BASE}/", timeout=15, allow_redirects=True)
        except Exception as e:
            log.warning(f"Session init warning: {e}")

    def _try_api_search(self, doc_code: str) -> list[dict]:
        """Try hitting the internal API endpoints that the JS app calls."""
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        records = []

        # Common internal API patterns for clerk portals
        api_endpoints = [
            f"{self.API_BASE}/api/search",
            f"{self.API_BASE}/api/OfficialRecords/search",
            f"{self.API_BASE}/Search/Results",
            "https://www2.miamidadeclerk.gov/api/OfficialRecords",
        ]

        payload = {
            "docType": doc_code,
            "dateFrom": self.date_from,
            "dateTo": self.date_to,
            "recordedDateFrom": self.date_from,
            "recordedDateTo": self.date_to,
        }

        for endpoint in api_endpoints:
            try:
                r = self.session.post(endpoint, json=payload, timeout=20)
                if r.status_code == 200 and r.text.strip().startswith("{"):
                    data = r.json()
                    log.info(f"API hit! {endpoint}: {str(data)[:200]}")
                    # Parse response
                    items = data.get("records", data.get("results", data.get("data", [])))
                    if isinstance(items, list):
                        for item in items:
                            records.append(self._api_item_to_record(item, doc_code, cat, doc_label))
                        return records
            except Exception:
                pass

        return records

    def _api_item_to_record(self, item: dict, doc_code: str, cat: str, doc_label: str) -> dict:
        def g(*keys):
            for k in keys:
                v = item.get(k, "")
                if v:
                    return str(v).strip()
            return ""

        filed = g("REC_DATE", "recordedDate", "filedDate", "filed")
        try:
            filed = datetime.fromisoformat(filed.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except Exception:
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    filed = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass

        amount = None
        raw_amt = g("CONSIDERATION", "amount", "consideration")
        if raw_amt:
            try:
                amount = float(re.sub(r"[^\d.]", "", raw_amt))
            except Exception:
                pass

        doc_num = g("CFN_SEQ", "CFN_MASTER_ID", "docNumber", "instrumentNumber")
        cfn_year = g("CFN_YEAR", "year")
        if cfn_year and doc_num:
            doc_num = f"{cfn_year}-{doc_num}"

        clerk_url = f"https://www2.miamidadeclerk.gov/ocs/ViewDocument.aspx?cfn={doc_num}"

        return {
            "doc_num":   doc_num,
            "doc_type":  doc_code,
            "filed":     filed,
            "cat":       cat,
            "cat_label": CAT_LABELS.get(cat, cat),
            "owner":     g("FIRST_PARTY", "grantor", "owner"),
            "grantee":   g("SECOND_PARTY", "grantee"),
            "amount":    amount,
            "legal":     g("LEGAL_DESC", "legalDescription", "legal"),
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
        }

    def _scrape_html_search(self, doc_code: str) -> list[dict]:
        """Scrape the old-style HTML search pages as fallback."""
        doc_label, cat = DOC_TYPES.get(doc_code, (doc_code, "other"))
        records = []

        # Try the old ASP.NET form-based portal
        old_urls = [
            "https://www2.miamidadeclerk.gov/ocs/Search.aspx",
            "https://onlineservices.miamidadeclerk.gov/officialrecords/StandardSearch.aspx",
        ]

        for base_url in old_urls:
            try:
                # GET first to grab viewstate
                r = self.session.get(base_url, timeout=20)
                if r.status_code != 200:
                    continue

                soup = BeautifulSoup(r.text, "lxml")
                vs   = soup.find("input", {"id": "__VIEWSTATE"})
                evv  = soup.find("input", {"id": "__EVENTVALIDATION"})
                vsg  = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

                # Find form field names
                inputs = {i.get("name", ""): i.get("value", "") for i in soup.find_all("input") if i.get("name")}
                log.info(f"Form fields at {base_url}: {list(inputs.keys())[:15]}")

                if not vs and not inputs:
                    log.info(f"No form at {base_url}")
                    continue

                # Build POST data
                post_data = dict(inputs)
                if vs:
                    post_data["__VIEWSTATE"] = vs["value"]
                if evv:
                    post_data["__EVENTVALIDATION"] = evv["value"]
                if vsg:
                    post_data["__VIEWSTATEGENERATOR"] = vsg["value"]

                # Try to find the right field names
                for k in list(post_data.keys()):
                    kl = k.lower()
                    if "startdate" in kl or "datefrom" in kl or "begindate" in kl:
                        post_data[k] = self.date_from
                    elif "enddate" in kl or "dateto" in kl or "throughdate" in kl:
                        post_data[k] = self.date_to
                    elif "doctype" in kl or "instrumenttype" in kl:
                        post_data[k] = doc_code

                # Submit
                self.session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})
                r2 = self.session.post(base_url, data=post_data, timeout=30)
                soup2 = BeautifulSoup(r2.text, "lxml")

                rows = self._parse_results_table(soup2, doc_code, cat, doc_label, base_url)
                if rows:
                    log.info(f"Got {len(rows)} records from {base_url}")
                    records.extend(rows)
                    return records

            except Exception as e:
                log.warning(f"HTML scrape error at {base_url}: {e}")

        return records

    def _parse_results_table(self, soup, doc_code, cat, doc_label, base_url):
        records = []
        tables = soup.find_all("table")
        table = None
        for t in tables:
            rows = t.find_all("tr")
            if len(rows) > 1:
                table = t
                break
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

                def cell(i):
                    return cells[i].get_text(strip=True) if i < len(cells) else ""

                def hcell(frag):
                    for i, h in enumerate(headers):
                        if frag in h:
                            return cell(i)
                    return ""

                doc_num  = hcell("doc") or hcell("instrument") or hcell("cfn") or cell(0)
                filed    = hcell("date") or hcell("record") or cell(1)
                grantor  = hcell("grantor") or hcell("owner") or hcell("name") or cell(2)
                grantee  = hcell("grantee") or cell(3)
                legal    = hcell("legal") or cell(4)
                amount_s = hcell("amount") or hcell("consider") or cell(5)

                filed_clean = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
                    try:
                        filed_clean = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass
                if not filed_clean:
                    filed_clean = filed

                amount = None
                if amount_s:
                    try:
                        amount = float(re.sub(r"[^\d.]", "", amount_s))
                    except Exception:
                        pass

                link_tag = row.find("a", href=True)
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else f"https://www2.miamidadeclerk.gov{href}"
                else:
                    clerk_url = f"https://www2.miamidadeclerk.gov/ocs/Search.aspx"

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
        return records

    def run(self) -> list[dict]:
        all_records = []
        log.info("Initializing session with Clerk portal...")
        self._init_session()

        for doc_code in DOC_TYPES:
            doc_label, cat = DOC_TYPES[doc_code]
            log.info(f"Fetching {doc_code} ({doc_label})...")
            try:
                # Try API first
                recs = self._try_api_search(doc_code)
                if not recs:
                    # Fall back to HTML scraping
                    recs = self._scrape_html_search(doc_code)
                log.info(f"  {doc_code}: {len(recs)} records")
                all_records.extend(recs)
            except Exception as e:
                log.error(f"Failed {doc_code}: {e}")

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
        "date_range":   {
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
                "Document Type":         DOC_TYPES.get(r.get("doc_type",""), (r.get("doc_type",""),))[0],
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", ""),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                "Miami-Dade Clerk Official Records",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved: {output_path}")


def main():
    log.info("=" * 60)
    log.info("Miami-Dade Motivated Seller Scraper — Starting")
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info("=" * 60)

    scraper = ClerkScraper(lookback_days=LOOKBACK_DAYS)
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
