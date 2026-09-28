"""
Miami-Dade County Motivated Seller Lead Scraper  (v2)
=====================================================

Pipeline
--------
1. CLERK  – A real headless Chromium (Playwright) opens the public Official
   Records search page, so it collects every cookie the site hands out
   (load-balancer, ASP.NET session, …) exactly like a person's browser.
   All API calls are then made *from inside that page* with fetch(), so they
   are same-origin requests with a genuine browser fingerprint.
   If CLERK_EMAIL / CLERK_PASSWORD are set it logs in automatically each run;
   CLERK_SESSION (.PremierIDDade cookie) is still honoured as a fallback.
   -> No more hand-copied 30-minute cookies.

2. DOC TYPES – The portal's own document-type list is read from the page and
   mapped to the lead codes below (LP, NOFC, TAXDEED, JUD …). Known-good
   portal names are used as a fallback if discovery finds nothing.

3. QUERIES – One query per document type *per day* (instead of one per week),
   so busy types never hit the portal's ~500-row result cap.

4. GROUPING – The portal returns one row per party pair; rows are merged by
   clerk file number (CFN) so each document is ONE lead with all its parties.

5. PARCELS – The Property Appraiser's full parcel roll (943k parcels, owner,
   site + mailing address, legal description) is downloaded for free from the
   county's Open Data layer on Esri's cloud, stored in a local SQLite file and
   refreshed weekly. Matching cascade per document:
       folio  ->  street address  ->  owner name (FIRST LAST / LAST FIRST /
       LAST, FIRST / compound surnames / initials)  ->  legal description
       (subdivision / condo name + lot / block / unit / plat book)
   The party that matches a parcel becomes the "owner" – so the bank or HOA
   that filed a lis pendens is no longer shown as the owner.
   If the parcel file is unavailable, a live per-record lookup against the
   county's PaParcel GIS layer is used instead.

6. SCORE / OUTPUT – Seller score 0-100, dashboard/records.json,
   data/records.json and a GoHighLevel CSV.
   A run that finds 0 records NEVER overwrites the dashboard; it exits with an
   error so GitHub Actions marks the run red and emails you.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.parse
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

CLERK_BASE = os.environ.get(
    "CLERK_BASE_URL", "https://onlineservices.miamidadeclerk.gov/officialrecords"
).rstrip("/")
# The portal is now a single-page app. The legacy StandardSearch.aspx URL is routed by the
# client to ".../officialrecords/undefined" (blank page, no login link, API 404s), so the
# SPA root is tried first and the legacy/hash routes only as fallbacks.
CLERK_SEARCH_PAGES = [
    f"{CLERK_BASE}/",
    f"{CLERK_BASE}/#/standardsearch",
    f"{CLERK_BASE}/standardsearch",
    f"{CLERK_BASE}/StandardSearch.aspx",
]
CLERK_SEARCH_PAGE = CLERK_SEARCH_PAGES[0]
CLERK_API = f"{CLERK_BASE}/api"

CLERK_EMAIL = os.environ.get("CLERK_EMAIL", "")
CLERK_PASSWORD = os.environ.get("CLERK_PASSWORD", "")
CLERK_SESSION = os.environ.get("CLERK_SESSION", "")  # optional .PremierIDDade cookie

# Free full parcel roll (Miami-Dade Open Data "Property Point View", weekly refresh)
PARCEL_BULK_URL = os.environ.get(
    "PARCEL_BULK_URL",
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/"
    "PaGISView_gdb/FeatureServer/0/query",
)
# Live per-record fallback (county GIS server, verified working)
PARCEL_LIVE_URL = os.environ.get(
    "PARCEL_LIVE_URL",
    "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/26/query",
)
PARCEL_PAGE_SIZE = int(os.environ.get("PARCEL_PAGE_SIZE", "2000"))
PARCEL_MAX_AGE_DAYS = int(os.environ.get("PARCEL_MAX_AGE_DAYS", "7"))
PARCEL_WORKERS = int(os.environ.get("PARCEL_WORKERS", "4"))

HEADLESS = os.environ.get("HEADLESS", "1") != "0"
REQUEST_PAUSE = float(os.environ.get("REQUEST_PAUSE", "0.4"))  # seconds between clerk calls
RESULT_CAP_WARN = 490  # portal appears to cap results near 500

ROOT_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
DATA_DIR = ROOT_DIR / "data"
DEBUG_DIR = ROOT_DIR / "debug"
PARCEL_DB = DATA_DIR / "pa_parcels.sqlite"
for _d in (DASHBOARD_DIR, DATA_DIR, DEBUG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("miami")


class AuthError(RuntimeError):
    """Clerk portal refused every search – session/login problem."""


# ─────────────────────────────────────────────────────────────────────────────
# LEAD TYPES
# Order matters: a portal document type is assigned to the FIRST code whose
# pattern matches, so specific types come before generic ones.
# ─────────────────────────────────────────────────────────────────────────────
LEAD_TYPES: "OrderedDict[str, dict]" = OrderedDict([
    ("RELLP",    dict(label="Release of Lis Pendens", cat="release",
                      pat=r"(RELEASE|CANCEL|DISCHARGE|WITHDRAW|DISMISS).*LIS PENDENS",
                      known=["CANCELLATION OF LIS PENDENS - CLP"])),
    ("NOFC",     dict(label="Notice of Foreclosure", cat="pre-foreclosure",
                      pat=r"FORECLOS", known=[])),
    ("TAXDEED",  dict(label="Tax Deed", cat="tax-distressed",
                      pat=r"TAX DEED", known=[])),
    ("LP",       dict(label="Lis Pendens", cat="pre-foreclosure",
                      pat=r"^LIS PENDENS\b", known=["LIS PENDENS - LIS"])),
    ("CCJ",      dict(label="Certified Judgment", cat="judgment",
                      pat=r"CERTIFIED.*JUDG|JUDG.*CERTIFIED", known=[])),
    ("DRJUD",    dict(label="Domestic Judgment", cat="judgment",
                      pat=r"(DOMESTIC|FAMILY|SUPPORT|DISSOLUTION).*JUDG|JUDG.*(DOMESTIC|FAMILY|SUPPORT)",
                      known=[])),
    ("JUD",      dict(label="Judgment", cat="judgment",
                      pat=r"^(FINAL )?JUDG(E)?MENT\b", known=["JUDGEMENT - JUD"])),
    ("LNIRS",    dict(label="IRS Lien", cat="tax-lien",
                      pat=r"\bIRS\b|INTERNAL REVENUE", known=[])),
    ("LNFED",    dict(label="Federal Lien", cat="tax-lien",
                      pat=r"FEDERAL.*LIEN", known=["FEDERAL TAX LIEN - FTL"])),
    ("LNCORPTX", dict(label="Corp Tax Lien", cat="tax-lien",
                      pat=r"NOTICE OF TAX LIEN|CORP.*TAX|STATE TAX LIEN|DEPARTMENT OF REVENUE|TAX WARRANT",
                      known=["NOTICE OF TAX LIEN - NTL"])),
    ("LNMECH",   dict(label="Mechanic Lien", cat="lien",
                      pat=r"MECHANIC|CLAIM OF LIEN|CONSTRUCTION LIEN", known=[])),
    ("LNHOA",    dict(label="HOA Lien", cat="lien",
                      pat=r"ASSOCIATION.*LIEN|LIEN.*ASSOCIATION|\bHOA\b|CONDOMINIUM.*LIEN", known=[])),
    ("MEDLN",    dict(label="Medicaid Lien", cat="lien",
                      pat=r"MEDICAID|\bAHCA\b|HOSPITAL LIEN", known=[])),
    ("LN",       dict(label="Lien", cat="lien",
                      pat=r"^LIEN\b", known=["LIEN - LIE"])),
    ("PRO",      dict(label="Probate Document", cat="probate",
                      pat=r"PROBATE|LETTERS OF ADMIN|SUMMARY ADMIN|DEATH CERTIF",
                      known=["PROBATE & ADMINISTRATION - PAD",
                             "PROBATE ORDER OF DISTRIBUTION - PRO"])),
    ("NOC",      dict(label="Notice of Commencement", cat="notice",
                      pat=r"NOTICE OF COMMENCEMENT", known=["NOTICE OF COMMENCEMENT - NCO"])),
])

# Documents that undo / satisfy something are never leads (except RELLP)
NEGATIVE_TYPE_RE = re.compile(
    r"^(PARTIAL )?(RELEASE|SATISFACTION|CANCEL|DISCHARGE|WITHDRAW|TERMINATION|"
    r"ASSIGNMENT|MODIFICATION|AMEND|SUBORDINATION|CERTIFICATE OF (DISCHARGE|NON)|"
    r"NOTICE OF CONTEST|RE-?RECORD)", re.I)

# Portal type strings look like "LIS PENDENS - LIS"
PORTAL_TYPE_RE = re.compile(r"^[A-Z0-9][A-Z0-9 &/'().,#-]{2,80} - [A-Z0-9]{2,6}$")

CAT_LABELS = {
    "pre-foreclosure": "Pre-Foreclosure", "tax-distressed": "Tax Distressed",
    "judgment": "Judgment", "tax-lien": "Tax / Fed Lien", "lien": "Lien",
    "probate": "Probate / Estate", "notice": "Notice", "release": "Release",
}

# ─────────────────────────────────────────────────────────────────────────────
# PARTY CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────
LENDER_RE = re.compile(
    r"\bBANK\b|BANCORP|MORTGAGE|LENDING|\bLOANS?\b|FINANCIAL|FINANCE\b|CREDIT UNION|"
    r"SERVICING|SERVICER|FUNDING|SAVINGS|\bFSB\b|FEDERAL NATIONAL|FANNIE MAE|FREDDIE MAC|"
    r"NATIONAL ASSOCIATION|\bN ?A\b|TRUST COMPANY|AS TRUSTEE|TRUSTEE FOR|CERTIFICATEHOLDERS|"
    r"SECURITIZATION|NEWREZ|NATIONSTAR|MR COOPER|\bPHH\b|SHELLPOINT|PENNYMAC|WELLS FARGO|"
    r"JPMORGAN|CHASE BANK|CHASE HOME|CITIBANK|U ?S BANK|DEUTSCHE|HSBC|MIDFIRST|"
    r"\bCAPITAL\b.*\b(LP|LLC|TRUST|FUND)\b|WILMINGTON|BANK OF NEW YORK|MELLON|"
    r"HOME POINT|LOANDEPOT|RUSHMORE|COMMUNITY LOAN")
ASSOC_RE = re.compile(
    r"ASSOCIATION|\bASSN\b|\bHOA\b|HOMEOWNERS|CONDOMINIUM|\bCONDO\b|MASTER ASSOC|"
    r"COMMUNITY (ASSOC|DEVELOPMENT)|PROPERTY OWNERS|\bPOA\b|\bCOA\b")
GOV_RE = re.compile(
    r"UNITED STATES|\bUSA\b|INTERNAL REVENUE|\bIRS\b|DEPARTMENT OF|STATE OF FLORIDA|"
    r"MIAMI[- ]DADE|\bCOUNTY\b|CITY OF|TOWN OF|VILLAGE OF|CLERK|TAX COLLECTOR|"
    r"SECRETARY OF|\bHUD\b|HOUSING AND URBAN|AGENCY FOR HEALTH|\bAHCA\b|MEDICAID|"
    r"SCHOOL BOARD|WATER AND SEWER")
NOISE_RE = re.compile(
    r"^(UNKNOWN|ALL UNKNOWN|ANY AND ALL|JOHN DOE|JANE DOE|TENANT|UNKNOWN TENANT|"
    r"UNKNOWN SPOUSE|HEIRS|THE UNKNOWN|OCCUPANT)|\bUNKNOWN (TENANT|SPOUSE|HEIRS|PARTIES)\b")
CORP_RE = re.compile(
    r"\b(LLC|L L C|INC|CORP|CORPORATION|CO|COMPANY|LTD|LP|LLP|PLLC|PA|TRUST|TR|"
    r"TRUSTEE|HOLDINGS|PROPERTIES|INVESTMENTS?|GROUP|PARTNERS|VENTURES|ENTERPRISES|"
    r"REALTY|DEVELOPMENT|FUND|FOUNDATION|CHURCH|MINISTR\w*)\b")
ESTATE_RE = re.compile(r"\bESTATE OF\b|\bEST OF\b|\bDECEASED\b|\bDEC'D\b|\bDECD\b")


def party_kind(name: str) -> str:
    n = f" {name.upper()} "
    if NOISE_RE.search(name.upper()):
        return "noise"
    if GOV_RE.search(n):
        return "gov"
    if LENDER_RE.search(n):
        return "lender"
    if ASSOC_RE.search(n):
        return "assoc"
    return "person_or_owner"


BUSINESS_RE = re.compile(
    r"\b(LLC|L L C|INC|CORP|CORPORATION|CO|COMPANY|LTD|LP|LLP|PLLC|PA|TRUST|"
    r"HOLDINGS|PROPERTIES|INVESTMENTS?|GROUP|PARTNERS|VENTURES|ENTERPRISES|REALTY|"
    r"DEVELOPMENT|FUND|FOUNDATION|CHURCH|MINISTR\w*)\b")


def is_corp(name: str) -> bool:
    """Owner is a company / trust (used for the 'LLC / corp owner' flag)."""
    return bool(CORP_RE.search(name.upper().replace(".", "").replace(",", " ")))


def is_business(name: str) -> bool:
    """Name should be matched as a whole business name (trustees are people)."""
    return bool(BUSINESS_RE.search(name.upper().replace(".", "").replace(",", " ")))


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
_NAME_DROP = {
    "JR", "SR", "II", "III", "IV", "TR", "TRS", "TRUSTEE", "TRUSTEES", "ET", "AL",
    "ETAL", "ETUX", "ETVIR", "UX", "VIR", "EST", "ESTATE", "OF", "THE", "HE", "WE",
    "LE", "REM", "AND", "DECEASED", "DECD", "AKA", "FKA", "NKA", "INDIVIDUALLY",
}
_CORP_CANON = [
    (r"\bL\s*L\s*C\b", "LLC"), (r"\bINCORPORATED\b", "INC"),
    (r"\bCORPORATION\b", "CORP"), (r"\bLIMITED\b", "LTD"), (r"\bCOMPANY\b", "CO"),
]


def norm_text(s: str) -> str:
    s = (s or "").upper().replace("&", " ")
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def norm_company(name: str) -> str:
    s = (name or "").upper().replace(".", "").replace(",", " ")
    for pat, rep in _CORP_CANON:
        s = re.sub(pat, rep, s)
    return norm_text(s)


def person_tokens(name: str) -> list:
    return [t for t in norm_text(name).split() if t not in _NAME_DROP]


def parcel_name_keys(owner: str) -> set:
    """Index keys for a Property Appraiser owner string (usually FIRST MIDDLE LAST)."""
    if not owner:
        return set()
    if is_business(owner):
        k = norm_company(owner)
        return {k} if len(k) >= 4 else set()
    toks = person_tokens(owner)
    if len(toks) < 2:
        return set()
    keys = {" ".join(toks)}
    no_init = [t for t in toks if len(t) > 1]
    if len(no_init) >= 2:
        keys.add(" ".join(no_init))
    return keys


def clerk_name_variants(name: str) -> list:
    """
    Candidate keys for a clerk party name. Clerk names are usually
    "LAST FIRST MIDDLE"; the parcel roll uses "FIRST MIDDLE LAST".
    Every rotation is tried, which covers FIRST LAST, LAST FIRST,
    "LAST, FIRST" and compound surnames ("GARCIA LOPEZ MARIA").
    Returns [(key, first_part, last_part), ...] in preference order.
    """
    if not name:
        return []
    if is_business(name):
        k = norm_company(name)
        return [(k, k, "")] if len(k) >= 4 else []
    raw = name.upper()
    out, seen = [], set()

    def add(tokens, first, last):
        key = " ".join(tokens)
        if len(tokens) >= 2 and key not in seen:
            seen.add(key)
            out.append((key, " ".join(first), " ".join(last)))

    if "," in raw:  # "LAST, FIRST MIDDLE"
        last_s, first_s = raw.split(",", 1)
        lt, ft = person_tokens(last_s), person_tokens(first_s)
        add(ft + lt, ft, lt)
    toks = person_tokens(raw)
    if len(toks) < 2:
        return out
    # rotations: tokens[k:] is the first-name part, tokens[:k] the surname part
    for k in range(1, len(toks)):
        first, last = toks[k:], toks[:k]
        add(first + last, first, last)
    add(toks, toks[:-1], toks[-1:])  # already FIRST LAST
    # same again without middle initials
    base = list(out)
    for key, first, last in base:
        f2 = [t for t in first.split() if len(t) > 1]
        l2 = [t for t in last.split() if len(t) > 1]
        if f2 and l2:
            add(f2 + l2, f2, l2)
    return out


_STREET_SUBS = [
    (r"\bSTREET\b", "ST"), (r"\bAVENUE\b", "AVE"), (r"\bROAD\b", "RD"),
    (r"\bDRIVE\b", "DR"), (r"\bCOURT\b", "CT"), (r"\bPLACE\b", "PL"),
    (r"\bTERRACE\b", "TER"), (r"\bBOULEVARD\b", "BLVD"), (r"\bLANE\b", "LN"),
    (r"\bHIGHWAY\b", "HWY"), (r"\bCIRCLE\b", "CIR"), (r"\bPARKWAY\b", "PKWY"),
    (r"\bNORTHWEST\b", "NW"), (r"\bNORTHEAST\b", "NE"),
    (r"\bSOUTHWEST\b", "SW"), (r"\bSOUTHEAST\b", "SE"),
    (r"\bN E\b", "NE"), (r"\bN W\b", "NW"), (r"\bS E\b", "SE"), (r"\bS W\b", "SW"),
    (r"\bNORTH\b", "N"), (r"\bSOUTH\b", "S"), (r"\bEAST\b", "E"), (r"\bWEST\b", "W"),
    (r"\b(\d+)(ST|ND|RD|TH)\b", r"\1"),
]


def norm_street(addr: str) -> tuple:
    """Return (street, unit) normalised like the parcel roll: '2224 NE 136 ST'."""
    s = (addr or "").upper()
    unit = ""
    m = re.search(r"(?:#|\bUNIT\b|\bAPT\b|\bSTE\b|\bSUITE\b)\s*([A-Z0-9-]+)", s)
    if m:
        unit = m.group(1)
        s = s[:m.start()]
    s = s.split(",")[0]
    s = norm_text(s)
    for pat, rep in _STREET_SUBS:
        s = re.sub(pat, rep, s)
    return re.sub(r"\s+", " ", s).strip(), unit


def to_folio(v) -> str:
    if v is None:
        return ""
    digits = re.sub(r"\D", "", str(v))
    if not digits or int(digits) == 0:
        return ""
    return digits.zfill(13)


def zip5(z) -> str:
    return (str(z or "").strip().split("-")[0])[:5]


# ─────────────────────────────────────────────────────────────────────────────
# LEGAL DESCRIPTION PARSING
# ─────────────────────────────────────────────────────────────────────────────
_LEGAL_STOP = {
    "LOT", "LOTS", "BLK", "BLOCK", "UNIT", "PB", "PG", "OR", "SEC", "TWP", "RGE",
    "LESS", "AND", "THE", "OF", "IN", "TO", "SEE", "DOC", "FOR", "FULL", "LEGAL",
    "DESC", "DESCRIPTION", "SUB", "ADD", "REV", "AMD", "PLAT", "TRACT", "TR",
    "FT", "N", "S", "E", "W", "NE", "NW", "SE", "SW", "ST", "AVE", "BEG", "COR",
    "UNDIV", "INT", "COMMON", "ELEMENTS", "OFF", "REC", "COC", "SIZE", "X", "AC",
    "PT", "PORT", "HALF", "THRU", "INCL", "ALL", "DESC",
}


def legal_parts(s: str) -> dict:
    s = (s or "").upper()
    lots = set()
    for m in re.finditer(r"\bLOTS?\s+((?:\d+[A-Z]?\s*(?:&|,|AND|THRU|-)?\s*)+)", s):
        nums = re.findall(r"\d+[A-Z]?", m.group(1))
        if "THRU" in m.group(1) and len(nums) == 2 and nums[0].isdigit() and nums[1].isdigit():
            a, b = int(nums[0]), int(nums[1])
            if 0 < b - a < 60:
                nums = [str(i) for i in range(a, b + 1)]
        lots.update(n.lstrip("0") or "0" for n in nums)
    blk = re.search(r"\b(?:BLK|BLOCK)\s+([A-Z0-9]+)", s)
    unit = re.search(r"\bUNIT\s+(?:NO\s+)?([A-Z0-9-]+)", s)
    pb = re.search(r"\bPB\s*(\d+)\s*-\s*(\d+)", s)
    return {
        "lots": lots,
        "blk": (blk.group(1).lstrip("0") or "0") if blk else "",
        "unit": unit.group(1).lstrip("0") if unit else "",
        "pb": f"{int(pb.group(1))}-{int(pb.group(2))}" if pb else "",
    }


def parcel_sub_name(legal: str) -> str:
    """Subdivision / condo name from a Property Appraiser legal description."""
    s = norm_text(legal)
    s = re.sub(r"^(\d+ ){2,3}", "", s)  # section township range prefix
    m = re.match(r"(.+?)\s+(?:PB|UNIT|LOT|LOTS|BLK|BLOCK|REV|AMD|SUB|ADDN|ADD|SEC|TR|PLAT)\b", s)
    name = (m.group(1) if m else "").strip()
    toks = [t for t in name.split() if not t.isdigit()]
    if not (1 <= len(toks) <= 7):
        return ""
    name = " ".join(toks)
    return name if len(name) >= 5 else ""


def legal_ngrams(legal: str) -> list:
    """Candidate subdivision / condo names inside a clerk legal description.
    Single words only count when long (8+ letters) to avoid generic hits."""
    toks = [t for t in norm_text(legal).split()
            if not any(c.isdigit() for c in t) and t not in _LEGAL_STOP]
    grams = set()
    for n in range(1, 6):
        for i in range(len(toks) - n + 1):
            g = " ".join(toks[i:i + n])
            if (n == 1 and len(g) >= 8) or (n > 1 and len(g) >= 6):
                grams.add(g)
    return list(grams)


def legal_score(a: dict, b: dict) -> int:
    score = 0
    if a["pb"] and a["pb"] == b["pb"]:
        score += 3
    if a["blk"] and a["blk"] == b["blk"]:
        score += 2
    if a["lots"] and b["lots"] and a["lots"] & b["lots"]:
        score += 3
    if a["unit"] and a["unit"] == b["unit"]:
        score += 4
    # contradictions
    if a["blk"] and b["blk"] and a["blk"] != b["blk"]:
        score -= 3
    if a["unit"] and b["unit"] and a["unit"] != b["unit"]:
        score -= 4
    if a["lots"] and b["lots"] and not (a["lots"] & b["lots"]):
        score -= 2
    return score


def legal_is_useful(p: dict) -> bool:
    return bool(p["lots"] or p["blk"] or p["unit"] or p["pb"])


# ─────────────────────────────────────────────────────────────────────────────
# PARCEL DATABASE  (free bulk parcel roll -> SQLite)
# ─────────────────────────────────────────────────────────────────────────────
PARCEL_FIELDS = [
    "FOLIO", "TRUE_SITE_ADDR", "TRUE_SITE_UNIT", "TRUE_SITE_CITY", "TRUE_SITE_ZIP_CODE",
    "TRUE_MAILING_ADDR1", "TRUE_MAILING_ADDR2", "TRUE_MAILING_CITY",
    "TRUE_MAILING_STATE", "TRUE_MAILING_ZIP_CODE", "TRUE_MAILING_COUNTRY",
    "TRUE_OWNER1", "TRUE_OWNER2", "TRUE_OWNER3", "LEGAL",
]


def http_get_json(session: requests.Session, url: str, params: dict, tries: int = 3,
                  timeout: int = 60) -> dict:
    last = None
    for attempt in range(1, tries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"ArcGIS error: {data['error']}")
            return data
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * attempt)
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


class ParcelDB:
    """SQLite copy of the Property Appraiser parcel roll with lookup indexes."""

    def __init__(self, path: Path = PARCEL_DB):
        self.path = path
        self.conn: sqlite3.Connection | None = None

    # ── build / refresh ─────────────────────────────────────────────────────
    def age_days(self) -> float | None:
        if not self.path.exists():
            return None
        try:
            c = sqlite3.connect(self.path)
            row = c.execute("SELECT v FROM meta WHERE k='built_at'").fetchone()
            cnt = c.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
            c.close()
            if not row or cnt < 1000:
                return None
            return (datetime.utcnow() - datetime.fromisoformat(row[0])).total_seconds() / 86400
        except Exception:  # noqa: BLE001
            return None

    def ensure(self) -> bool:
        age = self.age_days()
        if age is not None and age <= PARCEL_MAX_AGE_DAYS:
            log.info(f"Parcel DB is {age:.1f} days old – reusing {self.path.name}")
            return self.open()
        log.info("Parcel DB missing or stale – downloading the parcel roll …")
        try:
            self.build()
        except Exception as e:  # noqa: BLE001
            log.error(f"Parcel roll download failed: {e}")
            if age is not None:
                log.warning("Using the previous (stale) parcel DB instead.")
                return self.open()
            return False
        return self.open()

    def build(self):
        sess = requests.Session()
        sess.headers["User-Agent"] = USER_AGENT
        total = http_get_json(sess, PARCEL_BULK_URL,
                              {"where": "1=1", "returnCountOnly": "true", "f": "json"})["count"]
        pages = list(range(0, total, PARCEL_PAGE_SIZE))
        log.info(f"Parcel roll: {total:,} parcels in {len(pages)} pages")

        tmp = self.path.with_suffix(".building")
        if tmp.exists():
            tmp.unlink()
        conn = sqlite3.connect(tmp)
        self._create_schema(conn)

        def fetch_page(offset):
            data = http_get_json(sess, PARCEL_BULK_URL, {
                "where": "1=1", "outFields": ",".join(PARCEL_FIELDS),
                "returnGeometry": "false", "orderByFields": "OBJECTID ASC",
                "resultOffset": offset, "resultRecordCount": PARCEL_PAGE_SIZE,
                "f": "json",
            }, tries=4, timeout=120)
            return [f.get("attributes", {}) for f in data.get("features", [])]

        failed, done, rows_in = 0, 0, 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=PARCEL_WORKERS) as ex:
            futs = {ex.submit(fetch_page, off): off for off in pages}
            for fut in as_completed(futs):
                try:
                    rows = fut.result()
                    self._insert(conn, rows)
                    rows_in += len(rows)
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    log.warning(f"Parcel page at offset {futs[fut]} failed: {e}")
                done += 1
                if done % 50 == 0 or done == len(pages):
                    log.info(f"  parcel pages {done}/{len(pages)} ({rows_in:,} rows, "
                             f"{time.time() - t0:.0f}s)")
        if failed > max(3, len(pages) * 0.02):
            conn.close()
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"{failed} of {len(pages)} parcel pages failed")

        log.info("Indexing parcel DB …")
        conn.executescript("""
            CREATE INDEX IF NOT EXISTS ix_names ON names(k);
            CREATE INDEX IF NOT EXISTS ix_addr ON parcels(site_addr);
            CREATE INDEX IF NOT EXISTS ix_sub ON subs(k);
            CREATE INDEX IF NOT EXISTS ix_pb ON pbs(k);
        """)
        conn.execute("INSERT OR REPLACE INTO meta VALUES('built_at', ?)",
                     (datetime.utcnow().isoformat(),))
        conn.execute("INSERT OR REPLACE INTO meta VALUES('count', ?)", (str(rows_in),))
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
        tmp.replace(self.path)
        log.info(f"Parcel DB ready: {rows_in:,} parcels "
                 f"({self.path.stat().st_size / 1e6:.0f} MB, {time.time() - t0:.0f}s)")

    @staticmethod
    def _create_schema(conn):
        conn.executescript("""
            PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            CREATE TABLE parcels(folio TEXT PRIMARY KEY, site_addr TEXT, site_unit TEXT,
                site_city TEXT, site_zip TEXT, mail1 TEXT, mail2 TEXT, mail_city TEXT,
                mail_state TEXT, mail_zip TEXT, mail_country TEXT,
                owner1 TEXT, owner2 TEXT, owner3 TEXT, legal TEXT);
            CREATE TABLE names(k TEXT, folio TEXT);
            CREATE TABLE subs(k TEXT, folio TEXT);
            CREATE TABLE pbs(k TEXT, folio TEXT);
            CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
        """)

    @staticmethod
    def _insert(conn, rows):
        p, n, s, b = [], [], [], []
        for a in rows:
            folio = to_folio(a.get("FOLIO"))
            if not folio:
                continue
            site, _ = norm_street(a.get("TRUE_SITE_ADDR") or "")
            legal = (a.get("LEGAL") or "").strip()
            p.append((folio, site, (a.get("TRUE_SITE_UNIT") or "").strip(),
                      (a.get("TRUE_SITE_CITY") or "").strip(), zip5(a.get("TRUE_SITE_ZIP_CODE")),
                      (a.get("TRUE_MAILING_ADDR1") or "").strip(),
                      (a.get("TRUE_MAILING_ADDR2") or "").strip(),
                      (a.get("TRUE_MAILING_CITY") or "").strip(),
                      (a.get("TRUE_MAILING_STATE") or "").strip(),
                      zip5(a.get("TRUE_MAILING_ZIP_CODE")),
                      (a.get("TRUE_MAILING_COUNTRY") or "").strip(),
                      (a.get("TRUE_OWNER1") or "").strip(), (a.get("TRUE_OWNER2") or "").strip(),
                      (a.get("TRUE_OWNER3") or "").strip(), legal))
            keys = set()
            for o in ("TRUE_OWNER1", "TRUE_OWNER2", "TRUE_OWNER3"):
                keys |= parcel_name_keys(a.get(o) or "")
            n.extend((k, folio) for k in keys)
            sub = parcel_sub_name(legal)
            if sub:
                s.append((sub, folio))
            pb = legal_parts(legal)["pb"]
            if pb:
                b.append((pb, folio))
        conn.executemany("INSERT OR REPLACE INTO parcels VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", p)
        conn.executemany("INSERT INTO names VALUES(?,?)", n)
        conn.executemany("INSERT INTO subs VALUES(?,?)", s)
        conn.executemany("INSERT INTO pbs VALUES(?,?)", b)
        conn.commit()

    # ── lookups ─────────────────────────────────────────────────────────────
    def open(self) -> bool:
        try:
            self.conn = sqlite3.connect(self.path)
            self.conn.row_factory = sqlite3.Row
            n = self.conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
            log.info(f"Parcel DB opened: {n:,} parcels")
            return n > 0
        except Exception as e:  # noqa: BLE001
            log.error(f"Could not open parcel DB: {e}")
            self.conn = None
            return False

    def _rows(self, sql, args) -> list:
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def by_folio(self, folio: str) -> dict | None:
        rows = self._rows("SELECT * FROM parcels WHERE folio=?", (folio,))
        return rows[0] if rows else None

    def by_address(self, street: str) -> list:
        return self._rows("SELECT * FROM parcels WHERE site_addr=? LIMIT 400", (street,))

    def by_name_key(self, key: str, limit: int = 60) -> list:
        return self._rows(
            "SELECT p.* FROM names n JOIN parcels p ON p.folio=n.folio WHERE n.k=? LIMIT ?",
            (key, limit))

    def by_legal(self, legal: str, limit: int = 3000) -> list:
        grams = legal_ngrams(legal)
        pb = legal_parts(legal)["pb"]
        out = []
        if grams:
            q = ",".join("?" * len(grams))
            rows = self._rows(
                f"SELECT s.k AS sub_k, p.* FROM subs s JOIN parcels p ON p.folio=s.folio "
                f"WHERE s.k IN ({q}) LIMIT ?", (*grams, limit))
            if rows:  # keep only the most specific (longest) subdivision name found
                best = max(len(r["sub_k"]) for r in rows)
                out = [r for r in rows if len(r["sub_k"]) == best]
            if len(rows) >= limit:
                out = []  # subdivision too large to disambiguate safely
        if not out and pb:
            out = self._rows(
                "SELECT p.* FROM pbs b JOIN parcels p ON p.folio=b.folio WHERE b.k=? LIMIT ?",
                (pb, limit))
            if len(out) >= limit:
                out = []
        return out


class LiveParcelLookup:
    """Fallback when the bulk parcel DB is unavailable: live county GIS queries."""

    FIELDS = ("FOLIO,TRUE_SITE_ADDR,TRUE_SITE_CITY,TRUE_SITE_ZIP_CODE,TRUE_MAILING_ADDR1,"
              "TRUE_MAILING_CITY,TRUE_MAILING_STATE,TRUE_MAILING_ZIP_CODE,TRUE_OWNER1,TRUE_OWNER2")

    def __init__(self):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = USER_AGENT
        self.cache: dict = {}

    def _q(self, where: str, n: int = 25) -> list:
        if where in self.cache:
            return self.cache[where]
        rows = []
        try:
            data = http_get_json(self.s, PARCEL_LIVE_URL, {
                "where": where, "outFields": self.FIELDS, "returnGeometry": "false",
                "resultRecordCount": n, "f": "json"}, tries=3, timeout=30)
            for f in data.get("features", []):
                a = f.get("attributes", {})
                rows.append({
                    "folio": to_folio(a.get("FOLIO")),
                    "site_addr": (a.get("TRUE_SITE_ADDR") or "").strip(), "site_unit": "",
                    "site_city": (a.get("TRUE_SITE_CITY") or "").strip(),
                    "site_zip": zip5(a.get("TRUE_SITE_ZIP_CODE")),
                    "mail1": (a.get("TRUE_MAILING_ADDR1") or "").strip(), "mail2": "",
                    "mail_city": (a.get("TRUE_MAILING_CITY") or "").strip(),
                    "mail_state": (a.get("TRUE_MAILING_STATE") or "").strip(),
                    "mail_zip": zip5(a.get("TRUE_MAILING_ZIP_CODE")), "mail_country": "",
                    "owner1": (a.get("TRUE_OWNER1") or "").strip(),
                    "owner2": (a.get("TRUE_OWNER2") or "").strip(), "owner3": "", "legal": "",
                })
        except Exception as e:  # noqa: BLE001
            log.debug(f"live parcel query failed: {e}")
        time.sleep(0.2)
        self.cache[where] = rows
        return rows

    @staticmethod
    def _sq(s: str) -> str:
        return s.replace("'", "''")

    def by_folio(self, folio):
        r = self._q(f"FOLIO='{folio}'", 1)
        return r[0] if r else None

    def by_address(self, street):
        return self._q(f"TRUE_SITE_ADDR='{self._sq(street)}'", 50)

    def by_name_key(self, key, limit=60):
        return self._q(f"TRUE_OWNER1='{self._sq(key)}' OR TRUE_OWNER2='{self._sq(key)}'", limit)

    def by_legal(self, legal, limit=0):
        return []  # not supported live


# ─────────────────────────────────────────────────────────────────────────────
# ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────
MAX_NAME_PARCELS = 25


class Enricher:
    def __init__(self, source):
        self.src = source
        self.stats = Counter()

    @staticmethod
    def _addr_fields(p: dict, with_prop=True, with_mail=True) -> dict:
        out = {}
        if with_prop:
            street = p.get("site_addr", "")
            if p.get("site_unit"):
                street = f"{street} #{p['site_unit']}"
            out.update(prop_address=street, prop_city=p.get("site_city", "").title(),
                       prop_state="FL", prop_zip=p.get("site_zip", ""))
        if with_mail:
            mail = " ".join(x for x in (p.get("mail1", ""), p.get("mail2", "")) if x)
            out.update(mail_address=mail, mail_city=p.get("mail_city", "").title(),
                       mail_state=p.get("mail_state", "") or ("FL" if mail else ""),
                       mail_zip=p.get("mail_zip", ""))
        return out

    @staticmethod
    def _mail_key(p):
        return (norm_text(p.get("mail1", "")), zip5(p.get("mail_zip", "")))

    @staticmethod
    def _owner_names(p) -> set:
        keys = set()
        for o in ("owner1", "owner2", "owner3"):
            keys |= parcel_name_keys(p.get(o, ""))
        return keys

    def _pick_by_legal(self, parcels: list, legal: str) -> dict | None:
        lp = legal_parts(legal)
        if not legal_is_useful(lp) or not parcels:
            return None
        scored = sorted(((legal_score(lp, legal_parts(p.get("legal", ""))), p) for p in parcels),
                        key=lambda x: -x[0])
        best = scored[0]
        second = scored[1][0] if len(scored) > 1 else -99
        needed = 4 if lp["unit"] else 5
        if best[0] >= needed and best[0] > second:
            return best[1]
        return None

    def enrich(self, doc: dict) -> dict:
        """doc has: folios, addresses, legal, candidates (party names). Returns fields."""
        # 1. folio
        for folio in doc["folios"]:
            p = self.src.by_folio(folio)
            if p:
                self.stats["folio"] += 1
                owner = self._owner_from_parcel(doc, p)
                return {**self._addr_fields(p), "folio": folio, "match": "folio", **owner}

        # 2. street address from the clerk record
        street_only: dict = {}
        for addr in doc["addresses"]:
            street, unit = norm_street(addr)
            if not street or not re.match(r"^\d", street):
                continue
            hits = self.src.by_address(street)
            if not hits:
                continue
            if unit:
                u = [h for h in hits if (h.get("site_unit") or "").lstrip("0") == unit.lstrip("0")]
                hits = u or hits
            named = [h for h in hits if self._owner_names(h) & doc["cand_keys"]]
            pick = named[0] if len(named) >= 1 else (hits[0] if len(hits) == 1 else None)
            if pick is None:
                pick = self._pick_by_legal(hits, doc["legal"])
            if pick:
                self.stats["address"] += 1
                return {**self._addr_fields(pick), "folio": pick["folio"], "match": "address",
                        **self._owner_from_parcel(doc, pick)}
            # building found but unit/owner ambiguous: the street is still right
            if not street_only:
                street_only = {**self._addr_fields({**hits[0], "site_unit": unit}, with_mail=False),
                               "match": "address-street"}

        # 3. owner name (+ legal description to disambiguate)
        for party in doc["candidates"]:
            for key, first, last in clerk_name_variants(party):
                parcels = self.src.by_name_key(key, MAX_NAME_PARCELS + 1)
                if not parcels:
                    continue
                owner = {"owner": party, "owner_first": first, "owner_last": last}
                if len(parcels) == 1:
                    self.stats["owner"] += 1
                    p = parcels[0]
                    return {**self._addr_fields(p), "folio": p["folio"], "match": "owner", **owner}
                pick = self._pick_by_legal(parcels, doc["legal"])
                if pick:
                    self.stats["owner+legal"] += 1
                    return {**self._addr_fields(pick), "folio": pick["folio"],
                            "match": "owner+legal", **owner}
                if len(parcels) <= MAX_NAME_PARCELS:
                    mails = {self._mail_key(p) for p in parcels}
                    if len(mails) == 1 and parcels[0].get("mail1"):
                        # several properties, one owner mailing address – mail is reliable
                        self.stats["owner-mail-only"] += 1
                        return {**street_only, **self._addr_fields(parcels[0], with_prop=False),
                                "match": "owner-mail", **owner}
                break  # ambiguous for this party; try next party

        # 4. legal description alone
        if doc["legal"] and legal_is_useful(legal_parts(doc["legal"])):
            parcels = self.src.by_legal(doc["legal"])
            pick = self._pick_by_legal(parcels, doc["legal"])
            if pick:
                self.stats["legal"] += 1
                return {**self._addr_fields(pick), "folio": pick["folio"], "match": "legal",
                        **self._owner_from_parcel(doc, pick)}

        if street_only:
            self.stats["address-street-only"] += 1
            return street_only
        self.stats["none"] += 1
        return {}

    @staticmethod
    def _owner_from_parcel(doc: dict, p: dict) -> dict:
        """Pick the party whose name matches the parcel owner."""
        pkeys = set()
        for o in ("owner1", "owner2", "owner3"):
            pkeys |= parcel_name_keys(p.get(o, ""))
        for party in doc["candidates"] + doc["others"]:
            for key, first, last in clerk_name_variants(party):
                if key in pkeys:
                    return {"owner": party, "owner_first": first, "owner_last": last}
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# CLERK PORTAL (Playwright)
# ─────────────────────────────────────────────────────────────────────────────
FETCH_JS = """
async ([url, method]) => {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 55000);
  try {
    const r = await fetch(url, {
      method, credentials: 'include', signal: ctl.signal,
      headers: {'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json; charset=utf-8'},
    });
    return {status: r.status, text: await r.text()};
  } catch (e) {
    return {status: 0, text: String(e)};
  } finally { clearTimeout(t); }
}
"""

COLLECT_TYPES_JS = """
() => {
  const out = new Set();
  const add = t => { t = (t || '').replace(/\\s+/g, ' ').trim(); if (t) out.add(t); };
  document.querySelectorAll('option, [role=option], datalist option, li').forEach(el => {
    add(el.textContent); add(el.getAttribute('value'));
  });
  return Array.from(out);
}
"""


class ClerkPortal:
    def __init__(self):
        self.pw = None
        self.browser = None
        self.context = None
        self.page = None
        self.network_strings: set = set()
        self.logged_in = None
        self.api_base = CLERK_API
        self.api_seen: list = []

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=HEADLESS, args=["--disable-blink-features=AutomationControlled"])
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT, locale="en-US", timezone_id="America/New_York",
            viewport={"width": 1366, "height": 900})
        await self.context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        if CLERK_SESSION:
            await self.context.add_cookies([{
                "name": ".PremierIDDade", "value": CLERK_SESSION, "url": CLERK_BASE + "/"}])
            log.info("Added CLERK_SESSION cookie to browser")
        self.page = await self.context.new_page()
        self.page.on("response", self._sniff)
        self.page.on("request", self._sniff_request)
        await self.open_search_page()
        self.logged_in = await self.check_login()
        if not self.logged_in and CLERK_EMAIL and CLERK_PASSWORD:
            await self.login()
            self.logged_in = await self.check_login()
        log.info(f"Clerk session ready (logged in: {self.logged_in}) api_base={self.api_base}")
        if self.logged_in is None:
            for line in self.api_seen[:25]:
                log.info(f"  portal call: {line}")
        return self

    async def __aexit__(self, *exc):
        try:
            if exc and exc[0] is not None:
                await self.screenshot("error")
            await self.browser.close()
            await self.pw.stop()
        except Exception:  # noqa: BLE001
            pass

    async def _sniff(self, resp):
        """Collect strings from the portal's own API responses (doc-type lists)."""
        try:
            if "/api/" not in resp.url or "json" not in (resp.headers.get("content-type") or ""):
                return
            data = await resp.json()
        except Exception:  # noqa: BLE001
            return

        def walk(x, depth=0):
            if depth > 6:
                return
            if isinstance(x, str):
                if PORTAL_TYPE_RE.match(x.strip()):
                    self.network_strings.add(x.strip())
            elif isinstance(x, list):
                for i in x[:3000]:
                    walk(i, depth + 1)
            elif isinstance(x, dict):
                for v in x.values():
                    walk(v, depth + 1)
        walk(data)

    async def screenshot(self, name: str):
        try:
            path = DEBUG_DIR / f"{datetime.now():%Y%m%d_%H%M%S}_{name}.png"
            await self.page.screenshot(path=str(path), full_page=True)
            (DEBUG_DIR / f"{path.stem}.html").write_text(await self.page.content(), "utf-8")
            log.info(f"Saved debug screenshot {path.name}")
        except Exception:  # noqa: BLE001
            pass

    def _sniff_request(self, req):
        """Learn the real API base from the portal's own XHR/fetch calls."""
        try:
            url = req.url
            i = url.find("/api/")
            if i > 0 and req.resource_type in ("xhr", "fetch"):
                if len(self.api_seen) < 40:
                    self.api_seen.append(f"{req.method} {url[:160]}")
                base = url[:i + 4]
                if base != self.api_base and base.startswith(CLERK_BASE.split("/officialrecords")[0]):
                    log.info(f"Portal API base discovered: {base}")
                    self.api_base = base
        except Exception:  # noqa: BLE001
            pass

    async def _page_is_usable(self) -> bool:
        url = self.page.url
        if url.rstrip("/").lower().endswith("/undefined"):
            return False
        try:
            body = (await self.page.evaluate(
                "() => (document.body && document.body.innerText || '').trim().length"))
        except Exception:  # noqa: BLE001
            return False
        return body > 50

    async def open_search_page(self):
        last_err = None
        for attempt in range(1, 4):
            for target in CLERK_SEARCH_PAGES:
                try:
                    await self.page.goto(target, wait_until="domcontentloaded", timeout=60000)
                    try:
                        await self.page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:  # noqa: BLE001
                        pass
                    log.info(f"Opened {target} -> {self.page.url}")
                    if await self._page_is_usable():
                        CLERK_SEARCH_PAGES.sort(key=lambda t: t != target)  # remember winner
                        return
                    log.warning(f"Page at {self.page.url} looks empty/invalid – trying next route")
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    log.warning(f"Search page load failed ({target}, attempt {attempt}): {e}")
            await asyncio.sleep(5 * attempt)
        await self.screenshot("search_page_failed")
        raise RuntimeError(f"Could not open the clerk search page ({last_err})")

    async def api(self, method: str, path: str, params: dict | None = None) -> tuple:
        url = f"{self.api_base}/{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        last = (0, "")
        for attempt in range(1, 4):
            try:
                res = await self.page.evaluate(FETCH_JS, [url, method])
                last = (res["status"], res["text"])
            except Exception as e:  # noqa: BLE001  (page crashed / navigated)
                last = (0, str(e))
            status = last[0]
            if 200 <= status < 300:
                await asyncio.sleep(REQUEST_PAUSE)
                return last
            log.warning(f"API {method} {path} -> {status} (attempt {attempt}) {last[1][:150]}")
            if status in (401, 403, 419, 440):
                if attempt >= 2:
                    break  # refreshing the session once didn't help – it's an auth problem
                await self.open_search_page()  # refresh cookies / session, retry once
            elif status == 0 or status >= 500:
                await asyncio.sleep(3 * attempt)
                await self.open_search_page()
            else:
                break
        return last

    async def check_login(self):
        status, text = await self.api("GET", "Environment/isLoggedIn")
        if status != 200:
            return None
        try:
            d = json.loads(text)
        except Exception:  # noqa: BLE001
            return "true" in text.lower()
        if isinstance(d, bool):
            return d
        if isinstance(d, dict):
            for k in ("isLoggedIn", "loggedIn", "authenticated", "IsLoggedIn"):
                if k in d:
                    return bool(d[k])
        return None

    async def login(self):
        page = self.page
        log.info("Logging in to clerk portal …")
        try:
            clicked = False
            for sel in ["a:has-text('Log In')", "a:has-text('Login')", "a:has-text('Sign In')",
                        "button:has-text('Log In')", "button:has-text('Login')",
                        "button:has-text('Sign In')", "a[href*='login' i]"]:
                loc = page.locator(sel).first
                if await loc.count() and await loc.is_visible():
                    await loc.click()
                    clicked = True
                    break
            if not clicked:
                log.warning("No login link found on the search page")
                try:
                    links = await page.evaluate(
                        "() => Array.from(document.querySelectorAll('a,button'))"
                        ".map(e => (e.innerText||'').trim()).filter(Boolean).slice(0,40)")
                    log.warning(f"Visible links/buttons: {links}")
                except Exception:  # noqa: BLE001
                    pass
                await self.screenshot("no_login_link")
            await page.wait_for_selector("input[type=password]", timeout=30000)
            user = page.locator(
                "input[type=email], input[name*='user' i], input[id*='user' i], "
                "input[name*='email' i], input[id*='email' i], input[type=text]").first
            await user.fill(CLERK_EMAIL)
            await page.locator("input[type=password]").first.fill(CLERK_PASSWORD)
            submit = page.locator(
                "button[type=submit], input[type=submit], button:has-text('Log In'), "
                "button:has-text('Login'), button:has-text('Sign In')").first
            if await submit.count():
                await submit.click()
            else:
                await page.keyboard.press("Enter")
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            await self.open_search_page()
        except Exception as e:  # noqa: BLE001
            log.warning(f"Login attempt failed: {e}")
            await self.screenshot("login_failed")

    # ── document types ──────────────────────────────────────────────────────
    async def discover_types(self) -> dict:
        """Return {lead_code: [portal type names]}."""
        strings = set(self.network_strings)
        try:
            for s in await self.page.evaluate(COLLECT_TYPES_JS):
                if PORTAL_TYPE_RE.match(s):
                    strings.add(s)
        except Exception as e:  # noqa: BLE001
            log.warning(f"Doc-type discovery from page failed: {e}")
        log.info(f"Discovered {len(strings)} portal document types")
        mapping = {code: [] for code in LEAD_TYPES}
        for s in sorted(strings):
            code = classify_portal_type(s)
            if code:
                mapping[code].append(s)
        for code, meta in LEAD_TYPES.items():
            for k in meta["known"]:
                if k not in mapping[code]:
                    mapping[code].append(k)
        for code, names in mapping.items():
            if names:
                log.info(f"  {code:8s} <- {names}")
            else:
                log.info(f"  {code:8s} <- (no matching portal type)")
        return mapping

    # ── search ──────────────────────────────────────────────────────────────
    async def search_day(self, portal_type: str, day: str) -> list | None:
        params = {
            "partyName": "", "dateRangeFrom": day, "dateRangeTo": day,
            "documentType": portal_type, "searchT": portal_type,
            "firstQuery": "y", "searchtype": "Name/Document",
        }
        status, text = await self.api("POST", "home/standardsearch", params)
        if status != 200:
            return None
        try:
            d = json.loads(text)
        except Exception:  # noqa: BLE001
            log.warning(f"standardsearch non-JSON: {text[:200]}")
            return None
        qs = d.get("qs") if isinstance(d, dict) else None
        if not qs or (isinstance(d, dict) and d.get("isValidSearch") is False):
            log.debug(f"standardsearch invalid for {portal_type} {day}: {text[:200]}")
            return None
        status, text = await self.api("GET", "SearchResults/getStandardRecords", {"qs": qs})
        if status != 200:
            return None
        try:
            d = json.loads(text)
        except Exception:  # noqa: BLE001
            return None
        items = []
        if isinstance(d, list):
            items = d
        elif isinstance(d, dict):
            for k in ("recordingModels", "records", "results", "data", "items"):
                if isinstance(d.get(k), list):
                    items = d[k]
                    break
        return [i for i in items if isinstance(i, dict)]


def classify_portal_type(name: str) -> str | None:
    n = name.upper()
    for code, meta in LEAD_TYPES.items():
        if code != "RELLP" and NEGATIVE_TYPE_RE.search(n):
            continue
        if re.search(meta["pat"], n):
            return code
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ROW -> DOCUMENT
# ─────────────────────────────────────────────────────────────────────────────
def parse_date(raw) -> str:
    raw = str(raw or "").strip()
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:22].strip(), fmt).strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            continue
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else ""


def parse_amount(*vals) -> float | None:
    best = None
    for v in vals:
        try:
            f = float(str(v).replace("$", "").replace(",", ""))
            if f > 0 and (best is None or f > best):
                best = f
        except Exception:  # noqa: BLE001
            continue
    return best


def row_to_part(item: dict, code: str, portal_type: str) -> dict | None:
    try:
        year = str(item.get("cfN_YEAR") or "").strip()
        seq = str(item.get("cfN_SEQ") or "").strip()
        doc_num = str(item.get("clerk_File") or (f"{year} R {seq}" if year and seq else seq)).strip()
        if not doc_num:
            return None
        qs = item.get("qs") or ""
        clerk_url = (f"{CLERK_BASE}/DocumentDetail.aspx?qs={urllib.parse.quote(qs)}"
                     if qs else CLERK_SEARCH_PAGE)
        return {
            "doc_num": doc_num,
            "doc_type": code,
            "portal_type": str(item.get("doC_TYPE") or portal_type).strip(),
            "filed": parse_date(item.get("reC_DATE") or item.get("doC_DATE")),
            "p1": str(item.get("firsT_PARTY") or "").strip(),
            "p2": str(item.get("seconD_PARTY") or "").strip(),
            "legal": str(item.get("legaL_DESCRIPTION") or "").strip(),
            "amount": parse_amount(item.get("consideratioN_1"), item.get("consideratioN_2")),
            "folio": to_folio(item.get("foliO_NUMBER")),
            "address": str(item.get("address") or item.get("addressnounit") or "").strip(),
            "clerk_url": clerk_url,
        }
    except Exception as e:  # noqa: BLE001
        log.debug(f"bad row skipped: {e}")
        return None


def group_documents(parts: list) -> list:
    docs: "OrderedDict[str, dict]" = OrderedDict()
    for p in parts:
        key = f"{p['doc_num']}|{p['doc_type']}"
        d = docs.get(key)
        if d is None:
            d = docs[key] = {
                "doc_num": p["doc_num"], "doc_type": p["doc_type"],
                "portal_type": p["portal_type"], "filed": p["filed"],
                "p1": [], "p2": [], "legal": "", "amount": None,
                "folios": [], "addresses": [], "clerk_url": p["clerk_url"],
            }
        for side in ("p1", "p2"):
            if p[side] and p[side] not in d[side]:
                d[side].append(p[side])
        if len(p["legal"]) > len(d["legal"]) and "SEE DOC" not in p["legal"].upper():
            d["legal"] = p["legal"]
        elif not d["legal"]:
            d["legal"] = p["legal"]
        if p["amount"] and (d["amount"] is None or p["amount"] > d["amount"]):
            d["amount"] = p["amount"]
        if p["folio"] and p["folio"] not in d["folios"]:
            d["folios"].append(p["folio"])
        if p["address"] and p["address"] not in d["addresses"]:
            d["addresses"].append(p["address"])
        if not d["filed"]:
            d["filed"] = p["filed"]
    return list(docs.values())


def refine_type(doc: dict) -> str:
    """Sharpen generic lien codes using the parties involved."""
    code = doc["doc_type"]
    everyone = " | ".join(doc["p1"] + doc["p2"]).upper()
    if code == "LN":
        if ASSOC_RE.search(everyone):
            return "LNHOA"
        if re.search(r"MEDICAID|AGENCY FOR HEALTH|\bAHCA\b", everyone):
            return "MEDLN"
        if re.search(r"INTERNAL REVENUE|\bIRS\b", everyone):
            return "LNIRS"
    if code == "LNFED" and re.search(r"INTERNAL REVENUE|\bIRS\b|UNITED STATES", everyone):
        return "LNIRS"
    return code


def owner_candidates(doc: dict) -> tuple:
    """(likely owner parties, other parties). Banks, HOAs, government and
    'unknown tenant' style parties are never treated as the owner."""
    cands, others = [], []
    for name in doc["p1"] + doc["p2"]:
        kind = party_kind(name)
        (cands if kind == "person_or_owner" else others).append(name)
    # de-dup preserving order
    cands = list(OrderedDict.fromkeys(cands))
    others = [o for o in OrderedDict.fromkeys(others) if o not in cands]
    return cands, others


# ─────────────────────────────────────────────────────────────────────────────
# SCORE ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def compute_flags(rec: dict, doc: dict) -> list:
    code = rec["doc_type"]
    flags = []
    parties = doc["p1"] + doc["p2"]
    lender_or_assoc = any(party_kind(p) in ("lender", "assoc") for p in parties)
    if code == "LP":
        flags.append("Lis pendens")
        if lender_or_assoc:
            flags.append("Pre-foreclosure")
    if code in ("NOFC", "TAXDEED"):
        flags.append("Pre-foreclosure")
    if code in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if code in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if code in ("LNMECH", "LN"):
        flags.append("Mechanic lien")
    if code == "LNHOA":
        flags.append("HOA lien")
    if code == "MEDLN":
        flags.append("Medicaid lien")
    if code == "PRO" or ESTATE_RE.search((rec.get("owner") or "").upper()):
        flags.append("Probate / estate")
    if rec.get("owner") and is_corp(rec["owner"]) and code != "RELLP":
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(rec["filed"], "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:  # noqa: BLE001
        pass
    return list(OrderedDict.fromkeys(flags))


def compute_score(rec: dict) -> int:
    flags = rec.get("flags", [])
    score = 30 + 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amt = rec.get("amount") or 0
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5
    return max(0, min(100, score))


def stack_flags(records: list):
    """If one property has several distress filings this week, each record
    carries the other filings' distress flags (stacked distress = hotter lead)."""
    distress = {"Lis pendens", "Pre-foreclosure", "Judgment lien", "Tax lien",
                "Mechanic lien", "HOA lien", "Medicaid lien", "Probate / estate"}
    by_folio: dict = {}
    for r in records:
        if r.get("folio") and r["doc_type"] != "RELLP":
            by_folio.setdefault(r["folio"], []).append(r)
    stacked = 0
    for group in by_folio.values():
        if len({r["doc_num"] for r in group}) < 2:
            continue
        union = [f for r in group for f in r["flags"] if f in distress]
        for r in group:
            before = len(r["flags"])
            r["flags"] = list(OrderedDict.fromkeys(r["flags"] + union))
            if len(r["flags"]) > before:
                stacked += 1
    if stacked:
        log.info(f"Stacked distress flags on {stacked} records")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_clerk(days: list) -> list:
    parts, valid_searches, attempted = [], 0, 0
    async with ClerkPortal() as portal:
        mapping = await portal.discover_types()
        for code, portal_types in mapping.items():
            code_parts = 0
            for pt in portal_types:
                for day in days:
                    attempted += 1
                    try:
                        items = await portal.search_day(pt, day)
                    except Exception as e:  # noqa: BLE001
                        log.warning(f"{code} {pt} {day}: {e}")
                        items = None
                    if items is None:
                        if valid_searches == 0 and attempted >= 6:
                            await portal.screenshot("no_valid_searches")
                            raise AuthError(
                                "The clerk portal rejected the first 6 searches. The session is not "
                                "authorised – set CLERK_EMAIL/CLERK_PASSWORD (or refresh "
                                "CLERK_SESSION) in GitHub Secrets.")
                        continue
                    valid_searches += 1
                    if len(items) >= RESULT_CAP_WARN:
                        log.warning(f"{pt} on {day} returned {len(items)} rows – may be capped")
                    for it in items:
                        part = row_to_part(it, code, pt)
                        if part and part["filed"] in days:
                            parts.append(part)
                            code_parts += 1
            log.info(f"{code:8s}: {code_parts} rows")
        if attempted and valid_searches == 0:
            await portal.screenshot("no_valid_searches")
            raise AuthError(
                "The clerk portal rejected every search. The session is not authorised – "
                "set CLERK_EMAIL/CLERK_PASSWORD (or refresh CLERK_SESSION) in GitHub Secrets.")
    log.info(f"Clerk rows collected: {len(parts)} ({valid_searches}/{attempted} searches OK)")
    return parts


def build_records(parts: list, enricher: Enricher | None) -> list:
    docs = group_documents(parts)
    log.info(f"Unique documents: {len(docs)} (from {len(parts)} rows)")
    records = []
    for i, doc in enumerate(docs, 1):
        try:
            doc["doc_type"] = refine_type(doc)
            cands, others = owner_candidates(doc)
            doc["candidates"], doc["others"] = cands, others
            doc["cand_keys"] = {k for c in cands for k, _, _ in clerk_name_variants(c)}
            info = {}
            if enricher is not None:
                try:
                    info = enricher.enrich(doc)
                except Exception as e:  # noqa: BLE001
                    log.debug(f"enrich failed for {doc['doc_num']}: {e}")
            owner = info.get("owner") or (cands[0] if cands else (doc["p1"] or doc["p2"] or [""])[0])
            everyone = list(OrderedDict.fromkeys(doc["p1"] + doc["p2"]))
            grantee = "; ".join(p for p in everyone if p != owner)[:300]
            meta = LEAD_TYPES[doc["doc_type"]]
            rec = {
                "doc_num": doc["doc_num"],
                "doc_type": doc["doc_type"],
                "filed": doc["filed"],
                "cat": meta["cat"],
                "cat_label": CAT_LABELS[meta["cat"]],
                "owner": owner,
                "grantee": grantee,
                "amount": doc["amount"],
                "legal": doc["legal"],
                "prop_address": info.get("prop_address") or (doc["addresses"][0] if doc["addresses"] else ""),
                "prop_city": info.get("prop_city", ""),
                "prop_state": "FL",
                "prop_zip": info.get("prop_zip", ""),
                "mail_address": info.get("mail_address", ""),
                "mail_city": info.get("mail_city", ""),
                "mail_state": info.get("mail_state", ""),
                "mail_zip": info.get("mail_zip", ""),
                "clerk_url": doc["clerk_url"],
                "flags": [],
                "score": 0,
                # extras (ignored by the dashboard, useful for debugging / GHL)
                "folio": info.get("folio") or (doc["folios"][0] if doc["folios"] else ""),
                "match": info.get("match", ""),
                "portal_type": doc["portal_type"],
                "owner_first": info.get("owner_first", ""),
                "owner_last": info.get("owner_last", ""),
            }
            rec["flags"] = compute_flags(rec, doc)
            records.append(rec)
        except Exception as e:  # noqa: BLE001
            log.warning(f"Skipping bad document {doc.get('doc_num')}: {e}")
        if i % 250 == 0:
            log.info(f"  enriched {i}/{len(docs)}")
    stack_flags(records)
    for r in records:
        r["score"] = compute_score(r)
    records.sort(key=lambda r: (r["score"], r["filed"] or ""), reverse=True)
    return records


def build_output(records: list, days: list) -> dict:
    return {
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "Miami-Dade Clerk of Courts Official Records + Property Appraiser parcel roll",
        "date_range": {"from": days[0], "to": days[-1]},
        "total": len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "with_mail": sum(1 for r in records if r.get("mail_address")),
        "records": records,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GHL EXPORT
# ─────────────────────────────────────────────────────────────────────────────
GHL_FIELDS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State",
    "Mailing Zip", "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
]


def split_owner(rec: dict) -> tuple:
    name = re.sub(r"\s+", " ", ESTATE_RE.sub("", rec.get("owner") or "")).strip(" ,")
    if not name:
        return "", ""
    if is_business(name):
        return name, ""
    if rec.get("owner_first") and rec.get("owner_last"):
        return rec["owner_first"].title(), rec["owner_last"].title()
    if "," in name:
        last, first = name.split(",", 1)
        return first.strip().title(), last.strip().title()
    toks = name.split()
    if len(toks) == 1:
        return toks[0].title(), ""
    return " ".join(toks[1:]).title(), toks[0].title()  # clerk format LAST FIRST MIDDLE


def save_ghl_csv(records: list, path: Path):
    rows, seen = 0, set()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_FIELDS)
        w.writeheader()
        for r in records:
            if r["doc_type"] == "RELLP":
                continue  # a released lis pendens is not a lead
            first, last = split_owner(r)
            dedupe = (first.upper(), last.upper(),
                      (r.get("mail_address") or r.get("prop_address") or r.get("doc_num")).upper())
            if dedupe in seen:
                continue
            seen.add(dedupe)
            w.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address", ""), "Mailing City": r.get("mail_city", ""),
                "Mailing State": r.get("mail_state", ""), "Mailing Zip": r.get("mail_zip", ""),
                "Property Address": r.get("prop_address", ""), "Property City": r.get("prop_city", ""),
                "Property State": r.get("prop_state", "FL"), "Property Zip": r.get("prop_zip", ""),
                "Lead Type": r.get("cat_label", ""),
                "Document Type": LEAD_TYPES[r["doc_type"]]["label"],
                "Date Filed": r.get("filed", ""), "Document Number": r.get("doc_num", ""),
                "Amount/Debt Owed": f"{r['amount']:.2f}" if r.get("amount") else "",
                "Seller Score": r.get("score", ""),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source": "Miami-Dade Clerk Official Records",
                "Public Records URL": r.get("clerk_url", ""),
            })
            rows += 1
    log.info(f"GHL CSV saved: {path.name} ({rows} contacts)")


def prune_old_exports(keep_days: int = 30):
    cutoff = datetime.now() - timedelta(days=keep_days)
    for p in DATA_DIR.glob("ghl_export_2*.csv"):
        try:
            if datetime.strptime(p.stem.split("_")[-1], "%Y%m%d") < cutoff:
                p.unlink()
        except Exception:  # noqa: BLE001
            continue


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def lookback_days() -> list:
    today = datetime.now().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(LOOKBACK_DAYS, -1, -1)]


def get_enricher() -> Enricher | None:
    db = ParcelDB()
    if db.ensure():
        return Enricher(db)
    log.warning("Parcel DB unavailable – falling back to live county GIS lookups (slower)")
    return Enricher(LiveParcelLookup())


def main() -> int:
    days = lookback_days()
    log.info("=" * 64)
    log.info("Miami-Dade Motivated Seller Scraper v2")
    log.info(f"Window: {days[0]} -> {days[-1]} | login: {'yes' if CLERK_EMAIL else 'no'} | "
             f"session cookie: {'yes' if CLERK_SESSION else 'no'}")
    log.info("=" * 64)

    enricher = get_enricher()

    try:
        parts = asyncio.run(scrape_clerk(days))
    except AuthError as e:
        log.error(str(e))
        return 2
    except Exception as e:  # noqa: BLE001
        log.exception(f"Clerk scrape crashed: {e}")
        return 3

    records = build_records(parts, enricher)
    if enricher:
        log.info(f"Enrichment matches: {dict(enricher.stats)}")

    if not records:
        log.error("0 records found – NOT overwriting the dashboard. Check the log above "
                  "(login / portal changes). Previous data is kept.")
        return 4

    output = build_output(records, days)
    for path in (DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"):
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(output, indent=2, ensure_ascii=False), "utf-8")
        tmp.replace(path)
        log.info(f"Saved {path.relative_to(ROOT_DIR)}")

    save_ghl_csv(output["records"], DATA_DIR / f"ghl_export_{datetime.now():%Y%m%d}.csv")
    save_ghl_csv(output["records"], DATA_DIR / "ghl_export_latest.csv")
    prune_old_exports()

    by_type = Counter(r["doc_type"] for r in records)
    log.info(f"By type: {dict(by_type)}")
    log.info(f"Done. Total: {output['total']} | With property address: {output['with_address']} "
             f"| With mailing address: {output['with_mail']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
