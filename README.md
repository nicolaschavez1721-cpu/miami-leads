[README.md](https://github.com/user-attachments/files/26588827/README.md)
# Miami-Dade Motivated Seller Lead Scraper

Automated daily scraper for motivated seller leads from Miami-Dade County Clerk of Courts Official Records. Enriches leads with property appraiser parcel data and scores each record 0–100.

## 🏠 Live Dashboard

> Deployed via GitHub Pages at `https://<your-username>.github.io/<repo-name>/`

---

## Lead Types Collected

| Code | Description | Category |
|------|-------------|----------|
| LP | Lis Pendens | Pre-Foreclosure |
| NOFC | Notice of Foreclosure | Pre-Foreclosure |
| TAXDEED | Tax Deed | Tax Distressed |
| JUD / CCJ / DRJUD | Judgment / Certified / Domestic | Judgment |
| LNCORPTX / LNIRS / LNFED | Corp Tax / IRS / Federal Lien | Tax Lien |
| LN / LNMECH / LNHOA | Lien / Mechanic / HOA | Lien |
| MEDLN | Medicaid Lien | Lien |
| PRO | Probate Documents | Probate |
| NOC | Notice of Commencement | Notice |
| RELLP | Release of Lis Pendens | Release |

---

## Seller Score (0–100)

| Factor | Points |
|--------|--------|
| Base score | 30 |
| Per flag detected | +10 |
| LP + Foreclosure combo | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed this week | +5 |
| Has property address | +5 |

**Flags detected:** Lis pendens · Pre-foreclosure · Judgment lien · Tax lien · Mechanic lien · Probate / estate · LLC / corp owner · New this week

---

## Setup & Deployment

### 1. Fork / Clone this repo

```bash
git clone https://github.com/your-username/miami-leads.git
cd miami-leads
```

### 2. Enable GitHub Pages

Go to **Settings → Pages → Source: GitHub Actions**

### 3. Enable GitHub Actions

Go to **Actions → Enable workflows**

The scraper runs automatically at **7:00 AM UTC** (3:00 AM ET) daily.

You can also trigger it manually via **Actions → Scrape Miami-Dade Leads → Run workflow**.

### 4. Local development

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

---

## File Structure

```
├── scraper/
│   ├── fetch.py          # Main scraper + enrichment pipeline
│   └── requirements.txt
├── dashboard/
│   ├── index.html        # Lead dashboard (GitHub Pages)
│   └── records.json      # Latest scraped data
├── data/
│   ├── records.json      # Same data (kept for history)
│   └── ghl_export_YYYYMMDD.csv  # GoHighLevel export
└── .github/workflows/
    └── scrape.yml        # Daily automation
```

---

## GoHighLevel Export

Each run generates a GHL-compatible CSV in `data/ghl_export_YYYYMMDD.csv` with columns:

`First Name, Last Name, Mailing Address, Mailing City, Mailing State, Mailing Zip, Property Address, Property City, Property State, Property Zip, Lead Type, Document Type, Date Filed, Document Number, Amount/Debt Owed, Seller Score, Motivated Seller Flags, Source, Public Records URL`

---

## Data Sources

- **Clerk Portal**: https://www2.miami-dadeclerk.com/officialrecords/Search.aspx
- **Property Appraiser**: https://www.miamidade.gov/pa/download.asp (bulk parcel NAL.zip)

---

*Built for Lifestyle International Realty — Doral Office*
