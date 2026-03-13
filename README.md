# Health Enforcement Analytics
End-to-end data pipeline and risk scoring model for California health facility enforcement actions (CDPH, 1998–2024).

---

## Quickstart
```bash
git clone https://github.com/natalias546/health-enforcement-analytics
cd health-enforcement-analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 pipeline.py
```


## Project Structure

**`ingest.py`** — Downloads 3 raw datasets from the California Health and Human Services Open Data Portal

**`transform.py`** — Cleans, standardizes, and enriches raw data before loading

**`schema.py`** — Loads cleaned CSVs into DuckDB and builds enriched mart via LEFT JOINs

**`pipeline.py`** — Orchestrates full pipeline end to end

**`data_audit.ipynb`** — Identifies data quality issues, mismatches, and anomalies before transformation

**`db_load_check.ipynb`** — Validates schema, join integrity, and mart shape post-load

**`analysis.ipynb`** — EDA, regression modeling, risk scoring, and top 25 facility prioritization

---

## Data Sources

**Enforcement Actions** — CDPH Open Data, 20,550 rows

**LTC Citation Narratives** — CDPH Open Data, 2,885 rows

**Facility Type Lookup** — CDPH Open Data, 41 rows

---

## Pipeline Architecture

```
ingest.py → raw/ → transform.py → cleaned/ → schema.py → health_enforcement.duckdb
```

## Key Transform Decisions

**Citation class standardization** — `CLASS_ASSESSED_INITIAL` and `CLASS_ASSESSED_FINAL` mapped to official CDPH Citation Class Criteria (`AA`, `A`, `B`, `AP`, `FTR`, `WMF`, `WMO`, `RD`, `DISMISSED`). Raw values contained tab characters, dollar-amount suffixes, and outcome labels requiring normalization.

**Complaint history decomposition** — `PRIORITY_ALL` decomposed into `HIGHEST_PRIORITY` and `COMPLAINT_COUNT` to capture pre-inspection complaint urgency and volume at the citation level.

**Facility type lookup filtering** — `dim_facility_types` filtered to `VARIABLE == FAC_TYPE_CODE` rows only. The source file is a multi-purpose lookup table containing reference data across multiple column types. Retaining all rows caused duplicate join keys and inflated the mart from 20,550 to 35,583 rows.

**Facility name standardization** — Facility names standardized to title case for consistent joins across enforcement and narratives tables.

**TF-IDF keyword extraction** — Applied to citation narrative text to extract the top 3 keywords per record.


## Data Quality Notes

**`CLASS_ASSESSED_FINAL` nulls** — Null for 16,416 records. Expected, as most enforcement actions are never formally appealed.

**`SPHOSP`** — 1 record with no corresponding description in the facility type lookup table.

**`ICFDD_CN`** — Lookup entry contained embedded facility IDs in the `VALUE` field. Cleaned via regex before loading.

**Facility name casing** — Acronyms may display with mixed casing after title case standardization (e.g. `Ucsf`, `Llc`). Values are consistent across datasets and do not affect joins.

**FACID**During exploration I discovered that facility names are not unique identifiers — the same company can operate multiple licensed facilities with separate FACIDs.

**Penalty_Category*The official penalty category lookup table contains 54 categories. Cross-referencing against the enforcement dataset revealed 10 additional categories present in enforcement records but absent from the lookup — including DHPPD nursing hour penalty tiers and hospital-specific adverse event codes. This may suggest data entry errors and suggests the lookup table has not been maintained in sync with the enforcement system. These categories were retained and flagged rather than discarded, preserving data integrity."