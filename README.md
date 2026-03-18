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

**`schema.py`** — Loads cleaned CSVs into DuckDB and builds enriched mart via JOINs

**`model.py`** — Feature Engineering, anomaly detection and top 25 facilities to be inspected.

**`pipeline.py`** — Orchestrates full pipeline end to end

Optional: **`EDA.py`** - Some visualizations and explanatory stats on types of citations 

**`model_explain`** - Includes visuals of feature importance of model for distinct facilities


---

## Data Sources

**Enforcement Actions** — CDPH Open Data, 20,550 rows

**LTC Citation Narratives** — CDPH Open Data, 2,885 rows

**Facility Type Lookup** — CDPH Open Data, 41 rows

---

## Pipeline Architecture

```
ingest.py → raw/ → transform.py → cleaned/ → schema.py → health_enforcement.duckdb → model.py 

Optional Visuals: EDA.py 
```

## Key Transform Decisions

**Citation class standardization** — `CLASS_ASSESSED_INITIAL` and `CLASS_ASSESSED_FINAL` mapped to official CDPH Citation Class Criteria (`AA`, `A`, `B`, `AP`, `FTR`, `WMF`, `WMO`, `RD`, `DISMISSED`). Raw values contained tab characters, dollar-amount suffixes, and outcome labels requiring normalization.

**Complaint history decomposition** — `PRIORITY_ALL` decomposed into `HIGHEST_PRIORITY` and `COMPLAINT_COUNT` to capture pre-inspection complaint urgency and volume at the citation level.

**Facility name standardization** — Facility names standardized to title case for consistent joins across enforcement and narratives tables.

**TF-IDF keyword extraction** — Applied to citation narrative text to extract the top 3 keywords per record.

**IS_HOSPITAL** -Added for potential seperate analysis for distinct types of facilities

## Data Quality Notes

**`CLASS_FINAL`** — Takes CLASS_INITIAL if not appealed, and keeps class_final if it was appealed

**`SPHOSP`** — 1 record with no corresponding description in the facility type lookup table.

**Facility name casing** — Acronyms may display with mixed casing after title case standardization (e.g. `Ucsf`, `Llc`). Values are consistent across datasets and do not affect joins.

**FACID**During exploration I discovered that facility names are not unique identifiers — the same company can operate multiple licensed facilities with separate FACIDs.

**EVENTID** Initially, I had understood EVENTID to be an identifier of a certain inspection event in which a facility could get multiple enforcements. I derived a feature "Severity_per_visit" aggregating at the EVENTID and FACID level. However, I found that a the some EVENTID and FACID had enforcements over a larger range of time (Not within same day) which challenged my initial assumption. I decided to eliminate this feature and aggregate solely at the facility level.

**`Improvements with more time`** 
- Given more time, I would have extracted more meaning from narratives_df. At the transform stage, I decided to apply TFIDF in order to obtain top 3 words, but ultimately did not utilize in my final analysis/model. I would have liked to extract sentiment scores/severity scores and incorporate that with current features. 
- Additionally, I would have engineereed richer features and gotten more insight at the facility type level (Ex. Birthing centers,Community Clinics, Free Clinic). My current approach seperates facilities by Hospital and LTC/NLTC due to distinct nature of enforcements, but including an initial analysis on certain trends across faciliy types at a more granular level could have led to richer insight.

**`Unsuccessful Methods`**
- Clustering: Low Silhouette score and poor separation during clustering. Elbow method showed optimal k=4, but the interpretability of cluster profiles was weak, and did not ultimately lend itself for the use case of selecting 25 facilities that require inspection.
- Creating "predictor" target variable "Adverse event" labeling those facilities with a previous lethal ('AA', 'APITJ') citations. I then did my aggregations at the facility level and only looked at the history BEFORE the adverse event for those who had a previous lethal citation to prevent leakage. My logistic regression and ensemble methods performed poorly (especially in predicting the 1 class (catastrophic event)), which could be due to class imbalance (adverse events are more rare) but ultimately likely due to weak predictive power from features used. Additionally, I tried to do a temporal time split to see if model generalized well (train on data before 2015 and test on data after), but model did not perform well, likely due to procedural changes over time and organizational changes. This type of modeling was tricky, especially since it was hard to prevent leakage.  I would like to highlight, that I did build these models on all types of facilities at once (not seperating between hospitals/LTC+NLTC), which I believe also contributed to poor performance. Given more time, I would have potentially revisited this approach across different facilities.

**`Assumptions`**
- Facilities that have only been inspected once across all history could still be operational, and lack of history indicates no future instances 
- Facilities in this dataset are still operational
- This list is comprehensive and to the best of its ability, accurate, all enforcements are captured in the table
- Inspections could have led to no enforcement, for which case, we would not see on this table 

**`Generative AI Use:`**
- Claude for some bug fixes, help in improving visualizations, code review,  formatting and general overall questions