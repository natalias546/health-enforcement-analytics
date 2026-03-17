#!/usr/bin/env python
# coding: utf-8

# In[3]:


import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns 
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd


# In[4]:


con = duckdb.connect("health_enforcement.duckdb")
df_mart = con.execute("SELECT * FROM enriched_mart").df()


# 

# In[7]:


INVALID_OUTCOMES = {
    'drop>deficiency',
    'dismissed by court', 
    'reversed',
    'appeal withdrawn by facility',
    'chow settlement',
}

def engineer_penalty_features(df):
    # ── filter out invalid outcomes first ─────────────────────────────────────
    valid_mask = ~df['CLASS_FINAL'].str.lower().fillna('').isin(INVALID_OUTCOMES)
    df = df[valid_mask].copy()
    print(f"  rows before: {valid_mask.shape[0]:,}  dropped: {(~valid_mask).sum():,}  remaining: {valid_mask.sum():,}")

    cat = df['PENALTY_CATEGORY'].str.lower().fillna('')

    # ── general ───────────────────────────────────────────────────────────────
    df['is_ae']               = cat.str.contains('ae:',               regex=False).astype(int)
    df['is_breach']           = cat.str.contains('breach',            regex=False).astype(int)
    df['is_deliberate_breach']= cat.str.contains('deliberate breach', regex=False).astype(int)
    df['is_ij']               = cat.str.contains('immediate jeopardy',regex=False).astype(int)
    df['is_non_ij']           = cat.str.contains('non-immediate jeopardy', regex=False).astype(int)

    # ── ae subtypes ───────────────────────────────────────────────────────────
    df['is_ae_death']         = cat.str.contains('death',             regex=False).astype(int)
    df['is_ae_assault']       = cat.str.contains('assault|sexual assault|physical assault', regex=True).astype(int)
    df['is_ae_surgery']       = cat.str.contains('surg|wrong body part|foreign object',    regex=True).astype(int)
    df['is_ae_medication']    = cat.str.contains('medication error',  regex=False).astype(int)
    df['is_ae_fall']          = cat.str.contains('fall',              regex=False).astype(int)
    df['is_ae_suicide']       = cat.str.contains('suicide',           regex=False).astype(int)
    df['is_ae_ulcer']         = cat.str.contains('ulcer',             regex=False).astype(int)

    # ── ltc-specific ──────────────────────────────────────────────────────────
    df['is_abuse']            = cat.str.contains('abuse',             regex=False).astype(int)
    df['is_falsification']    = cat.str.contains('falsif',            regex=False).astype(int)
    df['is_neglect']          = cat.str.contains('neglect',           regex=False).astype(int)
    df['is_staffing']         = cat.str.contains('staffing|nhppd',    regex=True).astype(int)

    return df

# apply to both
df_mart = engineer_penalty_features(df_mart.copy())


# Seperating based on facility type due to difference in nature of citations

# In[8]:


df_hospitals=df_mart[df_mart['IS_HOSPITAL']==1]
df_ltcn =df_mart[df_mart['IS_HOSPITAL']==0]


# In[11]:


facility_summary_hosp = df_hospitals.groupby('FACID').agg(
    total_penalties         = ('PENALTY_NUMBER',    'count'),
    total_deaths            = ('DEATH_RELATED',     'sum'),
    total_appealed          = ('APPEALED',          'sum'),
    total_balance_due       = ('TOTAL_BALANCE_DUE', 'sum'),
    total_complaints        = ('COMPLAINT_COUNT',   'sum'),
    total_ae                = ('is_ae',             'sum'),
    total_ae_death          = ('is_ae_death',       'sum'),
    total_ae_assault        = ('is_ae_assault',     'sum'),
    total_ae_surgery        = ('is_ae_surgery',     'sum'),
    total_ae_medication     = ('is_ae_medication',  'sum'),
    total_ae_fall           = ('is_ae_fall',        'sum'),
    total_ae_suicide        = ('is_ae_suicide',     'sum'),
    total_breach            = ('is_breach',         'sum'),
    total_deliberate_breach = ('is_deliberate_breach', 'sum'),
    total_ij                = ('is_ij',             'sum'),
    total_non_ij            = ('is_non_ij',         'sum'),
    total_ap_ij             = ('CLASS_FINAL',       lambda x: (x == 'AP IJ').sum()),
    total_ap_non_ij         = ('CLASS_FINAL',       lambda x: (x == 'AP NON-IJ').sum()),
    total_ftr_ae            = ('CLASS_FINAL',       lambda x: (x == 'FTR AE').sum()),
    total_ftr_br            = ('CLASS_FINAL',       lambda x: (x == 'FTR BR').sum()),
    total_ap_br             = ('CLASS_FINAL',       lambda x: (x == 'AP BR').sum()),
    last_penalty_date       = ('PENALTY_ISSUE_DATE','max'),
).reset_index()
print(f'Hospital facilities aggregated: {len(facility_summary_hosp):,}')


# In[12]:


facility_summary_ltc = df_ltcn.groupby('FACID').agg(
    total_penalties      = ('PENALTY_NUMBER',    'count'),
    total_deaths         = ('DEATH_RELATED',     'sum'),
    total_appealed       = ('APPEALED',          'sum'),
    total_balance_due    = ('TOTAL_BALANCE_DUE', 'sum'),
    total_complaints     = ('COMPLAINT_COUNT',   'sum'),
    total_abuse          = ('is_abuse',          'sum'),
    total_falsification  = ('is_falsification',  'sum'),
    total_neglect        = ('is_neglect',        'sum'),
    total_staffing       = ('is_staffing',       'sum'),
    total_ae_death       = ('is_ae_death',       'sum'),
    total_ae_ulcer       = ('is_ae_ulcer',       'sum'),
    total_class_aa       = ('CLASS_FINAL',       lambda x: (x == 'AA').sum()),
    total_class_a        = ('CLASS_FINAL',       lambda x: (x == 'A').sum()),
    total_class_b        = ('CLASS_FINAL',       lambda x: (x == 'B').sum()),
    total_ap_nhppd       = ('CLASS_FINAL',       lambda x: (x == 'AP NHPPD').sum()),
    total_wmf            = ('CLASS_FINAL',       lambda x: x.isin(['WMF>$1000','WMF<$1000']).sum()),
    total_wmo            = ('CLASS_FINAL',       lambda x: x.isin(['WMO>$1000','WMO=<$1000']).sum()),
    total_trebled        = ('CLASS_FINAL',       lambda x: x.isin(['A TREBLED','B TREBLED']).sum()),
    total_ftr_br         = ('CLASS_FINAL',       lambda x: (x == 'FTR BR').sum()),
    total_ap_br          = ('CLASS_FINAL',       lambda x: (x == 'AP BR').sum()),
    last_penalty_date    = ('PENALTY_ISSUE_DATE','max'),
).reset_index()
print(f'LTC facilities aggregated: {len(facility_summary_ltc):,}')


# In[18]:


def compute_rates(df, count_cols):
    df = df.copy()
    n = df['total_penalties'].replace(0, np.nan)
    for col in count_cols:
        df[col.replace('total_', '') + '_rate'] = df[col] / n
    df['appeal_rate']         = df['total_appealed']  / n
    df['complaint_rate_log']  = np.log1p(df['total_complaints']) / n
    df['balance_per_penalty'] = df['total_balance_due'] / n
    return df

HOSP_COUNT_COLS = [
    'total_ae','total_ae_death','total_ae_assault','total_ae_surgery',
    'total_ae_medication','total_ae_fall','total_ae_suicide',
    'total_breach','total_deliberate_breach','total_ij','total_non_ij',
    'total_ap_ij','total_ap_non_ij','total_ftr_ae','total_ftr_br',
    'total_ap_br','total_deaths',
]
LTC_COUNT_COLS = [
    'total_abuse','total_falsification','total_neglect','total_staffing',
    'total_ae_death','total_ae_ulcer',
    'total_class_aa','total_class_a','total_class_b',
    'total_ap_nhppd','total_wmf','total_wmo','total_trebled',
    'total_ftr_br','total_ap_br','total_deaths',
]

facility_summary_hosp = compute_rates(facility_summary_hosp, HOSP_COUNT_COLS)
facility_summary_ltc  = compute_rates(facility_summary_ltc,  LTC_COUNT_COLS)
print('Rates computed.')


# In[ ]:





# In[ ]:


def credibility_weight(df, rate_col, k=5):
    vals        = df[rate_col]
    counts      = df['total_penalties']
    global_mean = vals[counts > 0].mean()
    w           = counts / (counts + k)
    return w * vals + (1 - w) * global_mean

def apply_eb_smoothing(df, rates):
    df = df.copy()
    for rate in rates:
        if rate in df.columns:
            df[f'{rate}_eb'] = credibility_weight(df, rate)
    return df

HOSP_RATES = [
    'ae_rate','ae_death_rate','ae_assault_rate','ae_surgery_rate',
    'ae_medication_rate','ae_fall_rate','ae_suicide_rate',
    'breach_rate','deliberate_breach_rate','ij_rate','non_ij_rate',
    'ap_ij_rate','ap_non_ij_rate','ftr_ae_rate','ftr_br_rate',
    'ap_br_rate','deaths_rate',
]
LTC_RATES = [
    'abuse_rate','falsification_rate','neglect_rate','staffing_rate',
    'ae_death_rate','ae_ulcer_rate',
    'class_aa_rate','class_a_rate','class_b_rate',
    'ap_nhppd_rate','wmf_rate','wmo_rate','trebled_rate',
    'ftr_br_rate','ap_br_rate','deaths_rate',
]

facility_summary_hosp = apply_eb_smoothing(facility_summary_hosp, HOSP_RATES)
facility_summary_ltc  = apply_eb_smoothing(facility_summary_ltc,  LTC_RATES)
print('EB smoothing applied.')


# In[ ]:


from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import numpy as np

CONTAMINATION = 0.05
HOSP_CONTAM   = 0.15  # small pool so higher prior
RANDOM_STATE  = 42

def run_iso(df, features, contamination, label):
    X_raw    = df[features].copy()
    imputer  = SimpleImputer(strategy='median')
    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(imputer.fit_transform(X_raw))

    iso = IsolationForest(
        n_estimators=300,
        contamination=contamination,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    iso.fit(X_scaled)
    df = df.copy()
    df['anomaly_score'] = iso.decision_function(X_scaled)
    df['is_anomaly']    = iso.predict(X_scaled)

    print(f'[{label}] scored: {len(df):,}   anomalies flagged: {(df["is_anomaly"]==-1).sum()}')
    return df, iso, X_scaled, X_raw

HOSPITAL_FEATURES = [
    'total_penalties',
    'ap_ij_rate_eb', 'ap_non_ij_rate_eb',
    'ftr_ae_rate_eb', 'ftr_br_rate_eb', 'ap_br_rate_eb',
    'ae_death_rate_eb', 'ae_surgery_rate_eb', 'ae_assault_rate_eb',
    'ae_medication_rate_eb', 'deliberate_breach_rate_eb',
    'deaths_rate_eb', 'balance_per_penalty'
]
LTC_FEATURES = [
    'total_penalties',
    'class_aa_rate_eb', 'class_a_rate_eb',
    'abuse_rate_eb', 'falsification_rate_eb', 'neglect_rate_eb',
    'wmf_rate_eb', 'wmo_rate_eb', 'trebled_rate_eb',
    'ae_death_rate_eb', 'ae_ulcer_rate_eb', 'ap_nhppd_rate_eb',
    'ftr_br_rate_eb', 'deaths_rate_eb',
    'complaint_rate_log', 'balance_per_penalty'
]

df_hosp_scored, iso_hosp, X_hosp_scaled, X_hosp_raw = run_iso(
    facility_summary_hosp, HOSPITAL_FEATURES, HOSP_CONTAM, 'Hospital'
)
df_ltc_scored, iso_ltc, X_ltc_scaled, X_ltc_raw = run_iso(
    facility_summary_ltc, LTC_FEATURES, CONTAMINATION, 'LTC'
)


# In[40]:


n_hosp = (df_hosp_scored['is_anomaly'] == -1).sum()
n_ltc  = (df_ltc_scored['is_anomaly']  == -1).sum()
total  = n_hosp + n_ltc

HOSPITAL_QUOTA = max(1, round(25 * n_hosp / total))
LTC_QUOTA      = 25 - HOSPITAL_QUOTA

print(f'Anomalies  — hospitals: {n_hosp}   LTC: {n_ltc}')
print(f'Audit slots — hospitals: {HOSPITAL_QUOTA}   LTC: {LTC_QUOTA}')

top_hosp = df_hosp_scored.nsmallest(HOSPITAL_QUOTA, 'anomaly_score').copy()
top_hosp['stratum'] = 'Hospital'
top_ltc  = df_ltc_scored.nsmallest(LTC_QUOTA, 'anomaly_score').copy()
top_ltc['stratum']  = 'LTC & NTC'

top25 = (
    pd.concat([top_hosp, top_ltc])
    .sort_values('anomaly_score')
    .reset_index(drop=True)
)
top25.index += 1
top25[['stratum','FACID','anomaly_score','total_penalties','deaths_rate','balance_per_penalty']]

facility_info = df_mart[['FACID', 'FACILITY_NAME', 'DISTRICT_OFFICE']].drop_duplicates('FACID')
top25 = top25.merge(facility_info, on='FACID', how='left')

print("\nTOP 25 FACILITIES SELECTED FOR INSPECTION\n")
print(top25[[
    'stratum', 'FACID', 'FACILITY_NAME','DISTRICT_OFFICE','anomaly_score',
    'total_penalties', 'deaths_rate', 'balance_per_penalty'
]].to_string())



# import shap

# def explain(iso, X_scaled, X_raw, features, label):
#     explainer  = shap.TreeExplainer(iso)
#     sv_raw     = explainer.shap_values(X_scaled)
#     sv         = sv_raw[0] if isinstance(sv_raw, list) else sv_raw

#     print(f'── {label}: global feature importance ──')
#     shap.summary_plot(sv, X_raw, feature_names=features, plot_type='bar', show=True)

#     top_pos  = X_raw.reset_index(drop=True).index[
#         pd.Series(iso.decision_function(X_scaled)).idxmin()
#     ]
#     base_val = explainer.expected_value
#     if hasattr(base_val, '__len__'): base_val = float(base_val[0])

#     shap.plots.waterfall(
#         shap.Explanation(
#             values        = sv[top_pos],
#             base_values   = base_val,
#             data          = X_scaled[top_pos],
#             feature_names = features,
#         )
#     )

# explain(iso_hosp, X_hosp_scaled, X_hosp_raw, HOSPITAL_FEATURES, 'Hospital')
# explain(iso_ltc,  X_ltc_scaled,  X_ltc_raw,  LTC_FEATURES,      'LTC')


con.close()





