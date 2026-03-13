import pandas as pd
import numpy as np
import re
import os
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS

# Load
df_enforcement    = pd.read_excel("raw/enforcement_actions.xlsx")
df_facility_types = pd.read_excel("raw/lookup_facility_types.xlsx")
df_narratives     = pd.read_csv("raw/ltc_narratives.csv", encoding="latin-1")

# Standardize column names immediately 
df_enforcement.columns    = df_enforcement.columns.str.strip().str.upper()
df_facility_types.columns = df_facility_types.columns.str.strip().str.upper()
df_narratives.columns     = df_narratives.columns.str.strip().str.upper()

# Clean facility types
df_facility_types = df_facility_types[
    ~df_facility_types['VARIABLE'].isin(['VARIABLE', 'VARIABLE  '])
].reset_index(drop=True)

df_facility_types['VALUE'] = df_facility_types['VALUE'].astype(str).str.strip()

# Clean enforcement actions 
print("Cleaning enforcement actions...")

df_enforcement['PENALTY_NUMBER']     = df_enforcement['PENALTY_NUMBER'].astype(str).str.strip()
df_enforcement['FAC_TYPE_CODE']      = df_enforcement['FAC_TYPE_CODE'].astype(str).str.strip().str.upper()
df_enforcement['FACID']              = df_enforcement['FACID'].astype(str).str.strip()
df_enforcement['PENALTY_ISSUE_DATE'] = pd.to_datetime(df_enforcement['PENALTY_ISSUE_DATE'], errors='coerce')
df_enforcement['VIOLATION_FROM_DATE']= pd.to_datetime(df_enforcement['VIOLATION_FROM_DATE'], errors='coerce')
df_enforcement['DEATH_RELATED']      = df_enforcement['DEATH_RELATED'].fillna('N')

df_enforcement['CLASS_ASSESSED_FINAL'] = (
    df_enforcement['CLASS_ASSESSED_FINAL']
    .str.strip()
    .str.replace(r'\t', '', regex=True)
)

df_enforcement['CLASS_ASSESSED_INITIAL'] = (
    df_enforcement['CLASS_ASSESSED_INITIAL']
    .str.strip()
    .str.replace(r'\t', '', regex=True)
)
# Penalty category standardization
penalty_category_map = {
    'AE04 - Retention of a foreign object in a patient': 'AE: Retention of a foreign object in a patient',
    'AE17 - Stage 3 or 4 ulcer acquired after admission': 'AE: Stage 3 or 4 ulcer acquired after admission',
    'AE22 - Death associated with a fall': 'AE: Death associated with a fall',
    'AE11 - Suicide/attempted suicide': 'AE: Suicide/attempted suicide',
    'AE12 - Medication error': 'AE: Medication error',
    'AE26  - Sexual assault on a patient': 'AE: Sexual assault on a patient',
    'AE27 - Death/injury from a physical assault': 'AE: Death/injury from a physical assault',
    'AE28 - Adverse event or series of adverse events': 'AE: Adverse event or series of adverse events',
    'AE01 - Surgery performed on a wrong body part': 'AE: Surgery performed on a wrong body part',
    'AE02 - Wrong patient surgery': 'AE: Wrong patient surgery',
    'AE03 - Wrong surgical procedure performed on a patient': 'AE: Wrong surgical procedure performed on a patient',
    'AE05 - Death during or up to 24 hours after surgery': 'AE: Death during or up to 24 hours after surgery',
    'AE06  -Use of contaminated drug, device, or biologic': 'AE: Use of contaminated drug, device, or biologic',
    'AE07 - Use of device other than as intended': 'AE: Use of device other than as intended',
    'AE08 - Death/disability due to intravascular air embolism': 'AE: Death/disability due to intravascular air embolism',
    'AE10 - Death/disability due to disappearance': 'AE: Death/disability due to disappearance',
    'AE13 - Hemolytic reaction': 'AE: Hemolytic reaction',
    'AE14 - Maternal death/disab due to labor/del/post': 'AE: Maternal death/disab due to labor/del/post',
    'AE15 - Death/disability directly related to hypoglycemia': 'AE: Death/disability directly related to hypoglycemia',
    'AE19 - Death/disability due to electric shock': 'AE: Death/disability due to electric shock',
    'AE20 - Line contaminated or use for wrong gas': 'AE: Line contaminated or use for wrong gas',
    'AE21 - Death/disability due to a burn': 'AE: Death/disability due to a burn',
    'AE23 - Death/disab assoc with use of restraints/bedrails': 'AE: Death/disab assoc with use of restraints/bedrails',
    'AE24 - Care by impersonating licensed provider': 'AE: Care by impersonating licensed provider',
    'AE25 - Abduction of a patient of any age': 'AE: Abduction of a patient of any age',
    'Financial Occurrence/Fac Not Self Reported': 'Financial Occurrence/Facility Not Self Reported',
    'Breach of IT system theft/loss of edevice/med rec': 'Breach of IT system theft/loss of device/med records',
    'Deliberate breach of PHI by health care worker': 'Deliberate breach of PHI (protected health information) by health care worker',
    'Other Immediate Jeopardy (not an AE)': 'Non-AE AP Immediate Jeopardy',
    'Other Non-Immediate Jeopardy (not an AE)': 'Non-AE AP Non-Immediate Jeopardy',
}
df_enforcement['PENALTY_CATEGORY'] = (
    df_enforcement['PENALTY_CATEGORY']
    .str.strip()
    .replace(penalty_category_map)
)

#Clean class_assesed_final before joining
final_map = {
    'B Trebled'           : 'B TREBLED',
    'A Trebled'           : 'A TREBLED',
    'B First'             : 'B FIRST',
    'Dismissed by Court'  : 'Dismissed by court',
    'CHOW- SETTLEMENT'    : 'CHOW SETTLEMENT',
    'APPEAL WITHDRAWN BY' : 'APPEAL WITHDRAWN BY FACILITY',
    'R/D=<$1000'          : 'R/D = <$1000',
    'FTR RES'             : 'FRTR RES',
    'DEPT WITHDREW CITATI': 'Dismissed by court', 
    'DISMISSED' : 'Dismissed by court',
    'WMF FIRST' : 'WF'
}

df_enforcement['CLASS_ASSESSED_FINAL'] = (
    df_enforcement['CLASS_ASSESSED_FINAL']
    .astype(str)
    .str.strip()
    .str.replace(r'\t', '', regex=True)
    .replace(final_map)
)

# Derived features
def get_highest_priority(val):
    if pd.isna(val):
        return None
    priorities = [p.strip() for p in str(val).split(',')]
    for p in ['A', 'B', 'C', 'D', 'E']:
        if p in priorities:
            return p
    return None

def count_complaints(val):
    if pd.isna(val):
        return 0
    return len(str(val).split(','))

df_enforcement['HIGHEST_PRIORITY'] = df_enforcement['PRIORITY_ALL'].apply(get_highest_priority)
df_enforcement['COMPLAINT_COUNT']  = df_enforcement['PRIORITY_ALL'].apply(count_complaints)

# Clean narratives + TF-IDF
print("Cleaning narratives...")

df_narratives['FACILITY_NAME']  = df_narratives['FACILITY_NAME'].str.title()
df_narratives['PENALTY_NUMBER'] = df_narratives['PENALTY_NUMBER'].astype(str).str.strip()
df_narratives['FACID']          = df_narratives['FACID'].astype(str).str.strip()

def clean_text(text):
    if pd.isna(text):
        return ''
    text = re.sub(r'F\d{3,4}', '', text)
    text = re.sub(r'T\d{2}', '', text)
    text = re.sub(r'DIV\d+', '', text)
    text = re.sub(r'CH\d+', '', text)
    text = re.sub(r'ART\d+', '', text)
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'\b\d+\b', '', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()

df_narratives['NARRATIVE_CLEAN'] = df_narratives['NARRATIVE'].apply(clean_text)

print("Extracting TF-IDF keywords...")
custom_stop_words = list(ENGLISH_STOP_WORDS) + [
    'resident', 'facility', 'patient', 'stated', 'according',
    'don', 'staff', 'nursing', 'hospital', 'following',
    'included', 'indicated', 'required', 'titled', 'dated',
    'review', 'record', 'noted', 'note', 'shall', 'use',
    'used', 'did', 'day', 'days', 'time', 'right', 'left'
]

tfidf = TfidfVectorizer(
    max_features=100,
    stop_words=custom_stop_words,
    ngram_range=(1, 2),
    token_pattern=r'[a-zA-Z]{3,}'
)

matrix = tfidf.fit_transform(df_narratives['NARRATIVE_CLEAN'].fillna(''))
terms  = tfidf.get_feature_names_out()

def get_top_keywords(row, terms, n=3):
    top_indices = np.argsort(row)[-n:][::-1]
    return ', '.join([terms[i] for i in top_indices])

df_narratives['TOP_KEYWORDS'] = [
    get_top_keywords(matrix[i].toarray()[0], terms)
    for i in range(matrix.shape[0])
]
df_narratives = df_narratives.drop(columns=['NARRATIVE'])

# Save 
os.makedirs("cleaned", exist_ok=True)

df_enforcement.to_csv("cleaned/enforcement_actions.csv", index=False)
df_narratives.to_csv("cleaned/ltc_narratives.csv", index=False)
df_facility_types.to_csv("cleaned/lookup_facility_types.csv", index=False)

print(f"\nDone!")
print(f"  Enforcement actions: {df_enforcement.shape}")
print(f"  Narratives:          {df_narratives.shape}")
print(f"  Lookup:              {df_facility_types.shape}")
print(f"\nCleaned files saved to cleaned/")