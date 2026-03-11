import pandas as pd
import numpy as np
import re
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

df_enforcement = pd.read_excel("raw/enforcement_actions.xlsx")
df_facility_types = pd.read_excel("raw/lookup_facility_types.xlsx")
df_narratives = pd.read_csv("raw/ltc_narratives.csv", encoding="latin-1")

os.makedirs("cleaned", exist_ok=True)

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

df_enforcement.columns = df_enforcement.columns.str.strip().str.upper()
df_enforcement['PENALTY_NUMBER']    = df_enforcement['PENALTY_NUMBER'].astype(str).str.strip()
df_enforcement['FAC_TYPE_CODE']     = df_enforcement['FAC_TYPE_CODE'].astype(str).str.strip().str.upper()
df_enforcement['FACID']             = df_enforcement['FACID'].astype(str).str.strip()
df_enforcement['PENALTY_ISSUE_DATE']= pd.to_datetime(df_enforcement['PENALTY_ISSUE_DATE'], errors='coerce')
df_enforcement['VIOLATION_FROM_DATE']= pd.to_datetime(df_enforcement['VIOLATION_FROM_DATE'], errors='coerce')
df_enforcement['DEATH_RELATED']     = df_enforcement['DEATH_RELATED'].fillna('N')


df_narratives.columns = df_narratives.columns.str.strip().str.upper()
df_narratives['PENALTY_NUMBER'] = df_narratives['PENALTY_NUMBER'].astype(str).str.strip()
df_narratives['FACID']          = df_narratives['FACID'].astype(str).str.strip()
df_narratives['NARRATIVE_CLEAN'] = df_narratives['NARRATIVE'].apply(clean_text)


print("TF-IDF keywords")
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

df_facility_types.columns = df_facility_types.columns.str.strip().str.upper()
df_facility_types['VALUE'] = df_facility_types['VALUE'].astype(str).str.strip().str.upper()
df_facility_types = df_facility_types.dropna(subset=['VALUE'])


print("Saving cleaned files")
df_enforcement.to_csv("cleaned/enforcement_actions.csv", index=False)
df_narratives.to_csv("cleaned/ltc_narratives.csv", index=False)
df_facility_types.to_csv("cleaned/lookup_facility_types.csv", index=False)

print(f"Complete")
print(f"  Enforcement actions: {df_enforcement.shape}")
print(f"  Narratives:          {df_narratives.shape}")
print(f"  Lookup:              {df_facility_types.shape}")
print(f"Saved Under Cleaned Folder")