import requests
import os

os.makedirs("raw", exist_ok=True)

files = {
    "ltc_narratives.csv": "https://data.chhs.ca.gov/dataset/1e1e2904-1bfb-448c-97e1-cf3e228c9159/resource/cf865dab-bbf6-4cd7-b6bf-952669ace9fb/download/ltccitationnarratives19982017.csv",
    "enforcement_actions.xlsx": "https://data.chhs.ca.gov/dataset/1e1e2904-1bfb-448c-97e1-cf3e228c9159/resource/7c885969-3349-427f-8696-fba4374cd7f8/download/sea_final_20240730.xlsx",
    "lookup_facility_types.xlsx": "https://data.chhs.ca.gov/dataset/1e1e2904-1bfb-448c-97e1-cf3e228c9159/resource/e3e2e2ef-d83b-4163-880d-ad39c4f40433/download/2022_12_05_lookup-table-healthcare-facility-state-enforcement-actions.xlsx"
}

def download_file(filename, url):
    print(f"Downloading {filename}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total = 0
    with open(f"raw/{filename}", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            total += len(chunk)
            print(f"\r  {total/1024/1024:.1f} MB downloaded...", end="")
    print(f"\r  ✓ {filename} saved ({total/1024/1024:.1f} MB)")

for filename, url in files.items():
    download_file(filename, url)

print("\nAll files downloaded to raw/")