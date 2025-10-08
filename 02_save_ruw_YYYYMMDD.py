"""
02_save_ruw_YYYYMMDD.py

Leest `_Pharmacies_mostrecent.csv` uit de map OUTPUT_Pharmacies en
schrijft een gedateerde kopie `Pharmacies_YYYYMMDD.csv` naar dezelfde map.

Dit script wordt automatisch aangeroepen door 01_download_pharmacies.py
"""

import os
import sys
import subprocess
from datetime import datetime
from filelock import FileLock
import pandas as pd

# pad instellen (OUTPUT_Pharmacies)
base_path = os.path.expanduser('~/Testje_Baldwin')
output_dir = os.path.join(base_path, 'OUTPUT_Pharmacies')

# pad naar meest recente bestand
mostrecent_path = os.path.join(output_dir, '_Pharmacies_mostrecent.csv')
if not os.path.exists(mostrecent_path):
    raise FileNotFoundError(f"Verwacht bestand niet gevonden: {mostrecent_path}")

# gebruik file lock rond kritieke sectie
lock_path = os.path.join(output_dir, '.pharmacies.lock')
with FileLock(lock_path, timeout=10):
    # lees en schrijf gedateerde kopie
    df = pd.read_csv(mostrecent_path)

    # Detect common latitude/longitude column names and rename to 'latitude'/'longitude'
    lat_candidates = ['latitude', 'lat', 'Latitude', 'LATITUDE']
    lon_candidates = ['longitude', 'lon', 'lng', 'Longitude', 'LONGITUDE']

    col_map = {}
    for c in df.columns:
        if c in lat_candidates:
            col_map[c] = 'latitude'
        if c in lon_candidates:
            col_map[c] = 'longitude'

    if col_map:
        df = df.rename(columns=col_map)

    today = datetime.now().strftime('%Y%m%d')
    dated_filename = f"Pharmacies_{today}.csv"
    dated_path = os.path.join(output_dir, dated_filename)

    # atomic write
    tmp_dated = dated_path + '.tmp'
    df.to_csv(tmp_dated, index=False)
    os.replace(tmp_dated, dated_path)
    print(f"💾 Gekopieerd naar gedateerde bestandsnaam: {dated_path}")
    # Verwijder het originele downloadbestand (indien aanwezig) om rommel in OUTPUT_Pharmacies te voorkomen
    original_download = os.path.join(output_dir, 'Pharmacies.csv')
    if os.path.exists(original_download):
        try:
            os.remove(original_download)
            print(f"🧹 Verwijderd origineel downloadbestand: {original_download}")
        except OSError as e:
            print(f"⚠️ Kon origineel bestand niet verwijderen: {original_download} — {e}")

# kickoff next step: make masterfile (in SCRIPTS)
next_script = os.path.join(os.path.dirname(__file__), '03_make_masterfile.py')
print(f"▶️ Start volgende script: {next_script}")
subprocess.check_call([sys.executable, next_script])
