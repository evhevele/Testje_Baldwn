import os
import sys
import subprocess
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
from filelock import FileLock

# === 1️⃣ Doelmap instellen ===
base_path = os.path.expanduser('~/Testje_Baldwin')
output_dir = os.path.join(base_path, 'OUTPUT_Pharmacies')
os.makedirs(output_dir, exist_ok=True)
download_path = output_dir  # download directly into OUTPUT_Pharmacies

dataset = 'imtkaggleteam/pharmacies'

try:
    # === 2️⃣ Verbinden met Kaggle API ===
    api = KaggleApi()
    api.authenticate()  # gebruikt automatisch ~/.kaggle/kaggle.json
    
    print(f"📦 Downloaden van '{dataset}' naar {download_path} ...")
    api.dataset_download_files(dataset, path=download_path, unzip=True)
    print("✅ Download voltooid")
    
    # === 3️⃣ Voorbeeld van verwerking ===
    # originele naam zoals geleverd door de dataset
    original_csv = os.path.join(download_path, 'Pharmacies.csv')
    if not os.path.exists(original_csv):
        raise FileNotFoundError(f"Verwacht bestand niet gevonden: {original_csv}")

    # nieuw gewenste bestandsnaam in OUTPUT_Pharmacies
    output_csv = os.path.join(output_dir, '_Pharmacies_mostrecent.csv')

    # veilige sectie: gebruik file lock om races met andere scripts te voorkomen
    lock_path = os.path.join(output_dir, '.pharmacies.lock')
    with FileLock(lock_path, timeout=10):
        # lees het originele CSV
        df = pd.read_csv(original_csv)

        # schrijf naar de nieuwe bestandsnaam (pandas overschrijft bestaande bestanden)
        # gebruik atomic write: schrijf eerst naar een tmp-bestand en replace
        tmp_path = output_csv + '.tmp'
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, output_csv)
        print(f"📄 CSV ingelezen: {len(df)} rijen, {len(df.columns)} kolommen")
        print(f"💾 Bestand weggeschreven naar: {output_csv}")

    # kickoff next step: create dated copy in SCRIPTS
    next_script = os.path.join(os.path.dirname(__file__), '02_save_ruw_YYYYMMDD.py')
    print(f"▶️ Start volgende script: {next_script}")
    subprocess.check_call([sys.executable, next_script])

except Exception as e:
    print(f"⚠️ Er is een fout opgetreden: {e}")
