"""
03_make_masterfile.py

Zoekt de twee meest recente bestanden met naam `Pharmacies_YYYYMMDD.csv` in
~/Testje_Baldwin/OUTPUT_Pharmacies, merge't ze en schrijft `_Pharmacies_MASTERFILE.csv`.

Dit script wordt automatisch aangeroepen door 02_save_ruw_YYYYMMDD.py
"""

import os
import re
from datetime import datetime
import pandas as pd
from filelock import FileLock

DOWNLOAD_PATH = os.path.expanduser('~/Testje_Baldwin')
OUTPUT_DIR = os.path.join(DOWNLOAD_PATH, 'OUTPUT_Pharmacies')
MASTER_FILENAME = '_Pharmacies_MASTERFILE.csv'
PATTERN = re.compile(r'^Pharmacies_(\d{8})\.csv$')


def find_dated_files(directory):
    files = []
    if not os.path.exists(directory):
        return files
    for fname in os.listdir(directory):
        m = PATTERN.match(fname)
        if m:
            datepart = m.group(1)
            try:
                dt = datetime.strptime(datepart, '%Y%m%d')
                files.append((dt, os.path.join(directory, fname)))
            except ValueError:
                continue
    files.sort()  # ascending by date
    return files


def load_csv(path):
    return pd.read_csv(path, dtype=object)


def normalize_latlon(df):
    """Rename common latitude/longitude column variants to 'latitude' and 'longitude'.
    Convert these columns to numeric (floats), accepting commas as decimal separators.
    Warn about values that cannot be converted and values out of valid ranges.
    Operates in-place and returns the dataframe.
    """
    lat_variants = {'latitude', 'lat'}
    lon_variants = {'longitude', 'lon', 'lng', 'long'}

    col_map = {}
    # find first matching latitude column (case-insensitive)
    for c in df.columns:
        if c.lower() in lat_variants and 'latitude' not in df.columns:
            col_map[c] = 'latitude'
            break
    # find first matching longitude column (case-insensitive)
    for c in df.columns:
        if c.lower() in lon_variants and 'longitude' not in df.columns:
            col_map[c] = 'longitude'
            break

    if col_map:
        df.rename(columns=col_map, inplace=True)
        renamed = ', '.join(f"{k}->{v}" for k, v in col_map.items())
        print(f"ℹ️ Kolomnamen genormaliseerd: {renamed}")

    # convert to numeric floats, accept comma as decimal separator
    for coord in ('latitude', 'longitude'):
        if coord in df.columns:
            # replace common null/empty markers with NaN-safe strings then to numeric
            # coerce errors to NaN
            # first ensure we operate on strings to replace commas
            try:
                series = df[coord].astype(str).str.strip()
            except Exception:
                series = df[coord]

            # replace empty strings, 'nan', 'None' with NaN-friendly empty string
            series = series.replace({'': None, 'nan': None, 'None': None})
            # replace comma decimal separators with dot
            series = series.where(series.isna(), series.str.replace(',', '.', regex=False))
            # convert
            df[coord] = pd.to_numeric(series, errors='coerce')

            n_total = len(df)
            n_nan = int(df[coord].isna().sum())
            if n_nan > 0:
                print(f"⚠️ {n_nan}/{n_total} waarden in kolom '{coord}' konden niet geconverteerd worden naar numeriek en zijn NaN")

    # simple range checks
    if 'latitude' in df.columns:
        bad_lat = df['latitude'].notna() & ((df['latitude'] < -90) | (df['latitude'] > 90))
        if bad_lat.any():
            print(f"⚠️ {int(bad_lat.sum())} latitude waarden buiten bereik (-90..90)")
    if 'longitude' in df.columns:
        bad_lon = df['longitude'].notna() & ((df['longitude'] < -180) | (df['longitude'] > 180))
        if bad_lon.any():
            print(f"⚠️ {int(bad_lon.sum())} longitude waarden buiten bereik (-180..180)")

    return df


def build_master(older_df, newer_df):
    # ensure ID column exists
    if 'ID' not in older_df.columns and 'ID' not in newer_df.columns:
        raise KeyError("Geen 'ID' kolom gevonden in beide bestanden")

    # make ID the index for proper alignment
    if 'ID' in older_df.columns:
        older = older_df.set_index('ID')
    else:
        older = older_df.copy()
    if 'ID' in newer_df.columns:
        newer = newer_df.set_index('ID')
    else:
        newer = newer_df.copy()

    # union of indexes and columns
    all_index = older.index.union(newer.index)
    all_columns = older.columns.union(newer.columns)

    # reindex both to full shape
    older = older.reindex(index=all_index, columns=all_columns)
    newer = newer.reindex(index=all_index, columns=all_columns)

    # start with older values, then overlay newer values (newer wins on conflicts)
    master = older.copy()
    # assign newer values where notnull in newer (overwrite)
    for col in all_columns:
        newer_col = newer[col]
        master[col] = newer_col.combine_first(master[col])

    # reset index to have ID as column
    master = master.reset_index()
    return master


def main():
    files = find_dated_files(OUTPUT_DIR)
    if len(files) == 0:
        raise FileNotFoundError(f"Geen Pharmacies_YYYYMMDD.csv bestanden gevonden in {OUTPUT_DIR}")
    elif len(files) == 1:
        # slechts één bestand: kopieer als masterfile
        single_path = files[-1][1]
        lock_path = os.path.join(OUTPUT_DIR, '.pharmacies.lock')
        with FileLock(lock_path, timeout=10):
            df = load_csv(single_path)
            # normaliseer latitude/longitude kolomnamen
            df = normalize_latlon(df)
            out = os.path.join(OUTPUT_DIR, MASTER_FILENAME)
            tmp_out = out + '.tmp'
            df.to_csv(tmp_out, index=False)
            os.replace(tmp_out, out)
            print(f"Alleen één bestand gevonden; gekopieerd naar masterfile: {out}")
        return

    # neem de twee laatste bestanden
    older_dt, older_path = files[-2]
    newer_dt, newer_path = files[-1]
    print(f"Gebruik voorlaatste: {older_path} ({older_dt.date()}), laatste: {newer_path} ({newer_dt.date()})")

    lock_path = os.path.join(OUTPUT_DIR, '.pharmacies.lock')
    with FileLock(lock_path, timeout=10):
        older_df = load_csv(older_path)
        newer_df = load_csv(newer_path)

    master_df = build_master(older_df, newer_df)
    # normaliseer latitude/longitude kolomnamen in de master
    master_df = normalize_latlon(master_df)

    out = os.path.join(OUTPUT_DIR, MASTER_FILENAME)
    tmp_out = out + '.tmp'
    master_df.to_csv(tmp_out, index=False)
    os.replace(tmp_out, out)
    print(f"Masterfile geschreven naar: {out}")


if __name__ == '__main__':
    main()
