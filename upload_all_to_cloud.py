import requests
import pandas as pd
import math

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def get_supabase_columns(table_name):
    """Παίρνει τα ονόματα των στηλών ενός πίνακα από τη Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?limit=1"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if data and isinstance(data, list) and len(data) > 0:
            return list(data[0].keys())
    # Αν ο πίνακας είναι κενός, κάνουμε ένα OPTIONS request για να πάρουμε τις στήλες
    response = requests.options(url, headers=HEADERS)
    if response.status_code == 200:
        # Δεν είναι αξιόπιστο, οπότε επιστρέφουμε κενή λίστα
        return []
    return []

def clean_row(row):
    cleaned = {}
    for key, value in row.items():
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            cleaned[key] = None
        elif value is None:
            cleaned[key] = None
        elif isinstance(value, str) and value.lower() in ("nan", "inf", "-inf"):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

def filter_columns(df, columns):
    """Κρατάει μόνο τις στήλες που υπάρχουν στη λίστα columns."""
    available = [col for col in columns if col in df.columns]
    return df[available]

def upload_table_smart(table_name, df):
    print(f"   📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    
    # 1. Παίρνουμε τις στήλες που δέχεται η Supabase
    supabase_cols = get_supabase_columns(table_name)
    if not supabase_cols:
        print(f"   ⚠️ Δεν βρέθηκαν στήλες για τον πίνακα '{table_name}'. Παραλείπεται.")
        return False
    
    # 2. Φιλτράρουμε το DataFrame
    df = filter_columns(df, supabase_cols)
    if df.empty or len(df.columns) == 0:
        print(f"   ⚠️ Καμία κοινή στήλη για τον πίνακα '{table_name}'. Παραλείπεται.")
        return False
    
    print(f"   ℹ️ Θα ανέβουν {len(df.columns)} στήλες: {', '.join(df.columns[:10])}...")
    
    # 3. Καθαρισμός και ανέβασμα
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    data = [clean_row(row) for row in records]
    
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    try:
        requests.delete(f"{url}?select=id", headers=HEADERS)
    except:
        pass
    
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code in (200, 201, 204):
            print(f"   ✅ {min(i+batch_size, len(data))}/{len(data)}")
        else:
            print(f"   ❌ Σφάλμα (batch {i//batch_size + 1}): {response.text[:200]}")
            return False
    return True

print("📤 Ανεβάζω όλα τα δεδομένα στη Supabase...")

# 1. Ledger
print("\n   📜 Ledger...")
df_ledger = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
upload_table_smart("ledger", df_ledger)

# 2. Enterprise Picks
print("\n   📄 Enterprise Picks...")
df_picks = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
upload_table_smart("picks", df_picks)

# 3. Top Picks
print("\n   🏆 Top Picks...")
df_top = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks", header=None)

# Βρίσκουμε τη γραμμή που περιέχει τις επικεφαλίδες (ψάχνουμε για "Match", "Market", "Odds")
header_row = None
for idx, row in df_top.iterrows():
    row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
    if "Match" in row_values and "Market" in row_values and "Odds" in row_values:
        header_row = idx
        break

if header_row is not None:
    df_top.columns = df_top.iloc[header_row]
    df_top = df_top.iloc[header_row+1:]
    df_top = df_top.reset_index(drop=True)
    # Αφαιρούμε γραμμές που είναι κενές ή περιέχουν "AVERAGES"
    if "Match" in df_top.columns:
        df_top = df_top[~df_top["Match"].astype(str).str.contains("AVERAGES", na=False)]
    df_top = df_top.dropna(how='all')
    upload_table_smart("top_picks", df_top)
else:
    print("   ⚠️ Δεν βρέθηκε γραμμή κεφαλίδας στο Top Picks. Το ανεβάζω όπως είναι.")
    upload_table_smart("top_picks", df_top)

print("\n🎉 Όλα τα δεδομένα ανέβηκαν επιτυχώς!")
