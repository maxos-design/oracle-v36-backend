import requests
import pandas as pd
import numpy as np
import datetime

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def clean_column_names(df):
    """Καθαρίζει τα ονόματα: μικρά γράμματα, χωρίς κενά."""
    df.columns = df.columns.astype(str).str.lower().str.replace(' ', '_').str.replace('.', '_', regex=False)
    return df

def clean_record_for_json(record):
    """Απολυμαίνει τα δεδομένα από NaT, NaN, ημερομηνίες και φτιάχνει τους ακέραιους."""
    cleaned = {}
    for k, v in record.items():
        if pd.isna(v):  # Πιάνει NaN, None, NaT
            cleaned[k] = None
        elif isinstance(v, (datetime.date, datetime.datetime, pd.Timestamp)):
            cleaned[k] = v.isoformat()  # Μετατρέπει ημερομηνίες σε κείμενο
        elif isinstance(v, float) and v.is_integer():
            cleaned[k] = int(v)  # Μετατρέπει το 1.0 σε καθαρό 1
        elif isinstance(v, str) and v.endswith('.0') and v.replace('.0', '').isdigit():
            cleaned[k] = int(v.replace('.0', ''))  # Πιάνει την περίπτωση που έγινε κείμενο "1.0"
        else:
            cleaned[k] = v
    return cleaned

def upload_dataframe(table_name, df):
    print(f"   📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    
    # Μετατροπή σε λεξικά (records)
    records = df.to_dict(orient='records')
    # Καθαρισμός κάθε κελιού ξεχωριστά! Αυτό λύνει το σφάλμα PGRST102
    clean_data = [clean_record_for_json(r) for r in records]
    
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # Διαγραφή παλιών εγγραφών (αν υπάρχουν)
    try:
        requests.delete(f"{url}?select=*", headers=HEADERS)
    except:
        pass
    
    batch_size = 100
    for i in range(0, len(clean_data), batch_size):
        batch = clean_data[i:i+batch_size]
        # Χρησιμοποιούμε json=batch για απόλυτη συμβατότητα
        resp = requests.post(url, headers=HEADERS, json=batch)
        if resp.status_code in (200, 201, 204):
            print(f"   ✅ {min(i+batch_size, len(df))}/{len(df)}")
        else:
            print(f"   ❌ Σφάλμα: {resp.status_code} - {resp.text[:200]}")
            return False
    return True

# --- ΕΚΤΕΛΕΣΗ ---
print("📤 Ανεβάζω όλα τα δεδομένα στη Supabase...")

# 1. Ledger
print("\n   📜 Ledger...")
df_ledger = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
df_ledger = clean_column_names(df_ledger)

# Διόρθωση για το σφάλμα Integer: Μετατρέπουμε τα 1.0, 2.0 κτλ σε 1, 2
for col in df_ledger.select_dtypes(include=[np.number]).columns:
    df_ledger[col] = df_ledger[col].apply(lambda x: int(x) if pd.notna(x) and float(x).is_integer() else x)

df_ledger = df_ledger.drop(columns=['λ_(lambda)', 'lambda_value', 'μ_(mu)', 'mu_value'], errors='ignore')
upload_dataframe("ledger", df_ledger)

# 2. Enterprise Picks
print("\n   📄 Enterprise Picks...")
df_picks = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
df_picks = clean_column_names(df_picks)
df_picks = df_picks.rename(columns={'book_%': 'book_pct', 'stat_%': 'stat_pct'})
if "id" not in df_picks.columns:
    df_picks.insert(0, "id", range(1, len(df_picks) + 1))
upload_dataframe("picks", df_picks)

# 3. Top Picks
print("\n   🏆 Top Picks...")
df_top = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks", header=None)

header_idx = None
for idx, row in df_top.iterrows():
    row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
    if "Match" in row_values and "Market" in row_values and "Odds" in row_values:
        header_idx = idx
        break

if header_idx is not None:
    df_top.columns = df_top.iloc[header_idx]
    df_top = df_top.iloc[header_idx+1:]
    df_top = df_top.reset_index(drop=True)
    
    if "Match" in df_top.columns:
        df_top = df_top[df_top["Match"].notna()]
        df_top = df_top[~df_top["Match"].astype(str).str.contains("AVERAGE", na=False)]
    
    df_top = clean_column_names(df_top)
    # Προσθέσαμε και το stat_% -> stat_pct
    df_top = df_top.rename(columns={'02/05': 'date', 'sharp_%': 'sharp_pct', 'stat_%': 'stat_pct'})
    df_top = df_top.drop(columns=['#', 'id'], errors='ignore')
    
    # Διόρθωση για Integer και εδώ
    for col in df_top.select_dtypes(include=[np.number]).columns:
        df_top[col] = df_top[col].apply(lambda x: int(x) if pd.notna(x) and float(x).is_integer() else x)
        
    upload_dataframe("top_picks", df_top)
else:
    print("   ⚠️ Δεν βρέθηκε γραμμή επικεφαλίδων. Το ανέβασμα ακυρώθηκε.")

print("\n🎉 Τέλος!")
