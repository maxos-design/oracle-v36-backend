import requests
import pandas as pd
import json
import math

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # Το service_role key σου
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return super().default(obj)

def upload_dataframe(table_name, df):
    print(f"📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    
    # Αντικαθιστούμε NaN/Inf με None παντού
    df = df.where(pd.notnull(df), None)
    # Δεύτερο πέρασμα για σιγουριά (μερικές φορές μένουν NaN)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) else x)
    
    records = df.to_dict(orient='records')
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # Διαγραφή προηγούμενων εγγραφών
    try:
        requests.delete(f"{url}?id=gt.0", headers=HEADERS)
    except:
        pass
    
    # Αποστολή με custom JSON encoding
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        # Σειριοποιούμε με τον SafeEncoder και στέλνουμε ως data (όχι ως json)
        json_data = json.dumps(batch, cls=SafeEncoder)
        resp = requests.post(url, headers=HEADERS, data=json_data)
        if resp.status_code in (200, 201, 204):
            print(f"   ✅ {min(i+batch_size, len(df))}/{len(df)}")
        else:
            print(f"   ❌ Σφάλμα: {resp.status_code} - {resp.text[:200]}")
            return False
    return True

# --- ΕΚΤΕΛΕΣΗ ---
print("📤 Ανεβάζω όλα τα δεδομένα στη Supabase...")

print("\n   📜 Ledger...")
df = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
upload_dataframe("ledger", df)

print("\n   📄 Enterprise Picks...")
df = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
upload_dataframe("picks", df)

print("\n   🏆 Top Picks...")
df = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks", header=None)

header_idx = None
for idx, row in df.iterrows():
    if "Match" in row.values and "Market" in row.values and "Odds" in row.values:
        header_idx = idx
        break

if header_idx is not None:
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:]
    df = df.reset_index(drop=True)
    
    if "Match" in df.columns:
        df = df[df["Match"].notna()]
        df = df[~df["Match"].astype(str).str.contains("AVERAGE", na=False)]
    
    upload_dataframe("top_picks", df)
else:
    print("   ⚠️ Δεν βρέθηκε γραμμή επικεφαλίδων. Το ανέβασμα ακυρώθηκε.")

print("\n🎉 Τέλος!")
