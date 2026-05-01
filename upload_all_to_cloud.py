import requests
import pandas as pd
import numpy as np
import math

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def clean_value(val):
    """Αντικαθιστά NaN, Inf, -Inf με None."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, str) and val.lower() == "nan":
        return None
    return val

def clean_dataframe(df):
    """Αντικαθιστά όλες τις NaN/Inf τιμές στο DataFrame με None."""
    # Χρήση της σύγχρονης μεθόδου df.map
    return df.map(clean_value)

def upload_table(table_name, df):
    print(f"   Καθαρισμός δεδομένων για τον πίνακα {table_name}...")
    df = clean_dataframe(df)
    data = df.where(pd.notnull(df), None).to_dict(orient="records")
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # Προσπάθεια διαγραφής προηγούμενων εγγραφών
    try:
        requests.delete(f"{url}?select=id", headers=HEADERS)
    except Exception as e:
        print(f"   Προειδοποίηση κατά τη διαγραφή: {e}")
    
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code not in (200, 201, 204):
            print(f"   Σφάλμα στον πίνακα {table_name}: {response.status_code}")
            return False
    return True

print("📤 Ανεβάζω όλα τα δεδομένα στη Supabase...")

# 1. Ledger
print("   📜 Ledger...")
df_ledger = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
upload_table("ledger", df_ledger)

# 2. Enterprise Picks
print("   📄 Enterprise Picks...")
df_picks = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
if "id" not in df_picks.columns:
    df_picks.insert(0, "id", range(1, len(df_picks) + 1))
upload_table("picks", df_picks)

# 3. Top Picks
print("   🏆 Top Picks...")
df_top = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks")
upload_table("top_picks", df_top)

print("✅ Ανέβασμα ολοκληρώθηκε!")
