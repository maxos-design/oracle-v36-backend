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

def clean_row(row):
    """Αντικαθιστά όλα τα NaN, Inf, -Inf σε ένα λεξικό (γραμμή) με None."""
    cleaned = {}
    for key, value in row.items():
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            cleaned[key] = None
        elif isinstance(value, str) and value.lower() in ("nan", "inf", "-inf"):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

def dataframe_to_safe_json(df):
    """Μετατρέπει ένα DataFrame σε λίστα από καθαρά λεξικά."""
    # Πρώτα αντικαθιστούμε όλα τα NaN/Inf με None στο DataFrame
    df = df.where(pd.notnull(df), None)
    # Μετά μετατρέπουμε σε λεξικά
    records = df.to_dict(orient="records")
    # Τέλος, καθαρίζουμε κάθε γραμμή ξεχωριστά (για σιγουριά)
    return [clean_row(row) for row in records]

def upload_table(table_name, df):
    print(f"   📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    data = dataframe_to_safe_json(df)
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # Διαγραφή προηγούμενων εγγραφών (προαιρετικό, αν θες να κρατάς μόνο τα τελευταία)
    try:
        requests.delete(f"{url}?select=id", headers=HEADERS)
    except Exception as e:
        print(f"   Προειδοποίηση κατά τη διαγραφή: {e}")
    
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code in (200, 201, 204):
            print(f"   ✅ {min(i+batch_size, len(data))}/{len(data)}")
        else:
            print(f"   ❌ Σφάλμα στον πίνακα {table_name} (batch {i//batch_size + 1}): {response.status_code}")
            print(response.text)
            return False
    return True

print("📤 Ανεβάζω όλα τα δεδομένα στη Supabase...")

# 1. Ledger
print("\n   📜 Ledger...")
df_ledger = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
upload_table("ledger", df_ledger)

# 2. Enterprise Picks
print("\n   📄 Enterprise Picks...")
df_picks = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
if "id" not in df_picks.columns:
    df_picks.insert(0, "id", range(1, len(df_picks) + 1))
upload_table("picks", df_picks)

# 3. Top Picks
print("\n   🏆 Top Picks...")
df_top = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks")
upload_table("top_picks", df_top)

print("\n🎉 Όλα τα δεδομένα ανέβηκαν επιτυχώς!")
