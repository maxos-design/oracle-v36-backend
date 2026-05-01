import requests
import pandas as pd
import math
import re

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def clean_row(row):
    """Αντικαθιστά όλα τα NaN, Inf, -Inf σε ένα λεξικό με None."""
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

def dataframe_to_safe_json(df):
    """Μετατρέπει ένα DataFrame σε λίστα από καθαρά λεξικά."""
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    return [clean_row(row) for row in records]

def upload_table(table_name, df):
    print(f"   📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    
    data = dataframe_to_safe_json(df)
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # Διαγραφή προηγούμενων εγγραφών
    try:
        requests.delete(f"{url}?select=id", headers=HEADERS)
    except Exception:
        pass
    
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

# 1. Ledger – μετονομάζουμε τις στήλες ώστε να ταιριάζουν με τη Supabase
print("\n   📜 Ledger...")
df_ledger = pd.read_excel("Oracle_Historical_Ledger.xlsx", sheet_name="Ledger")
rename_map = {
    "λ (Lambda)": "λ__Lambda_",
    "μ (Mu)": "μ__Mu_",
    "Home_Adv": "home_adv",
    "H_PPG": "h_ppg",
    "A_PPG": "a_ppg"
}
df_ledger.rename(columns=rename_map, inplace=True)
upload_table("ledger", df_ledger)

# 2. Enterprise Picks – ανέβηκε σωστά, απλά καθαρίζουμε τα ονόματα
print("\n   📄 Enterprise Picks...")
df_picks = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
rename_map_picks = {
    "Book %": "book",
    "Stat %": "stat",
    "stat_p": "stat_p",
    "Home Adv": "home_adv",
    "H PPG": "h_ppg",
    "A PPG": "a_ppg",
    "Proj Corners": "proj_corners",
    "Proj Cards": "proj_cards",
    "Top Scorer Pick": "top_scorer_pick",
    "Hedge Note": "hedge_note"
}
df_picks.rename(columns=rename_map_picks, inplace=True)
if "id" not in df_picks.columns:
    df_picks.insert(0, "id", range(1, len(df_picks) + 1))
upload_table("picks", df_picks)

# 3. Top Picks – αγνοούμε τις γραμμές τίτλου και μετονομάζουμε
print("\n   🏆 Top Picks...")
df_top = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks", header=None)

# Βρίσκουμε τη γραμμή που περιέχει τις επικεφαλίδες (συνήθως η 5η, index 4)
header_row = 4
df_top.columns = df_top.iloc[header_row]
df_top = df_top.iloc[header_row+1:]
df_top = df_top.reset_index(drop=True)

# Καθαρίζουμε τα ονόματα στηλών
df_top.columns = [str(col).strip() for col in df_top.columns]
rename_map_top = {
    "#": "id",
    "Stat %": "stat",
    "Sharp %": "sharp"
}
df_top.rename(columns=rename_map_top, inplace=True)

# Αφαιρούμε γραμμές που είναι κενές ή περιέχουν "AVERAGES"
df_top = df_top[~df_top['Match'].astype(str).str.contains('AVERAGES', na=False)]
df_top = df_top.dropna(subset=['Match'])

upload_table("top_picks", df_top)

print("\n🎉 Όλα τα δεδομένα ανέβηκαν επιτυχώς!")
