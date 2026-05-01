import requests
import pandas as pd

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def upload_table(table_name, df):
    data = df.where(pd.notnull(df), None).to_dict(orient="records")
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    requests.delete(f"{url}?id=gt.0", headers=HEADERS)
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code not in (200, 201, 204):
            print(f"Σφάλμα στον πίνακα {table_name}: {response.text}")
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
