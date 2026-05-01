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

def create_table_from_dataframe(table_name, df):
    """Δημιουργεί τον πίνακα στη Supabase με βάση τις στήλες του DataFrame."""
    # Απλοποιημένη αντιστοίχιση τύπων
    type_mapping = {
        'object': 'TEXT',
        'int64': 'FLOAT',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TEXT'
    }
    
    columns_def = []
    for col, dtype in df.dtypes.items():
        # Καθαρίζουμε το όνομα στήλης (το βάζουμε σε εισαγωγικά για να δεχτεί κενά και ειδικούς χαρακτήρες)
        col_name = f'"{col}"'
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        columns_def.append(f"{col_name} {sql_type}")
    
    columns_sql = ",\n    ".join(columns_def)
    create_sql = f"""
    DROP TABLE IF EXISTS public.{table_name} CASCADE;
    CREATE TABLE public.{table_name} (
        id SERIAL PRIMARY KEY,
        {columns_sql}
    );
    ALTER TABLE public.{table_name} DISABLE ROW LEVEL SECURITY;
    """
    
    # Εκτέλεση μέσω του SQL API της Supabase
    sql_url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    response = requests.post(sql_url, headers=HEADERS, json={"query": create_sql})
    
    if response.status_code in (200, 201, 204):
        print(f"   ✅ Ο πίνακας '{table_name}' δημιουργήθηκε με {len(df.columns)} στήλες.")
        return True
    else:
        # Αν το RPC δεν είναι διαθέσιμο, ενημερώνουμε τον χρήστη
        print(f"   ⚠️ Δεν ήταν δυνατή η αυτόματη δημιουργία του πίνακα '{table_name}'.")
        print(f"   Παρακαλώ εκτέλεσε το παρακάτω SQL στο Supabase SQL Editor:")
        print(create_sql)
        return False

def upload_dataframe(table_name, df):
    print(f"📤 Ανεβάζω {len(df)} εγγραφές στον πίνακα '{table_name}'...")
    
    # 1. Δημιουργούμε τον πίνακα αν δεν υπάρχει
    create_table_from_dataframe(table_name, df)
    
    # 2. Καθαρίζουμε τα δεδομένα
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) else x)
    
    records = df.to_dict(orient='records')
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # 3. Διαγραφή προηγούμενων εγγραφών
    try:
        requests.delete(f"{url}?id=gt.0", headers=HEADERS)
    except:
        pass
    
    # 4. Αποστολή με ασφαλή JSON encoding
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        json_data = json.dumps(batch, default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o)
        resp = requests.post(url, headers=HEADERS, data=json_data)
        if resp.status_code in (200, 201, 204):
            print(f"   ✅ {min(i+batch_size, len(df))}/{len(df)}")
        else:
            print(f"   ❌ Σφάλμα (batch {i//batch_size + 1}): {resp.status_code} - {resp.text[:200]}")
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

# Βρίσκουμε τη γραμμή με τα headers
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
