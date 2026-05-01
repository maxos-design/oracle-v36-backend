from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import subprocess
import pandas as pd
import io
import os
import json

app = FastAPI(title="Oracle V36 Backend")

# Επιτρέπει την επικοινωνία με το Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ───────────── ΡΥΘΜΙΣΕΙΣ SUPABASE ─────────────
SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # Το service_role key σου
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# ───────────── ΒΟΗΘΗΤΙΚΗ ΣΥΝΑΡΤΗΣΗ ΓΙΑ ΝΑ ΑΝΕΒΑΖΟΥΜΕ ΔΕΔΟΜΕΝΑ ΣΤΗ SUPABASE ─────────────
def upload_to_supabase(table_name, df):
    """Ανεβάζει ένα pandas DataFrame στον πίνακα table_name της Supabase."""
    data = df.to_dict(orient="records")
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    # Διαγραφή όλων των προηγούμενων εγγραφών για να έχουμε φρέσκα δεδομένα
    requests.delete(f"{url}?id=gt.0", headers=HEADERS)
    # Μαζική εισαγωγή
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code not in (200, 201, 204):
            print(f"❌ Σφάλμα ανεβάσματος στον πίνακα {table_name}: {response.text}")
            return False
    return True

# ───────────── ENDPOINTS ─────────────

@app.get("/")
def root():
    return {"message": "Welcome to Oracle V36 Backend API"}

@app.get("/ledger")
def get_ledger(limit: int = 50, market: str = None):
    """Ιστορικό Ledger"""
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    params = {"select": "*", "limit": limit, "order": "date.desc"}
    if market:
        params["market"] = f"eq.{market}"
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json() if response.status_code == 200 else {"error": response.text}

@app.get("/stats")
def get_stats():
    """Στατιστικά"""
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    response = requests.get(url, headers=HEADERS, params={"select": "result,pnl"})
    data = response.json() if response.status_code == 200 else []
    total_pnl = sum(float(item.get("pnl", 0)) for item in data)
    wins = sum(1 for item in data if item.get("result") == "WIN")
    return {
        "total_bets": len(data),
        "total_pnl": round(total_pnl, 2),
        "win_rate": round((wins/len(data))*100, 1) if data else 0
    }

@app.post("/run-scanner")
def run_scanner():
    """Τρέχει τον Scanner και ανεβάζει τα picks στη Supabase."""
    try:
        # Εκτέλεση του Scanner
        result = subprocess.run(
            ["python", "oracle_v36.py"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}

        # Διάβασε το Excel που δημιούργησε ο Scanner
        if not os.path.exists("Oracle_V36_Enterprise.xlsx"):
            return {"status": "error", "message": "Το αρχείο Enterprise δεν βρέθηκε."}
        
        df = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
        # Μικρή προετοιμασία για τη Supabase (αντικατέστησε nan με None)
        df = df.where(pd.notnull(df), None)

        # Ανεβάζουμε στον πίνακα "picks"
        success = upload_to_supabase("picks", df)
        if success:
            return {"status": "ok", "message": f"Ο Scanner ολοκληρώθηκε. {len(df)} picks ανέβηκαν."}
        else:
            return {"status": "error", "message": "Αποτυχία ανεβάσματος picks"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/run-analyst")
def run_analyst():
    """Τρέχει τον Analyst και ανεβάζει το Top 15 στη Supabase."""
    try:
        # Βεβαιώσου ότι ο Analyst μπορεί να βρει το αρχείο Enterprise
        if not os.path.exists("Oracle_V36_Enterprise.xlsx"):
            return {"status": "error", "message": "Πρέπει πρώτα να τρέξεις τον Scanner."}

        result = subprocess.run(
            ["python", "oracle_analyst_v6.py"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}

        if not os.path.exists("Oracle_Analyst_Report_v6.xlsx"):
            return {"status": "error", "message": "Το αρχείο Analyst Report δεν βρέθηκε."}

        # Διάβασε το φύλλο "Top Picks"
        df = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks")
        df = df.where(pd.notnull(df), None)

        success = upload_to_supabase("top_picks", df)
        if success:
            return {"status": "ok", "message": f"Ο Analyst ολοκληρώθηκε. {len(df)} picks ανέβηκαν."}
        else:
            return {"status": "error", "message": "Αποτυχία ανεβάσματος top picks"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/top-picks")
def get_top_picks():
    """Επιστρέφει το τελευταίο Top 15."""
    url = f"{SUPABASE_URL}/rest/v1/top_picks"
    response = requests.get(url, headers=HEADERS, params={"select": "*"})
    return response.json() if response.status_code == 200 else []
