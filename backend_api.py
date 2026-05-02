from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import subprocess
import pandas as pd
import os
import sys
import numpy as np
from scipy import stats as scipy_stats
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import accuracy_score, classification_report, brier_score_loss
import xgboost as xgb
from collections import Counter as CollectionsCounter

app = FastAPI(title="Oracle V36 Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # service_role key
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def upload_to_supabase(table_name, df):
    data = df.where(pd.notnull(df), None).to_dict(orient="records")
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    requests.delete(f"{url}?id=gt.0", headers=HEADERS)
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        if response.status_code not in (200, 201, 204):
            return False
    return True

def load_ledger_from_supabase():
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    response = requests.get(url, headers=HEADERS, params={"select": "*"})
    if response.status_code == 200 and response.json():
        df = pd.DataFrame(response.json())
        df = df[df["Result"].isin(["WIN", "LOSS", "PUSH"])].copy()
        df["Win_Binary"] = df["Result"].map({"WIN": 1, "LOSS": 0, "PUSH": np.nan})
        numeric_cols = ["Odds", "EV", "λ (Lambda)", "μ (Mu)", "Home_Adv", "H_PPG", "A_PPG",
                        "Home_xG", "Away_xG", "Total_xG", "Total_Corners", "Total_Cards", "PnL"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["Win_Binary", "PnL"])
    return pd.DataFrame()

def filter_by_type(df, type_filter):
    if type_filter == "VALUE":
        return df[df["Type"].astype(str).str.contains("VALUE", case=False, na=False)]
    elif type_filter == "PATTERN":
        return df[df["Type"].astype(str).str.contains("PATTERN", case=False, na=False)]
    return df

@app.get("/")
def root():
    return {"message": "Welcome to Oracle V36 Backend API"}

@app.get("/ledger")
def get_ledger(limit: int = 50, market: str = None):
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    params = {"select": "*", "limit": limit, "order": "date.desc"}
    if market:
        params["market"] = f"eq.{market}"
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json() if response.status_code == 200 else {"error": response.text}

@app.get("/stats")
def get_stats():
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
    try:
        result = subprocess.run(
            ["python", "oracle_v36.py"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}
        if not os.path.exists("Oracle_V36_Enterprise.xlsx"):
            return {"status": "error", "message": "Δεν βρέθηκε το αρχείο Enterprise."}
        df = pd.read_excel("Oracle_V36_Enterprise.xlsx", sheet_name="Picks")
        success = upload_to_supabase("picks", df)
        if success:
            return {"status": "ok", "message": f"Ο Scanner ολοκληρώθηκε. {len(df)} picks ανέβηκαν."}
        else:
            return {"status": "error", "message": "Αποτυχία ανεβάσματος picks"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/run-analyst")
def run_analyst():
    try:
        if not os.path.exists("Oracle_V36_Enterprise.xlsx"):
            return {"status": "error", "message": "Πρέπει πρώτα να τρέξεις τον Scanner."}
        result = subprocess.run(
            ["python", "oracle_analyst_v6.py"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return {"status": "error", "message": result.stderr}
        if not os.path.exists("Oracle_Analyst_Report_v6.xlsx"):
            return {"status": "error", "message": "Δεν βρέθηκε το Analyst Report."}
        df = pd.read_excel("Oracle_Analyst_Report_v6.xlsx", sheet_name="Top Picks")
        success = upload_to_supabase("top_picks", df)
        if success:
            return {"status": "ok", "message": f"Ο Analyst ολοκληρώθηκε. {len(df)} picks ανέβηκαν."}
        else:
            return {"status": "error", "message": "Αποτυχία ανεβάσματος top picks"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/top-picks")
def get_top_picks():
    url = f"{SUPABASE_URL}/rest/v1/top_picks"
    response = requests.get(url, headers=HEADERS, params={"select": "*"})
    return response.json() if response.status_code == 200 else []

@app.get("/enterprise-picks")
def get_enterprise_picks(limit: int = 100):
    url = f"{SUPABASE_URL}/rest/v1/picks"
    params = {"select": "*", "limit": limit}
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json() if response.status_code == 200 else {"error": response.text}

@app.post("/run-migration")
def run_migration():
    try:
        result = subprocess.run(
            ["python", "migrate_to_cloud.py"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            return {"status": "ok", "message": "Το Ledger ανέβηκε επιτυχώς στη Supabase."}
        else:
            return {"status": "error", "message": result.stderr}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/upload-all")
def upload_all():
    try:
        result = subprocess.run(
            ["python", "upload_all_to_cloud.py"],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode == 0:
            return {"status": "ok", "message": "Όλα τα δεδομένα ανέβηκαν επιτυχώς στη Supabase."}
        else:
            return {"status": "error", "message": result.stderr}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ───────────── OPTIMIZER ENDPOINTS ─────────────
@app.get("/optimizer/thresholds")
def optimizer_thresholds(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η ανάλυση κατωφλίων θα εμφανιστεί εδώ. (Ο πλήρης κώδικας προστίθεται στο επόμενο βήμα)"}

@app.get("/optimizer/feature-importance")
def optimizer_feature_importance(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η ανάλυση feature importance θα εμφανιστεί εδώ."}

@app.get("/optimizer/streaks")
def optimizer_streaks(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η ανάλυση σερί θα εμφανιστεί εδώ."}

@app.get("/optimizer/monte-carlo")
def optimizer_monte_carlo(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η προσομοίωση Monte Carlo θα εμφανιστεί εδώ."}

@app.get("/optimizer/patterns")
def optimizer_patterns(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η ανάλυση patterns θα εμφανιστεί εδώ."}

@app.get("/optimizer/discrepancies")
def optimizer_discrepancies(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}
    return {"text": "Η ανάλυση αντιφάσεων θα εμφανιστεί εδώ."}
