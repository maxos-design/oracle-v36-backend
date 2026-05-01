from fastapi import FastAPI
import requests
import os

# Δημιουργούμε την εφαρμογή μας
app = FastAPI(title="Oracle V36 Backend")

# ───────────── ΡΥΘΜΙΣΕΙΣ ΣΥΝΔΕΣΗΣ ΜΕ SUPABASE ─────────────
SUPABASE_URL = "https://huizvgyasqjtsekevjxs.supabase.co"
SUPABASE_KEY = "sb_secret_cPbMP7IfUMI4rieanbpKBg_J2Ysxlwj"  # Χρησιμοποίησε το Service Role Key για να έχεις πρόσβαση
# ───────────────────────────────────────────────────────────

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# =====================================================================
# 1. ENDPOINT: Αρχική Σελίδα
# =====================================================================
@app.get("/")
def read_root():
    """Ένα απλό μήνυμα για να ξέρουμε ότι ο server τρέχει."""
    return {"message": "Welcome to Oracle V36 Backend API"}

# =====================================================================
# 2. ENDPOINT: Λήψη Ιστορικού (Ledger)
# =====================================================================
@app.get("/ledger")
def get_ledger(limit: int = 50, market: str = None):
    """
    Επιστρέφει τα τελευταία 'limit' παιχνίδια από το Ledger.
    Μπορείς προαιρετικά να φιλτράρεις ανά αγορά (π.χ. Under_2.5).
    """
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    params = {
        "select": "*",
        "limit": limit,
        "order": "date.desc"  # τα πιο πρόσφατα πρώτα
    }
    
    # Αν ζητήσουμε συγκεκριμένη αγορά
    if market:
        params["market"] = f"eq.{market}"
    
    response = requests.get(url, headers=HEADERS, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch data", "details": response.text}

# =====================================================================
# 3. ENDPOINT: Στατιστικά Κέρδους (PnL)
# =====================================================================
@app.get("/stats")
def get_stats():
    """
    Υπολογίζει βασικά στατιστικά από το Ledger (Συνολικό PnL, Win Rate).
    """
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    params = {
        "select": "result,pnl"
    }
    response = requests.get(url, headers=HEADERS, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if not data:
            return {"total_pnl": 0, "win_rate": 0, "total_bets": 0}
            
        total_pnl = sum(float(item.get("pnl", 0)) for item in data)
        wins = sum(1 for item in data if item.get("result") == "WIN")
        win_rate = (wins / len(data)) * 100 if len(data) > 0 else 0
        
        return {
            "total_bets": len(data),
            "total_pnl": round(total_pnl, 2),
            "win_rate": round(win_rate, 1)
        }
    else:
        return {"error": "Failed to fetch stats"}

# =====================================================================
# 4. ΕΚΚΙΝΗΣΗ ΤΟΥ SERVER
# =====================================================================
# Για να τρέξει ο server, άνοιξε το τερματικό και πληκτρολόγησε:
# uvicorn backend_api:app --reload
#
# Μετά μπες στον browser στη διεύθυνση: http://127.0.0.1:8000/docs
