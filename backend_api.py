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
    """Φορτώνει το Ledger από τη Supabase και το επιστρέφει ως DataFrame."""
    url = f"{SUPABASE_URL}/rest/v1/ledger"
    response = requests.get(url, headers=HEADERS, params={"select": "*"})
    if response.status_code == 200 and response.json():
        df = pd.DataFrame(response.json())
        
        # 1. Εύρεση της στήλης αποτελέσματος (case‑insensitive)
        result_col = next((c for c in df.columns if c.lower() == "result"), None)
        if result_col is None:
            print("❌ Δεν βρέθηκε στήλη 'Result' ή 'result'")
            return pd.DataFrame()
        
        # 2. Εύρεση της στήλης P&L
        pnl_col = next((c for c in df.columns if c.lower() == "pnl"), "PnL")
        
        # 3. Φιλτράρισμα
        df = df[df[result_col].isin(["WIN", "LOSS", "PUSH"])].copy()
        df["Win_Binary"] = df[result_col].map({"WIN": 1, "LOSS": 0, "PUSH": np.nan})
        
        # 4. Μετατροπή αριθμητικών στηλών (ψάχνουμε με case‑insensitive)
        numeric_cols = {
            "odds": "Odds",
            "ev": "EV",
            "pnl": pnl_col,
            "home_xg": "Home_xG",
            "away_xg": "Away_xG",
            "total_xg": "Total_xG",
            "total_corners": "Total_Corners",
            "total_cards": "Total_Cards",
            "home_goals": "Home_Goals",
            "away_goals": "Away_Goals",
            "home_sot": "Home_SOT",
            "away_sot": "Away_SOT",
            "red_cards": "Red_Cards",
            "penalties": "Penalties"
        }
        
        for lower_name, standard_name in numeric_cols.items():
            col = next((c for c in df.columns if c.lower() == lower_name), None)
            if col:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            # Προσθέτουμε και τις στήλες λ, μ, ppg
            for greek_col in ["λ (Lambda)", "μ (Mu)", "Home_Adv", "H_PPG", "A_PPG"]:
                if greek_col in df.columns:
                    df[greek_col] = pd.to_numeric(df[greek_col], errors="coerce")
        
        # 5. Επιστρέφουμε μόνο γραμμές με P&L
        return df.dropna(subset=["Win_Binary", pnl_col])
    
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

    # ── ΟΛΟΚΛΗΡΗ Η ΑΝΑΛΥΣΗ ΚΑΤΩΦΛΙΩΝ ──
    # Ο κώδικας είναι ο ίδιος με αυτόν του τοπικού Optimizer (find_best_threshold & run_analysis)
    name_map = {
        "λ (Lambda)": ["λ (Lambda)", "λ", "Lambda"],
        "μ (Mu)": ["μ (Mu)", "μ", "Mu"],
        "Home_Adv": ["Home_Adv", "Home Adv", "HomeAdv"],
        "H_PPG": ["H_PPG", "H PPG", "HPPG"],
        "A_PPG": ["A_PPG", "A PPG", "APPG"],
        "EV": ["EV"],
        "Total_xG": ["Total_xG", "Total xG"],
    }
    col_mapping = {}
    for internal_name, possible_names in name_map.items():
        for col in possible_names:
            if col in df.columns:
                col_mapping[internal_name] = col
                break

    if not col_mapping:
        return {"text": "❌ Δεν βρέθηκαν γνωστές στήλες δεικτών στο Ledger."}

    features = list(col_mapping.keys())
    markets = df["Market"].unique()
    best_patterns = []

    for market in markets:
        for feat in features:
            real_col = col_mapping[feat]
            # --- συνάρτηση find_best_threshold ενσωματωμένη εδώ ---
            subset = df[df["Market"] == market]
            if len(subset) < 15:
                continue
            values = subset[real_col].dropna().unique()
            if len(values) < 5:
                continue
            test_thresholds = np.percentile(values, np.linspace(15, 85, 30))
            best_delta = -np.inf
            best_res = None
            for thresh in test_thresholds:
                group_above = subset[subset[real_col] >= thresh]
                group_below = subset[subset[real_col] < thresh]
                if len(group_above) < 10 or len(group_below) < 10:
                    continue
                wr_above = group_above["Win_Binary"].mean()
                wr_below = group_below["Win_Binary"].mean()
                roi_above = (group_above["PnL"].sum() / (len(group_above) * 10)) * 100
                roi_below = (group_below["PnL"].sum() / (len(group_below) * 10)) * 100
                delta = wr_above - wr_below
                _, p_val = scipy_stats.ttest_ind(group_above["Win_Binary"], group_below["Win_Binary"], equal_var=False)
                if abs(delta) > abs(best_delta) and p_val < 0.15:
                    best_delta = delta
                    best_res = {
                        'threshold': thresh,
                        'wr_above': wr_above,
                        'wr_below': wr_below,
                        'roi_above': roi_above,
                        'roi_below': roi_below,
                        'delta': delta,
                        'p_value': p_val,
                        'samples_above': len(group_above),
                        'samples_below': len(group_below),
                        'best_side': 'above' if wr_above > wr_below else 'below'
                    }
            if best_res:
                best_res['feat'] = feat
                best_res['market'] = market
                best_patterns.append(best_res)

    if not best_patterns:
        return {"text": "⚠️ Δεν βρέθηκαν στατιστικά σημαντικά μοτίβα.\nΑπαιτούνται τουλάχιστον 15-20 picks ανά αγορά."}

    best_patterns = sorted(best_patterns, key=lambda x: abs(x['delta']), reverse=True)

    # Δημιουργία αναφοράς κειμένου
    report_lines = []
    report_lines.append("🔍 Αποτελέσματα Ανάλυσης Κατωφλίων\n")
    report_lines.append("=" * 70)
    for p in best_patterns:
        sig = "⭐⭐⭐" if p['p_value'] < 0.05 else "⭐"
        better_side = p['best_side']
        if better_side == 'above':
            best_wr = p['wr_above']
            best_roi = p['roi_above']
            best_samples = p['samples_above']
            other_wr = p['wr_below']
            other_roi = p['roi_below']
            direction_text = f"ABOVE (≥ {p['threshold']:.3f})"
        else:
            best_wr = p['wr_below']
            best_roi = p['roi_below']
            best_samples = p['samples_below']
            other_wr = p['wr_above']
            other_roi = p['roi_above']
            direction_text = f"BELOW (< {p['threshold']:.3f})"
        report_lines.append(f"\n🎯 MARKET: {p['market']}")
        report_lines.append(f"   Feature  : {p['feat']} {sig}")
        report_lines.append(f"   Best Side: {direction_text}")
        report_lines.append(f"   Win Rate : {best_wr:.1%} (vs {other_wr:.1%} opposite)")
        report_lines.append(f"   Est. ROI : {best_roi:+.2f}% (vs {other_roi:+.2f}%)")
        report_lines.append(f"   vs Baseline 50%: {'✅ Above' if best_wr > 0.5 else '⚠️ Below'} baseline")
        report_lines.append(f"   Confidence: {(1-p['p_value']):.1%} | Samples: {best_samples}")
        report_lines.append("-" * 70)

    return {"text": "\n".join(report_lines)}

@app.get("/optimizer/feature-importance")
def optimizer_feature_importance(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}

    # ── ΑΝΑΛΥΣΗ FEATURE IMPORTANCE ──
    feature_cols = ["λ (Lambda)", "μ (Mu)", "Home_Adv", "H_PPG", "A_PPG", "EV", "Odds"]
    available = [c for c in feature_cols if c in df.columns]
    if len(available) < 3:
        return {"text": "❌ Δεν βρέθηκαν αρκετές διαθέσιμες στήλες-δείκτες στο Ledger."}

    model_df = df[available + ["Win_Binary"]].dropna()
    if len(model_df) < 20:
        return {"text": f"❌ Χρειάζονται τουλάχιστον 20 εγγραφές (βρέθηκαν {len(model_df)})."}

    X = model_df[available]
    y = model_df["Win_Binary"]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Random Forest
    rf = RandomForestClassifier(n_estimators=200, max_depth=4, random_state=42, class_weight='balanced')
    rf.fit(X_scaled, y)
    rf_importances = rf.feature_importances_
    rf_cv_mean = cross_val_score(rf, X_scaled, y, cv=5, scoring='accuracy').mean()

    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    rf.fit(X_train, y_train)
    rf_preds = rf.predict(X_test)
    rf_probs = rf.predict_proba(X_test)[:, 1]
    rf_acc = accuracy_score(y_test, rf_preds)
    rf_brier = brier_score_loss(y_test, rf_probs)

    # XGBoost
    xgb_model = xgb.XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.1,
                                  subsample=0.8, colsample_bytree=0.8,
                                  random_state=42, eval_metric='logloss')
    xgb_model.fit(X_train, y_train)
    xgb_importances = xgb_model.feature_importances_
    xgb_cv_mean = cross_val_score(xgb_model, X_scaled, y, cv=5, scoring='accuracy').mean()
    xgb_preds = xgb_model.predict(X_test)
    xgb_probs = xgb_model.predict_proba(X_test)[:, 1]
    xgb_acc = accuracy_score(y_test, xgb_preds)
    xgb_brier = brier_score_loss(y_test, xgb_probs)

    # Δημιουργία αναφοράς
    report_lines = []
    report_lines.append("🧠 Feature Importance (Random Forest vs XGBoost)\n")
    report_lines.append("=" * 60)
    report_lines.append(f"  Test Set Size: {len(X_test)} picks\n")
    report_lines.append(f"  {'Metric':<20} {'Random Forest':<15} {'XGBoost':<15}")
    report_lines.append("-" * 50)
    report_lines.append(f"  {'Accuracy':<20} {rf_acc:<15.1%} {xgb_acc:<15.1%}")
    report_lines.append(f"  {'Brier Score':<20} {rf_brier:<15.3f} {xgb_brier:<15.3f}")
    report_lines.append(f"  {'CV Accuracy':<20} {rf_cv_mean:<15.1%} {xgb_cv_mean:<15.1%}")

    best_model_name = "XGBoost" if xgb_brier < rf_brier else "Random Forest"
    report_lines.append(f"\n🏆 Best Model: {best_model_name}\n")
    report_lines.append(f"📋 Classification Report for {best_model_name}:")
    report_lines.append("-" * 50)
    report_lines.append(f"  {'Class':<8} {'Precision':<10} {'Recall':<10} {'F1-Score':<10} {'Support':<10}")
    best_report = classification_report(y_test, xgb_preds if best_model_name == "XGBoost" else rf_preds,
                                         target_names=["LOSS", "WIN"], output_dict=True)
    for cls, metrics in best_report.items():
        if isinstance(metrics, dict):
            report_lines.append(f"  {cls:<8} {metrics['precision']:<10.2f} {metrics['recall']:<10.2f} "
                                f"{metrics['f1-score']:<10.2f} {metrics['support']:<10.0f}")
    report_lines.append("-" * 50)

    report_lines.append("\n🔍 Feature Importance Rankings:\n")
    report_lines.append(f"  {'Feature':<20} {'RF Imp.':<10} {'XGB Imp.':<10}")
    report_lines.append("-" * 42)
    for name, r_imp, x_imp in zip(available, rf_importances, xgb_importances):
        report_lines.append(f"  {name:<20} {r_imp:<10.3f} {x_imp:<10.3f}")

    return {"text": "\n".join(report_lines)}

@app.get("/optimizer/streaks")
def optimizer_streaks(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}

    # ── ΑΝΑΛΥΣΗ ΣΕΡΙ ──
    results = df["Win_Binary"].dropna().values
    if len(results) < 20:
        return {"text": "❌ Χρειάζονται τουλάχιστον 20 αποτελέσματα για ανάλυση σερί."}

    streaks = []
    current_streak = []
    for r in results:
        if not current_streak or current_streak[-1] == r:
            current_streak.append(int(r))
        else:
            streaks.append((current_streak[0], len(current_streak)))
            current_streak = [int(r)]
    if current_streak:
        streaks.append((current_streak[0], len(current_streak)))

    win_streaks = [s[1] for s in streaks if s[0] == 1]
    loss_streaks = [s[1] for s in streaks if s[0] == 0]

    win_counter = CollectionsCounter(win_streaks) if win_streaks else {}
    loss_counter = CollectionsCounter(loss_streaks) if loss_streaks else {}

    win_rate = results.mean()
    loss_rate = 1 - win_rate
    max_win = max(win_streaks) if win_streaks else 0
    max_loss = max(loss_streaks) if loss_streaks else 0

    report_lines = []
    report_lines.append("📈 Ανάλυση Σερί (Streaks)\n")
    report_lines.append(f"   Συνολικά αποτελέσματα: {len(results)}")
    report_lines.append(f"   Win Rate: {win_rate:.1%}")
    report_lines.append(f"   Loss Rate: {loss_rate:.1%}\n")

    report_lines.append("   ── WIN STREAKS ──")
    for length in sorted(win_counter.keys()):
        report_lines.append(f"   {length} νίκες σερί: {win_counter[length]} φορές")
    report_lines.append(f"   Μέγιστο σερί νικών: {max_win}\n")

    report_lines.append("   ── LOSS STREAKS ──")
    for length in sorted(loss_counter.keys()):
        report_lines.append(f"   {length} ήττες σερί: {loss_counter[length]} φορές")
    report_lines.append(f"   Μέγιστο σερί ηττών: {max_loss}\n")

    report_lines.append("   ── ΘΕΩΡΗΤΙΚΕΣ ΠΙΘΑΝΟΤΗΤΕΣ (Markov) ──")
    report_lines.append(f"   Πιθανότητα 3 συνεχόμενων ηττών: {loss_rate ** 3:.2%}")
    report_lines.append(f"   Πιθανότητα 5 συνεχόμενων ηττών: {loss_rate ** 5:.2%}")
    report_lines.append(f"   Πιθανότητα 7 συνεχόμενων ηττών: {loss_rate ** 7:.2%}")

    expected_max = int(np.log(1/500) / np.log(loss_rate)) if 0 < loss_rate < 1 else 0
    report_lines.append(f"   Αναμενόμενο μέγιστο σερί ηττών σε 500 πονταρίσματα: ~{expected_max}\n")

    report_lines.append("💡 ΨΥΧΟΛΟΓΙΚΗ ΠΡΟΕΤΟΙΜΑΣΙΑ:")
    report_lines.append(f"   Να είσαι προετοιμασμένος να χάσεις {expected_max} συνεχόμενα στοιχήματα.")
    report_lines.append("   Αυτό είναι ΦΥΣΙΟΛΟΓΙΚΟ και ΑΝΑΜΕΝΟΜΕΝΟ. Μην αλλάξεις στρατηγική!")

    return {"text": "\n".join(report_lines)}

@app.get("/optimizer/monte-carlo")
def optimizer_monte_carlo(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}

    pnls = df["PnL"].dropna().values
    if len(pnls) < 20:
        return {"text": "❌ Χρειάζονται τουλάχιστον 20 settled picks για αξιόπιστο Monte Carlo."}

    n_bets = 500
    n_sims = 5000
    start_bankroll = 300

    simulations = np.random.choice(pnls, size=(n_sims, n_bets), replace=True)
    cumulative_pnl = np.cumsum(simulations, axis=1)
    bankrolls = start_bankroll + cumulative_pnl

    final_bankrolls = bankrolls[:, -1]
    ruin_prob = np.mean(np.any(bankrolls <= 0, axis=1))
    median_final = np.median(final_bankrolls)
    pct_5 = np.percentile(final_bankrolls, 5)
    pct_95 = np.percentile(final_bankrolls, 95)
    win_rate_hist = (pnls > 0).mean()

    report_lines = []
    report_lines.append("🎲 MONTE CARLO SIMULATOR (Bootstrapping από το Ledger)\n")
    report_lines.append("=" * 60)
    report_lines.append(f"📈 Ιστορικό Win Rate: {win_rate_hist:.1%} (Βασισμένο σε {len(pnls)} πονταρίσματα)\n")
    report_lines.append(f"💰 Αρχικό Κεφάλαιο : €{start_bankroll}")
    report_lines.append(f"📉 Risk of Ruin      : {ruin_prob:.1%} (Πιθανότητα μηδενισμού)")
    report_lines.append(f"🎯 Διάμεσο Τελικό     : €{median_final:.2f}")
    report_lines.append(f"⚠️ 5% Χειρότερο Σενάριο: €{pct_5:.2f}")
    report_lines.append(f"🚀 95% Καλύτερο Σενάριο: €{pct_95:.2f}")
    report_lines.append("=" * 60)
    if ruin_prob > 0.05:
        report_lines.append("⚠️ ΠΡΟΕΙΔΟΠΟΙΗΣΗ: Υψηλό Risk of Ruin. Σκέψου να μειώσεις το ποντάρισμα.")
    else:
        report_lines.append("✅ ΑΣΦΑΛΕΣ: Η διαχείριση κεφαλαίου σου είναι στέρεη.")

    return {"text": "\n".join(report_lines)}

@app.get("/optimizer/patterns")
def optimizer_patterns(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}

    # Φιλτράρισμα μόνο για PATTERN (ακόμα κι αν ο χρήστης επέλεξε "Όλα", εδώ δείχνουμε μόνο τα PATTERN)
    pattern_df = df[df["Type"].astype(str).str.contains("PATTERN", case=False, na=False)].copy()
    if pattern_df.empty:
        return {"text": "⚠️ Δεν βρέθηκαν PATTERN picks στο Ledger."}

    stats = {}
    for market, group in pattern_df.groupby("Market"):
        count = len(group)
        wins = group["Win_Binary"].sum()
        losses = count - wins
        win_rate = wins / count if count > 0 else 0
        total_pnl = group["PnL"].sum() if "PnL" in group.columns else 0
        avg_odds = group["Odds"].mean() if "Odds" in group.columns else 0
        stats[market] = {
            'count': count, 'wins': wins, 'losses': losses, 'win_rate': win_rate,
            'total_pnl': total_pnl, 'avg_odds': avg_odds,
        }

    total_patterns = len(pattern_df)
    total_wins = pattern_df["Win_Binary"].sum()
    overall_win_rate = total_wins / total_patterns if total_patterns > 0 else 0
    total_pnl_all = pattern_df["PnL"].sum() if "PnL" in pattern_df.columns else 0

    report_lines = []
    report_lines.append("📊 PATTERN PICKS SUMMARY\n")
    report_lines.append("=" * 60)
    report_lines.append(f"   Συνολικά PATTERN picks: {total_patterns}")
    report_lines.append(f"   Νίκες: {int(total_wins)}")
    report_lines.append(f"   Win Rate: {overall_win_rate:.1%}")
    report_lines.append(f"   Συνολικό P&L: {total_pnl_all:+.2f}€")
    report_lines.append("=" * 60 + "\n")
    report_lines.append(f"  {'Market':<15} {'Count':>6} {'Wins':>6} {'Win%':>8} {'P&L':>8} {'Avg Odds':>8}")
    report_lines.append("-" * 60)
    for market, s in sorted(stats.items(), key=lambda x: x[1]['count'], reverse=True):
        report_lines.append(f"  {market:<15} {s['count']:>6} {s['wins']:>6} {s['win_rate']:>7.1%} "
                            f"{s['total_pnl']:>+7.2f} {s['avg_odds']:>8.2f}")
    report_lines.append("-" * 60)

    return {"text": "\n".join(report_lines)}

@app.get("/optimizer/discrepancies")
def optimizer_discrepancies(type_filter: str = None):
    df = load_ledger_from_supabase()
    df = filter_by_type(df, type_filter)
    if df.empty:
        return {"text": "❌ Δεν υπάρχουν δεδομένα για ανάλυση."}

    # Φιλτράρουμε μόνο όσες γραμμές έχουν τιμή στη στήλη Discrepancy_Result
    if "Discrepancy_Result" not in df.columns:
        return {"text": "❌ Η στήλη 'Discrepancy_Result' δεν βρέθηκε στο Ledger."}

    disc_df = df[df["Discrepancy_Result"].notna() & (df["Discrepancy_Result"] != "")].copy()
    if disc_df.empty:
        return {"text": "⚠️ Δεν βρέθηκαν καταγεγραμμένες αντιφάσεις."}

    total = len(disc_df)
    model_correct = (disc_df["Discrepancy_Result"] == "MODEL_CORRECT").sum()
    detector_correct = (disc_df["Discrepancy_Result"] == "DETECTOR_CORRECT").sum()
    pushes = total - model_correct - detector_correct

    model_win_rate = model_correct / total if total > 0 else 0
    detector_win_rate = detector_correct / total if total > 0 else 0

    report_lines = []
    report_lines.append("🔍 DISCREPANCY RESOLUTION – Ποιος είχε τελικά δίκιο;\n")
    report_lines.append("=" * 60)
    report_lines.append(f"   Συνολικές αντιφάσεις: {total}")
    report_lines.append(f"   Το μοντέλο είχε δίκιο: {model_correct} φορές ({model_win_rate:.1%})")
    report_lines.append(f"   Ο ανιχνευτής είχε δίκιο: {detector_correct} φορές ({detector_win_rate:.1%})")
    if pushes > 0:
        report_lines.append(f"   Push/Άλλα: {pushes}")
    report_lines.append("=" * 60)

    if model_correct > detector_correct:
        report_lines.append("\n✅ ΤΟ ΜΟΝΤΕΛΟ ΚΕΡΔΙΖΕΙ ΣΤΙΣ ΑΝΤΙΦΑΣΕΙΣ!")
        report_lines.append("   Εμπιστεύσου το μοντέλο όταν έρχεται σε αντίθεση με τους απλούς δείκτες.")
    elif detector_correct > model_correct:
        report_lines.append("\n⚠️ Ο ΑΝΙΧΝΕΥΤΗΣ ΚΕΡΔΙΖΕΙ ΣΤΙΣ ΑΝΤΙΦΑΣΕΙΣ!")
        report_lines.append("   Οι απλοί δείκτες (λ, μ, PPG) είναι πιο αξιόπιστοι από το μοντέλο σε αυτές τις περιπτώσεις.")
    else:
        report_lines.append("\n🤝 ΙΣΟΠΑΛΙΑ ανάμεσα σε μοντέλο και ανιχνευτή.")

    return {"text": "\n".join(report_lines)}
