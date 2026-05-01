import sys
import os
import time as _time_module
import argparse
import re
import json
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import pandas as pd
import requests
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# =====================================================================
# CONFIGURATION
# =====================================================================
API_FOOTBALL_KEY = "57500312c70c8c45a39faecd6942f3ea"
HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}
APF_BASE = "https://v3.football.api-sports.io"
REQUEST_DELAY = 0.6

LEDGER_FILE = "Oracle_Historical_Ledger.xlsx"
PENDING_FILE = "pending_picks.json"

FIXTURES_LAST = 50
FIXTURES_NEXT = 20
LOOKBACK_DAYS = 5
LOOKAHEAD_DAYS = 5
MATCH_THRESHOLD = 0.70

TEAM_NAME_MAP = {
    "Olympiakos Piraeus": "Olympiacos",
    "Olympiakos": "Olympiacos",
    "Panathinaikos": "Panathinaikos",
    "Basaksehir": "Istanbul Basaksehir",
    "Trabzonspor": "Trabzonspor",
    "Borussia Monchengladbach": "Borussia Mönchengladbach",
    "Borussia M'gladbach": "Borussia Mönchengladbach",
    "FSV Mainz 05": "Mainz 05",
    "Mainz": "Mainz 05",
    "Albacete": "Albacete",
    "Granada CF": "Granada",
    "Empoli": "Empoli",
    "Virtus Entella": "Entella",
    "Sporting Lisbon": "Sporting CP",
    "Benfica": "Benfica",
    "USL Dunkerque": "Dunkerque",
    "Stade Lavallois": "Laval",
    "Paris Saint Germain": "Paris Saint-Germain",
    "PSG": "Paris Saint-Germain",
    "Lyon": "Olympique Lyonnais",
    "Crystal Palace": "Crystal Palace",
    "West Ham United": "West Ham",
    "FC Midtjylland": "Midtjylland",
    "AGF Aarhus": "AGF",
}

# --- Load Discrepancies ---
DISCREPANCY_FILE = "discrepancies.json"
discrepancy_map = {}
if os.path.exists(DISCREPANCY_FILE):
    with open(DISCREPANCY_FILE, "r", encoding="utf-8") as f:
        disc_list = json.load(f)
    for d in disc_list:
        key = (d["match"], d["market"])
        discrepancy_map[key] = d["discrepancy_type"]

# =====================================================================
# 1. HELPERS
# =====================================================================
def _apf(endpoint, params=None):
    _time_module.sleep(REQUEST_DELAY)
    try:
        r = requests.get(f"{APF_BASE}{endpoint}", headers=HEADERS, params=params or {}, timeout=12)
        return r.json()
    except Exception as e:
        print(f"   ⚠️ API error {endpoint}: {e}")
        return {}

def _similar(a, b):
    a = re.sub(r'[^\w\s]', '', a.lower().strip())
    b = re.sub(r'[^\w\s]', '', b.lower().strip())
    return SequenceMatcher(None, a, b).ratio()

def normalize_team_name(name):
    name = name.strip()
    return TEAM_NAME_MAP.get(name, name)

# ── GLOBAL SAFE FLOAT CONVERTER ──────────────────────────────────────
def _safe_float_val(val):
    if val is None or val == '' or val is False:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

# =====================================================================
# 2. TEAM ID CACHE
# =====================================================================
_team_cache = {}

def get_team_id(team_name):
    if team_name in _team_cache:
        return _team_cache[team_name]

    search_name = normalize_team_name(team_name)
    words = search_name.split()
    search_candidates = [search_name, words[0]]
    if len(words) > 1:
        search_candidates.append(words[1])
    if team_name != search_name:
        search_candidates.append(team_name.split()[0])

    best_id = None
    best_score = 0.0

    for term in search_candidates:
        if len(term) < 3:
            continue
        data = _apf("/teams", {"search": term})
        for item in data.get("response", []):
            api_name = item["team"]["name"]
            score = max(_similar(search_name, api_name), _similar(team_name, api_name))
            if score > best_score:
                best_score = score
                best_id = item["team"]["id"]
        if best_score >= 0.85:
            break

    if best_id and best_score >= MATCH_THRESHOLD:
        _team_cache[team_name] = best_id
        return best_id

    print(f"   ⚠️  Team not found: '{team_name}' (normalized: '{search_name}', best={best_score:.2f})")
    return None

# =====================================================================
# 3. FIXTURE FINDER (no future fixtures)
# =====================================================================
def _is_future_fixture(item) -> bool:
    try:
        match_date = datetime.fromisoformat(item["fixture"]["date"].replace('Z', '+00:00'))
        return match_date > datetime.now(timezone.utc)
    except:
        return False

def _days_from_today(date_str: str) -> int:
    if not date_str or date_str in ("nan", "None", ""):
        return 0
    try:
        day, month = date_str.strip().split("/")
        year = datetime.now().year
        target = datetime(year, int(month), int(day))
        return (target.date() - datetime.now().date()).days
    except Exception:
        return 0

def _get_date_range_from_pick(date_str: str):
    if not date_str or date_str in ("nan", "None", ""):
        return None, None
    try:
        day, month = date_str.strip().split("/")
        year = datetime.now().year
        target = datetime(year, int(month), int(day))
        from_date = (target - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        to_date = (target + timedelta(days=LOOKAHEAD_DAYS)).strftime("%Y-%m-%d")
        return from_date, to_date
    except Exception:
        return None, None

def find_fixture_id(home: str, away: str, match_date_str: str = "") -> tuple:
    home_id = get_team_id(home)
    away_id = get_team_id(away)

    norm_home = normalize_team_name(home)
    norm_away = normalize_team_name(away)

    def _is_match(item) -> bool:
        fh = item["teams"]["home"]["id"]
        fa = item["teams"]["away"]["id"]
        if home_id and away_id:
            return (fh == home_id and fa == away_id) or (fh == away_id and fa == home_id)
        api_h = item["teams"]["home"]["name"]
        api_a = item["teams"]["away"]["name"]
        score = _similar(norm_home, api_h) + _similar(norm_away, api_a)
        return score >= MATCH_THRESHOLD * 2

    def _name(item) -> str:
        return f"{item['teams']['home']['name']} vs {item['teams']['away']['name']}"

    # Strategy 1: date range
    from_date, to_date = _get_date_range_from_pick(match_date_str)
    if from_date and to_date:
        data = _apf("/fixtures", {"from": from_date, "to": to_date})
        for item in data.get("response", []):
            if not _is_future_fixture(item) and _is_match(item):
                return item["fixture"]["id"], _name(item)

    # Strategy 2: last/next
    days_diff = _days_from_today(match_date_str)
    use_next = days_diff > 0

    def _search_team(team_id: int, use_future: bool):
        if not team_id:
            return []
        params = {"team": team_id}
        if use_future:
            params["next"] = FIXTURES_NEXT
        else:
            params["last"] = FIXTURES_LAST
        data = _apf("/fixtures", params)
        return data.get("response", [])

    if home_id:
        for item in _search_team(home_id, use_next):
            if not _is_future_fixture(item) and _is_match(item):
                return item["fixture"]["id"], _name(item)
        if not use_next:
            for item in _search_team(home_id, True):
                if not _is_future_fixture(item) and _is_match(item):
                    return item["fixture"]["id"], _name(item)

    if away_id:
        for item in _search_team(away_id, use_next):
            if not _is_future_fixture(item) and _is_match(item):
                return item["fixture"]["id"], _name(item)
        if not use_next:
            for item in _search_team(away_id, True):
                if not _is_future_fixture(item) and _is_match(item):
                    return item["fixture"]["id"], _name(item)

    # Strategy 3: by name
    search_term = norm_home.split()[0]
    data = _apf("/fixtures", {"search": search_term})
    best_id, best_score, best_name = None, 0.0, ""
    for item in data.get("response", []):
        if _is_future_fixture(item):
            continue
        api_h = item["teams"]["home"]["name"]
        api_a = item["teams"]["away"]["name"]
        score = _similar(norm_home, api_h) + _similar(norm_away, api_a)
        if score > best_score:
            best_score = score
            best_id = item["fixture"]["id"]
            best_name = _name(item)
    if best_score >= MATCH_THRESHOLD * 2:
        return best_id, best_name

    print(f"   ⚠️  Not found (or only future fixtures) | home_id={home_id} away_id={away_id} | days_diff={days_diff:+d}")
    return None, ""

# =====================================================================
# 4. MATCH DATA FETCHER
# =====================================================================
def _safe_int(val, default=0):
    try: return int(val) if val is not None else default
    except: return default

def _safe_float(val, default=0.0):
    try: return float(val) if val is not None else default
    except: return default

def get_match_data(fixture_id):
    data = _apf("/fixtures", {"id": fixture_id})
    if not data.get("response"):
        return None
    m = data["response"][0]
    status = m["fixture"]["status"]["short"]
    if status not in ("FT", "AET", "PEN"):
        return "PENDING"
    home_g = _safe_int(m["goals"]["home"])
    away_g = _safe_int(m["goals"]["away"])
    total_g = home_g + away_g
    stat_data = _apf("/fixtures/statistics", {"fixture": fixture_id})
    home_stats, away_stats = {}, {}
    for ts in stat_data.get("response", []):
        is_home = ts["team"]["id"] == m["teams"]["home"]["id"]
        target = home_stats if is_home else away_stats
        for s in ts.get("statistics", []):
            if s["value"] is not None:
                target[s["type"]] = s["value"]
    evt_data = _apf("/fixtures/events", {"fixture": fixture_id})
    red_cards = 0
    penalties = 0
    for e in evt_data.get("response", []):
        if e.get("type") == "Card" and "Red" in str(e.get("detail", "")):
            red_cards += 1
        if e.get("type") == "Goal" and e.get("detail") == "Penalty":
            penalties += 1
    home_corners = _safe_int(home_stats.get("Corner Kicks"))
    away_corners = _safe_int(away_stats.get("Corner Kicks"))
    total_corners = home_corners + away_corners
    home_yellow = _safe_int(home_stats.get("Yellow Cards"))
    away_yellow = _safe_int(away_stats.get("Yellow Cards"))
    total_cards = home_yellow + away_yellow + red_cards
    home_xg = _safe_float(home_stats.get("expected_goals"))
    away_xg = _safe_float(away_stats.get("expected_goals"))
    home_sot = _safe_int(home_stats.get("Shots on Goal"))
    away_sot = _safe_int(away_stats.get("Shots on Goal"))
    home_poss = str(home_stats.get("Ball Possession", "50%"))
    away_poss = str(away_stats.get("Ball Possession", "50%"))
    btts = home_g >= 1 and away_g >= 1
    return {
        "home_goals": home_g, "away_goals": away_g, "total_goals": total_g,
        "score": f"{home_g}-{away_g}", "home_sot": home_sot, "away_sot": away_sot,
        "home_xg": home_xg, "away_xg": away_xg, "total_xg": round(home_xg + away_xg, 2),
        "possession": f"{home_poss} - {away_poss}", "home_corners": home_corners,
        "away_corners": away_corners, "total_corners": total_corners,
        "total_cards": total_cards, "red_cards": red_cards, "penalties": penalties,
        "btts": btts, "status": status,
    }

# =====================================================================
# 5. VERDICT ENGINE
# =====================================================================
def compute_verdict(market, d):
    hg, ag, tg = d["home_goals"], d["away_goals"], d["total_goals"]
    hxg, axg = d["home_xg"], d["away_xg"]
    rc, btts, pens = d["red_cards"], d["btts"], d["penalties"]
    won = False
    if market == "1": won = hg > ag
    elif market == "X": won = hg == ag
    elif market == "2": won = ag > hg
    elif market == "1X": won = hg >= ag
    elif market == "X2": won = ag >= hg
    elif market == "DNB_1": won = hg > ag
    elif market == "DNB_2": won = ag > hg
    elif market == "Over_2.5": won = tg > 2
    elif market == "Under_2.5": won = tg < 3
    elif market == "BTTS": won = btts
    elif market == "1_Over": won = (hg > ag and tg > 2)
    elif market == "2_Over": won = (ag > hg and tg > 2)
    else: return "PUSH", "PUSH", "Manual"
    if market in ("DNB_1","DNB_2") and hg == ag:
        return "PUSH", "PUSH", "Draw"
    if won:
        return "WIN", "CORRECT", ""
    if rc > 0:
        return "LOSS", "EXT_FACTOR", f"Red cards ({rc})"
    if pens > 0 and abs(hg - ag) == 1:
        return "LOSS", "EXT_FACTOR", f"Penalty ({pens})"
    if (market in ("1","1X","DNB_1","1_Over") and hxg > axg + 0.5 and hxg >= 1.0) or \
       (market in ("2","X2","DNB_2","2_Over") and axg > hxg + 0.5 and axg >= 1.0) or \
       (market == "Over_2.5" and d["total_xg"] > 3.0) or \
       (market == "Under_2.5" and d["total_xg"] < 2.0 and tg > 2) or \
       (market == "BTTS" and hxg >= 0.8 and axg >= 0.8 and not btts):
        return "LOSS", "BAD_BEAT", ""
    return "LOSS", "MODEL_ERROR", ""

# =====================================================================
# 6. LEDGER
# =====================================================================
LEDGER_COLS = [
    "Timestamp","Match","Date","League","Market","Odds","Type","Stat_Pct","EV",
    "Score","Home_Goals","Away_Goals","Total_Goals",
    "Home_xG","Away_xG","Total_xG","Home_SOT","Away_SOT",
    "Total_Corners","Total_Cards","Red_Cards","Penalties","BTTS",
    "Result","Verdict_Code","Explanation","PnL",
    "λ (Lambda)", "μ (Mu)", "Home_Adv", "H_PPG", "A_PPG", "Discrepancy_Result"
]

def load_ledger() -> pd.DataFrame:
    if os.path.exists(LEDGER_FILE):
        try:
            df = pd.read_excel(LEDGER_FILE, engine='openpyxl')
            for col in LEDGER_COLS:
                if col not in df.columns:
                    df[col] = None
            numeric_cols = ["Odds","EV","Home_Goals","Away_Goals","Total_Goals",
                            "Home_xG","Away_xG","Total_xG","Home_SOT","Away_SOT",
                            "Total_Corners","Total_Cards","Red_Cards","Penalties",
                            "λ (Lambda)","μ (Mu)","Home_Adv","H_PPG","A_PPG"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except Exception as e:
            print(f"   ⚠️ Warning: Could not load ledger Excel: {e}")
            return pd.DataFrame(columns=LEDGER_COLS)
    return pd.DataFrame(columns=LEDGER_COLS)

def save_to_ledger(row: dict) -> None:
    df = load_ledger()
    df = df.astype(object)
    new = pd.DataFrame([row]).astype(object)
    key = f"{row['Match']}|{row['Market']}"
    existing_keys = (df["Match"].astype(str) + "|" + df["Market"].astype(str)).tolist() if not df.empty else []
    if key in existing_keys:
        idx = existing_keys.index(key)
        for col in new.columns:
            if col in df.columns:
                df.at[idx, col] = row.get(col)
    else:
        df = pd.concat([df, new], ignore_index=True)
    numeric_cols = ["Odds","EV","Home_Goals","Away_Goals","Total_Goals",
                    "Home_xG","Away_xG","Total_xG","Home_SOT","Away_SOT",
                    "Total_Corners","Total_Cards","Red_Cards","Penalties",
                    "λ (Lambda)","μ (Mu)","Home_Adv","H_PPG","A_PPG"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    try:
        with pd.ExcelWriter(LEDGER_FILE, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, sheet_name="Ledger", index=False)
        print("   💾 Saved to Excel ledger")
    except Exception as e:
        print(f"   ❌ Failed to save ledger: {e}")

def build_ledger_row(pick_row, match_data, result, verdict_code, explanation, disc_result=None):
    stake = 10.0
    odds = float(pick_row.get("Odds", 1.0))
    pnl = round(stake * (odds - 1), 2) if result == "WIN" else (-stake if result == "LOSS" else 0.0)
    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Match": str(pick_row.get("Match", "")),
        "Date": str(pick_row.get("Date", "")),
        "League": str(pick_row.get("League", "Unknown")),
        "Market": str(pick_row.get("Market", "")),
        "Odds": odds,
        "Type": str(pick_row.get("Type", "")),
        "Stat_Pct": str(pick_row.get("Stat %", "")),
        "EV": float(pick_row.get("EV", 1.0)),
        "Score": match_data["score"],
        "Home_Goals": match_data["home_goals"],
        "Away_Goals": match_data["away_goals"],
        "Total_Goals": match_data["total_goals"],
        "Home_xG": match_data["home_xg"],
        "Away_xG": match_data["away_xg"],
        "Total_xG": match_data["total_xg"],
        "Home_SOT": match_data["home_sot"],
        "Away_SOT": match_data["away_sot"],
        "Total_Corners": match_data["total_corners"],
        "Total_Cards": match_data["total_cards"],
        "Red_Cards": match_data["red_cards"],
        "Penalties": match_data["penalties"],
        "BTTS": match_data["btts"],
        "Result": result,
        "Verdict_Code": verdict_code,
        "Explanation": explanation,
        "PnL": pnl,
        "λ (Lambda)": _safe_float_val(pick_row.get("λ")),
        "μ (Mu)": _safe_float_val(pick_row.get("μ")),
        "Home_Adv": _safe_float_val(pick_row.get("Home Adv")),
        "H_PPG": _safe_float_val(pick_row.get("H PPG")),
        "A_PPG": _safe_float_val(pick_row.get("A PPG")),
        "Discrepancy_Result": disc_result,
    }

# =====================================================================
# 7. EXCEL REPORT LOADER (supports sheet override)
# =====================================================================
def load_oracle_report(path="Oracle_Analyst_Report_v6.xlsx", sheet_override=None):
    if not os.path.exists(path):
        for fname in ["Oracle_Analyst_Report_v6.xlsx", "Oracle_V35.xlsx"]:
            if os.path.exists(fname):
                path = fname
                print(f"   📂 Using report: {path}")
                break
        else:
            raise FileNotFoundError(f"Missing: {path}")
    sheets_to_try = [sheet_override] if sheet_override else ["Top Picks", "🏆 Top Picks", "Picks"]
    for sheet in sheets_to_try:
        for skip in (3,2,1,0):
            try:
                df = pd.read_excel(path, sheet_name=sheet, header=skip)
                df.columns = [str(c).strip() for c in df.columns]
                if "Match" in df.columns and "Market" in df.columns:
                    df = df.dropna(subset=["Match"])
                    df = df[df["Match"].astype(str).str.contains(" – | vs ", na=False)]
                    if not df.empty:
                        print(f"   ✅ Loaded {len(df)} picks from {sheet}")
                        return df.reset_index(drop=True)
            except:
                continue
    raise ValueError("No valid picks found in Excel.")

def parse_teams(match_str):
    for sep in (" – ", " - ", " vs ", "–", "-"):
        if sep in match_str:
            parts = match_str.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    return match_str.strip(), ""

# =====================================================================
# 8. PENDING PICKS
# =====================================================================
def load_pending_picks():
    if os.path.exists(PENDING_FILE):
        try:
            with open(PENDING_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_pending_picks(pending):
    with open(PENDING_FILE, 'w', encoding='utf-8') as f:
        json.dump(pending, f, indent=2, ensure_ascii=False)

def add_pending_pick(pick_info):
    pending = load_pending_picks()
    if not any(p['Match'] == pick_info['Match'] and p['Market'] == pick_info['Market'] for p in pending):
        pending.append(pick_info)
        save_pending_picks(pending)
        return True
    return False

def remove_pending_pick(match, market):
    pending = load_pending_picks()
    pending = [p for p in pending if not (p['Match'] == match and p['Market'] == market)]
    save_pending_picks(pending)

# =====================================================================
# 9. SINGLE AUTOPSY
# =====================================================================
def run_single_autopsy(pick_row, verbose=True, allow_pending=True):
    match_str = str(pick_row.get("Match", ""))
    market = str(pick_row.get("Market", ""))
    date_str = str(pick_row.get("Date", ""))
    time_str = str(pick_row.get("Time", ""))
    home, away = parse_teams(match_str)
    if not home or not away:
        if verbose:
            print(f"   ⚠️ Cannot parse match: {match_str}")
        return None

    # --- Time check ---
    try:
        day, month = date_str.strip().split("/")
        year = datetime.now().year
        hour = minute = 0
        if time_str and ":" in time_str:
            hour, minute = map(int, time_str.split(":")[:2])
        utc_offset_hours = _time_module.localtime().tm_gmtoff // 3600
        match_dt_local = datetime(year, int(month), int(day), hour, minute, 0)
        match_dt_utc = match_dt_local - timedelta(hours=utc_offset_hours)
        match_dt_utc = match_dt_utc.replace(tzinfo=timezone.utc)
        if match_dt_utc > datetime.now(timezone.utc):
            if verbose:
                print(f"   ⏳ Match has not started yet ({date_str} {time_str}). Skipping.")
            return None
    except Exception:
        try:
            day, month = date_str.strip().split("/")
            year = datetime.now().year
            match_date = datetime(year, int(month), int(day)).date()
            if match_date > datetime.now(timezone.utc).date():
                if verbose:
                    print(f"   ⏳ Match date is in the future ({date_str}). Skipping.")
                return None
        except:
            pass

    if verbose:
        print(f"\n   🔍 {match_str} [{market}]")
        lam = pick_row.get("λ")
        mu = pick_row.get("μ")
        ha = pick_row.get("Home Adv")
        h_ppg = pick_row.get("H PPG")
        a_ppg = pick_row.get("A PPG")
        if lam is not None or mu is not None:
            print(f"   🧠 Model: λ={lam} μ={mu}  HA={ha}  H_PPG={h_ppg}  A_PPG={a_ppg}")
        print(f"   📡 Searching API...")

    fix_id, api_name = find_fixture_id(home, away, date_str)
    if not fix_id:
        if verbose:
            print(f"   ❌ Fixture not found.")
        if allow_pending:
            pick_info = {
                "Match": match_str,
                "Market": market,
                "Date": date_str,
                "Time": time_str,
                "Odds": float(pick_row.get("Odds", 1.0)),
                "Type": str(pick_row.get("Type", "")),
                "Stat %": str(pick_row.get("Stat %", "")),
                "EV": float(pick_row.get("EV", 1.0)),
                "League": str(pick_row.get("League", "Unknown")),
                "λ": _safe_float_val(pick_row.get("λ")),
                "μ": _safe_float_val(pick_row.get("μ")),
                "Home Adv": _safe_float_val(pick_row.get("Home Adv")),
                "H PPG": _safe_float_val(pick_row.get("H PPG")),
                "A PPG": _safe_float_val(pick_row.get("A PPG")),
                "pending_since": datetime.now().isoformat()
            }
            if add_pending_pick(pick_info):
                print(f"   📌 Added to pending list. Will retry later.")
        return None

    if verbose and api_name:
        print(f"   ✅ Matched: {api_name}")
    match_data = get_match_data(fix_id)
    if match_data == "PENDING":
        if verbose:
            print(f"   ⏳ Match not finished yet.")
        return None
    if not match_data:
        if verbose:
            print(f"   ❌ No data available.")
        return None

    result, verdict_code, explanation = compute_verdict(market, match_data)

    # --- Discrepancy Result (νέο) ---
    disc_result = None
    disc_key = (match_str, market)
    if disc_key in discrepancy_map:
        disc_type = discrepancy_map[disc_key]
        if result == "WIN":
            disc_result = "MODEL_CORRECT"
        elif result == "LOSS":
            disc_result = "DETECTOR_CORRECT"
        else:
            disc_result = "PUSH"

    row = build_ledger_row(pick_row, match_data, result, verdict_code, explanation, disc_result)
    if verbose:
        result_icon = {"WIN": "✅", "LOSS": "❌", "PUSH": "↩️"}.get(result, "❓")
        print(f"\n   {'▬'*58}")
        print(f"   📋 AUTOPSY: {match_str}")
        print(f"   {'▬'*58}")
        print(f"   Prediction  : [{market}] @ {pick_row.get('Odds','')}")
        print(f"   Result      : {result_icon} {result}")
        print(f"   Final Score : {match_data['score']}")
        print(f"   xG          : {match_data['home_xg']:.2f} – {match_data['away_xg']:.2f}  (total: {match_data['total_xg']:.2f})")
        print(f"   Shots on Tgt: {match_data['home_sot']} – {match_data['away_sot']}")
        print(f"   Possession  : {match_data['possession']}")
        print(f"   Corners     : {match_data['home_corners']} – {match_data['away_corners']}  (total: {match_data['total_corners']})")
        print(f"   Cards       : {match_data['total_cards']} total  ({match_data['red_cards']} red)")
        if match_data["penalties"]:
            print(f"   ⚠️ Penalties : {match_data['penalties']}")
        print(f"\n   💡 Verdict  : {verdict_code}")
        print(f"   📝 Detail   : {explanation}")
        stake, odds = 10.0, float(pick_row.get("Odds", 1.0))
        pnl = round(stake * (odds - 1), 2) if result == "WIN" else (-stake if result == "LOSS" else 0.0)
        print(f"   💶 P&L (€10): {pnl:+.2f}€")
    save_to_ledger(row)
    remove_pending_pick(match_str, market)
    return row

# =====================================================================
# 10. PROCESS PENDING
# =====================================================================
def process_pending_picks():
    pending = load_pending_picks()
    if not pending:
        return
    print(f"\n🔄 Checking {len(pending)} pending picks...")
    updated = 0
    still_pending = []
    for pick in pending:
        row = pd.Series(pick)
        result = run_single_autopsy(row, verbose=True, allow_pending=False)
        if result is not None:
            updated += 1
        else:
            still_pending.append(pick)
    if updated > 0:
        print(f"   ✅ Updated {updated} picks. Still pending: {len(still_pending)}")
    else:
        print(f"   ⏳ All {len(still_pending)} picks still pending (API delay).")

# =====================================================================
# 11. BATCH MODE (accepts sheet_override)
# =====================================================================
def run_batch_autopsy(report_path="Oracle_Analyst_Report_v6.xlsx", sheet_override=None):
    process_pending_picks()
    print("=" * 70)
    print("  BATCH AUTOPSY V2.8 (Pending Picks)")
    print("=" * 70)
    df = load_oracle_report(report_path, sheet_override=sheet_override)
    total = len(df)
    wins = losses = pushes = skipped = 0
    total_pnl = 0.0
    for i, (_, row) in enumerate(df.iterrows(), 1):
        print(f"\n[{i}/{total}] {row.get('Match','')} [{row.get('Market','')}]")
        result_row = run_single_autopsy(row, verbose=True, allow_pending=True)
        if result_row is None:
            skipped += 1
            continue
        r = result_row["Result"]
        if r == "WIN":
            wins += 1
        elif r == "LOSS":
            losses += 1
        else:
            pushes += 1
        total_pnl += result_row["PnL"]
    settled = wins + losses + pushes
    win_rate = wins / settled * 100 if settled else 0
    print(f"\n{'='*70}")
    print(f"  BATCH SUMMARY")
    print(f"{'='*70}")
    print(f"  Total picks   : {total}")
    print(f"  Settled       : {settled}  (Skipped/Pending: {skipped})")
    print(f"  Wins          : {wins}")
    print(f"  Losses        : {losses}")
    print(f"  Pushes        : {pushes}")
    print(f"  Win Rate      : {win_rate:.1f}%")
    print(f"  Total P&L     : {total_pnl:+.2f}€  (€10/pick)")
    if settled:
        print(f"  ROI           : {total_pnl / (settled * 10) * 100:.1f}%")

# =====================================================================
# 12. INTERACTIVE MODE (accepts sheet_override)
# =====================================================================
def run_interactive(report_path="Oracle_Analyst_Report_v6.xlsx", sheet_override=None):
    process_pending_picks()
    print("=" * 70)
    print("  ORACLE POST-MATCH AUTOPSY V2.8 (Pending Picks)")
    print("=" * 70)
    if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        report_path = sys.argv[2]
    try:
        df = load_oracle_report(report_path, sheet_override=sheet_override)
    except Exception as e:
        print(f"❌ {e}")
        return
    print("\n   Picks available:")
    for idx, (_, row) in enumerate(df.iterrows()):
        print(f"   [{idx+1:>2}] {str(row.get('Match',''))[:40]:<40} [{str(row.get('Market','')):<10}] @ {row.get('Odds','?')}")
    print(f"\n   Enter pick number(s) separated by commas (e.g. 1,3,5) or 'all':")
    choice = input("   > ").strip().lower()
    if choice == "all":
        run_batch_autopsy(report_path, sheet_override)
        return
    numbers = [x.strip() for x in choice.split(",") if x.strip()]
    if not numbers:
        print("   ❌ No valid numbers entered.")
        return
    for num_str in numbers:
        try:
            idx = int(num_str) - 1
            if 0 <= idx < len(df):
                print(f"\n--- Processing pick {idx+1} ---")
                run_single_autopsy(df.iloc[idx], verbose=True, allow_pending=True)
            else:
                print(f"   ❌ {num_str} is out of range (1-{len(df)}).")
        except ValueError:
            print(f"   ❌ '{num_str}' is not a valid number.")

# =====================================================================
# MAIN
# =====================================================================
def main():
    print("✅ Autopsy V2.8 – Pending Picks + Sheet Support")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--report-file", default="Oracle_Analyst_Report_v6.xlsx")
    parser.add_argument("--sheet", default=None, help="Sheet name to read (e.g. 'Picks')")
    args = parser.parse_args()
    if args.batch:
        run_batch_autopsy(args.report_file, sheet_override=args.sheet)
    elif args.report:
        print("ROI report not included in this quick version.")
    elif args.export:
        print("Excel export not included in this quick version.")
    else:
        run_interactive()

if __name__ == "__main__":
    main()
