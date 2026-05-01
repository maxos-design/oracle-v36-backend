import pandas as pd
import requests
import json
import time
import os
import sys
from difflib import SequenceMatcher

# =====================================================================
# CONFIGURATION – reads API key from environment, falls back to default
# =====================================================================
API_KEY = os.getenv("API_FOOTBALL_KEY", "57500312c70c8c45a39faecd6942f3ea")
HEADERS = {'x-apisports-key': API_KEY}
MATCH_THRESHOLD = 1.0

# Manual name corrections
TEAM_NAME_MAP = {
    "Oxford United": "Oxford Utd",
    "Wrexham AFC": "Wrexham",
    "Norwich City": "Norwich",
    "Brighton and Hove Albion": "Brighton",
    "Leicester City": "Leicester",
    "Bournemouth": "Bournemouth",
    "Go Ahead Eagles": "Go Ahead Eagles",
    "Stockport County FC": "Stockport County",
    "Bradford City": "Bradford",
    "SonderjyskE": "Sonderjyske",
    "Sheffield United": "Sheffield Utd",
    "Olympiakos Piraeus": "Olympiacos"
}

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_fixture_id(home_team, away_team):
    h_search = TEAM_NAME_MAP.get(home_team, home_team).split()[0]
    a_search = TEAM_NAME_MAP.get(away_team, away_team)
    
    team_url = f"https://v3.football.api-sports.io/teams?search={h_search}"
    try:
        print(f"   🔍 Searching for team: '{h_search}'...")
        resp = requests.get(team_url, headers=HEADERS)
        team_data = resp.json()
        if not team_data.get('response'):
            return None
        team_id = team_data['response'][0]['team']['id']
        actual_name = team_data['response'][0]['team']['name']
        print(f"   ✅ Found '{actual_name}' (ID: {team_id})")

        fix_url = f"https://v3.football.api-sports.io/fixtures?team={team_id}&next=10"
        resp = requests.get(fix_url, headers=HEADERS)
        fix_data = resp.json()
        results = fix_data.get('response', [])
        best_match = None
        best_score = 0

        for item in results:
            api_h = item['teams']['home']['name']
            api_a = item['teams']['away']['name']
            score = similar(home_team, api_h) + similar(a_search, api_a)
            if score > best_score:
                best_score = score
                best_match = item['fixture']['id']
                found_match_name = f"{api_h} - {api_a}"

        if best_score >= MATCH_THRESHOLD:
            print(f"   🎯 Match locked: {found_match_name}")
            return best_match
    except Exception as e:
        print(f"   ❌ Search error: {e}")
    return None

def get_live_odds(fixture_id):
    url = f"https://v3.football.api-sports.io/odds?fixture={fixture_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        data = response.json()
        if not data.get('response'): return None
        bookmakers = data['response'][0]['bookmakers']
        odds_data = {}
        for bookmaker in bookmakers:
            bets = bookmaker['bets']
            for bet in bets:
                if bet['name'] == 'Match Winner':
                    for val in bet['values']:
                        if val['value'] == 'Home': odds_data['1'] = float(val['odd'])
                        elif val['value'] == 'Draw': odds_data['X'] = float(val['odd'])
                        elif val['value'] == 'Away': odds_data['2'] = float(val['odd'])
                elif bet['name'] == 'Goals Over/Under':
                    for val in bet['values']:
                        if val['value'] == 'Over 2.5': odds_data['Over_2.5'] = float(val['odd'])
                        elif val['value'] == 'Under 2.5': odds_data['Under_2.5'] = float(val['odd'])
                elif bet['name'] == 'Double Chance':
                    for val in bet['values']:
                        if val['value'] == 'Home/Draw': odds_data['1X'] = float(val['odd'])
                        elif val['value'] == 'Draw/Away': odds_data['X2'] = float(val['odd'])
                elif bet['name'] == 'Draw No Bet':
                    for val in bet['values']:
                        if val['value'] == 'Home': odds_data['DNB_1'] = float(val['odd'])
                        elif val['value'] == 'Away': odds_data['DNB_2'] = float(val['odd'])
            if len(odds_data) > 2: break 
        return odds_data
    except: pass
    return None

def run_sniper():
    # Use Analyst V6 report by default, but allow cli argument
    file_path = 'Oracle_Analyst_Report_v6.xlsx'
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    sheet = 'Top Picks'
    
    print("="*75)
    print(" 🎯 ORACLE ODDS SNIPER V2.6 — GUI Edition")
    print("="*75)
    
    try:
        df = None
        for i in range(0, 6):
            temp_df = pd.read_excel(file_path, sheet_name=sheet, header=i)
            if 'Match' in temp_df.columns:
                df = temp_df
                break
        if df is None:
            print("❌ Error: 'Match' column not found in Excel.")
            return
        df = df.dropna(subset=['Match']).reset_index(drop=True)
    except Exception as e:
        print(f"❌ Excel error: {e}")
        return

    while True:
        print("\n" + "—"*45)
        print(f" {'#':>2} | {'MATCH':<35} | {'MARKET':<8} | {'ODDS'}")
        print("—"*45)
        for idx, row in df.iterrows():
            print(f" [{idx+1:>2}] {str(row['Match'])[:35]:<35} | {row['Market']:<8} | {row['Odds']}")
        print("-" * 75)
        choice = input("Pick number (or 'q' to exit): ").strip().lower()
        if choice == 'q': break
        
        try:
            val = int(choice)
            if not (1 <= val <= len(df)):
                print("⚠️ Invalid selection.")
                continue
                
            selected = df.iloc[val-1]
            match_name = str(selected['Match'])
            old_market = str(selected['Market'])
            old_odds = float(selected['Odds'])
            
            print(f"\n📡 Connecting for: {match_name}...")
            teams = match_name.replace('–','-').split('-')
            home, away = teams[0].strip(), teams[1].strip()
            
            fix_id = find_fixture_id(home, away)
            if not fix_id:
                print(f"❌ FAILURE: Match not found in API.")
                input("\nPress Enter to continue...")
                continue
                
            live_odds = get_live_odds(fix_id)
            if not live_odds:
                print("❌ No live odds found.")
                input("\nPress Enter...")
                continue
                
            print("\n" + "⭐"*25)
            print(f" LIVE UPDATE: {match_name}")
            print("⭐"*25)
            
            new_odds = live_odds.get(old_market)
            if new_odds:
                diff = new_odds - old_odds
                trend = "🟢 STABLE"
                if diff > 0.03: trend = f"📈 RISEN (+{diff:.2f}) 🔥"
                elif diff < -0.03: trend = f"📉 DROPPED ({diff:.2f}) ⚠️"
                
                print(f"➤ Market [{old_market}]:")
                print(f"   Initial (Excel): {old_odds:.2f}")
                print(f"   Live (Now)      : {new_odds:.2f}  {trend}")
            else:
                print(f"⚠️ Market {old_market} is closed.")

            print("\nOther odds:")
            for m, v in live_odds.items():
                if m != old_market: print(f"   {m:<10}: {v}")
            
            print("="*55)
            input("\nPress Enter for list...")
                    
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_sniper()
