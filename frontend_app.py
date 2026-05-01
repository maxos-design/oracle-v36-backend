import streamlit as st
import requests
import pandas as pd

BACKEND_URL = "https://oracle-v36-backend.onrender.com"

st.set_page_config(page_title="Oracle Suite V36", page_icon="⚽", layout="wide")
st.sidebar.title("ORACLE SUITE V36")
menu = st.sidebar.radio("Μενού", ["🏠 Dashboard", "📜 Ιστορικό Ledger", "🎮 Control Panel", "🏆 Top Picks"])

# ───────────── Dashboard ─────────────
if menu == "🏠 Dashboard":
    st.title("🏠 Dashboard")
    try:
        response = requests.get(f"{BACKEND_URL}/stats")
        stats = response.json()
        col1, col2, col3 = st.columns(3)
        col1.metric("Στοιχήματα", stats.get('total_bets', 0))
        col2.metric("Συνολικό PnL", f"€{stats.get('total_pnl', 0.0):.2f}")
        col3.metric("Win Rate", f"{stats.get('win_rate', 0.0):.1f}%")
    except:
        st.error("Δεν μπορώ να συνδεθώ με το backend.")

# ───────────── Ιστορικό Ledger ─────────────
elif menu == "📜 Ιστορικό Ledger":
    st.title("📜 Ιστορικό Ledger")
    try:
        response = requests.get(f"{BACKEND_URL}/ledger?limit=200")
        df = pd.DataFrame(response.json())
        if not df.empty:
            st.dataframe(df[['date','match','market','odds','result','pnl']], use_container_width=True)
            df['pnl'] = pd.to_numeric(df['pnl'])
            df['cumulative_pnl'] = df['pnl'].cumsum()
            st.subheader("Γράφημα Κέρδους")
            st.line_chart(df.set_index('date')['cumulative_pnl'])
    except:
        st.error("Αδυναμία φόρτωσης δεδομένων.")

# ───────────── Control Panel ─────────────
elif menu == "🎮 Control Panel":
    st.title("🎮 Control Panel")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔍 Τρέξε τον Scanner", use_container_width=True):
            with st.spinner("Ο Scanner εκτελείται... Μπορεί να πάρει λίγη ώρα."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/run-scanner")
                    if resp.status_code == 200:
                        st.success(resp.json().get("message", "Ο Scanner ολοκληρώθηκε!"))
                    else:
                        st.error(f"Σφάλμα: {resp.text}")
                except:
                    st.error("Αποτυχία σύνδεσης με τον server.")
    with col2:
        if st.button("🧠 Τρέξε τον Analyst", use_container_width=True):
            with st.spinner("Ο Analyst αναλύει τα δεδομένα..."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/run-analyst")
                    if resp.status_code == 200:
                        st.success(resp.json().get("message", "Ο Analyst ολοκληρώθηκε!"))
                    else:
                        st.error(f"Σφάλμα: {resp.text}")
                except:
                    st.error("Αποτυχία σύνδεσης με τον server.")

# ───────────── Top Picks ─────────────
elif menu == "🏆 Top Picks":
    st.title("🏆 Top 15 Picks")
    try:
        response = requests.get(f"{BACKEND_URL}/top-picks")
        data = response.json()
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df[['date','time','league','match','market','odds','score','grade']], use_container_width=True)
        else:
            st.info("Δεν υπάρχουν ακόμα Top Picks. Τρέξε τον Analyst πρώτα.")
    except:
        st.error("Σφάλμα φόρτωσης δεδομένων.")
