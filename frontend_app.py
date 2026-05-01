import streamlit as st
import requests
import pandas as pd

BACKEND_URL = "https://oracle-v36-backend.onrender.com"

st.set_page_config(page_title="Oracle Suite V36", page_icon="⚽", layout="wide")
st.sidebar.title("ORACLE SUITE V36")
st.sidebar.markdown("V36 · Cloud Edition")
menu = st.sidebar.radio("Μενού", ["🏠 Dashboard", "📜 Ιστορικό Ledger", "🎮 Control Panel", "🏆 Top Picks", "📄 Enterprise Picks"])

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
        st.success("Συνδέθηκε επιτυχώς με τον Cloud Server.")
    except:
        st.error("Αποτυχία σύνδεσης με το backend.")

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
    st.markdown("---")
    st.markdown('<p class="section-title">☁️ ΣΥΓΧΡΟΝΙΣΜΟΣ CLOUD</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📤 Upload All Reports", use_container_width=True):
            with st.spinner("Ανέβασμα Ledger, Enterprise και Top Picks στη Supabase..."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/upload-all", timeout=600)
                    if resp.status_code == 200:
                        st.success("✅ Όλα τα δεδομένα ανέβηκαν επιτυχώς! Δες τα στο κινητό.")
                    else:
                        st.error(f"❌ Σφάλμα: {resp.text}")
                except:
                    st.error("❌ Αποτυχία σύνδεσης με τον server.")
    with col2:
        if st.button("📤 Upload Ledger Only", use_container_width=True):
            with st.spinner("Ανέβασμα του Ledger..."):
                try:
                    resp = requests.post(f"{BACKEND_URL}/run-migration", timeout=300)
                    if resp.status_code == 200:
                        st.success("✅ Το Ledger ανέβηκε επιτυχώς!")
                    else:
                        st.error(f"❌ Σφάλμα: {resp.text}")
                except:
                    st.error("❌ Αποτυχία σύνδεσης με τον server.")

    st.markdown("---")
    st.markdown('<p class="section-title">📡 MODEL PIPELINE (Διαθέσιμα μόνο από τον υπολογιστή)</p>', unsafe_allow_html=True)
    
    col3, col4 = st.columns(2)
    with col3:
        st.button("🔍 Τρέξε τον Scanner", disabled=True, help="Διαθέσιμο μόνο από τον υπολογιστή σου (Oracle Suite).")
    with col4:
        st.button("🧠 Τρέξε τον Analyst", disabled=True, help="Διαθέσιμο μόνο από τον υπολογιστή σου (Oracle Suite).")

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
            st.info("Δεν υπάρχουν ακόμα Top Picks. Ανέβασε τα δεδομένα από τον υπολογιστή ή πάτα 'Upload All Reports'.")
    except:
        st.error("Σφάλμα φόρτωσης δεδομένων.")

# ───────────── Enterprise Picks ─────────────
elif menu == "📄 Enterprise Picks":
    st.title("📄 All Enterprise Picks")
    try:
        response = requests.get(f"{BACKEND_URL}/enterprise-picks?limit=500")
        data = response.json()
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df[['date','time','league','match','market','odds','stat_pct','ev']], use_container_width=True)
        else:
            st.info("Δεν υπάρχουν ακόμα picks. Ανέβασε τα δεδομένα από τον υπολογιστή.")
    except:
        st.error("Σφάλμα φόρτωσης δεδομένων.")
