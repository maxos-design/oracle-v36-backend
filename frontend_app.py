import streamlit as st
import requests
import pandas as pd

BACKEND_URL = "https://oracle-v36-backend.onrender.com"

st.set_page_config(page_title="Oracle Suite V36", page_icon="⚽", layout="wide")

# ───────────── SIDEBAR ─────────────
st.sidebar.title("ORACLE SUITE V36")
st.sidebar.markdown("V36 · Cloud Edition")

menu = st.sidebar.radio("Μενού", [
    "🏠 Dashboard",
    "📜 Ιστορικό Ledger",
    "🎮 Control Panel",
    "🏆 Top Picks",
    "📄 Enterprise Picks",
    "📊 Optimizer / Data Lab"   # <-- ΝΕΑ ΕΠΙΛΟΓΗ
])

# ───────────── DASHBOARD ─────────────
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

# ───────────── ΙΣΤΟΡΙΚΟ LEDGER ─────────────
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

# ───────────── CONTROL PANEL ─────────────
elif menu == "🎮 Control Panel":
    st.title("🎮 Control Panel")
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

# ───────────── TOP PICKS ─────────────
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

# ───────────── ENTERPRISE PICKS ─────────────
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

# ───────────── OPTIMIZER / DATA LAB (ΝΕΟ) ─────────────
elif menu == "📊 Optimizer / Data Lab":
    st.title("📊 Optimizer / Data Lab")
    
    # Φιλτράρισμα ανά τύπο
    type_filter = st.selectbox("Φιλτράρισμα ανά Τύπο:", ["Όλα", "🎯 VALUE", "🔥 PATTERN"], key="opt_type")
    type_param = None
    if type_filter == "🎯 VALUE":
        type_param = "VALUE"
    elif type_filter == "🔥 PATTERN":
        type_param = "PATTERN"
    
    # Tabs για τις αναλύσεις
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Thresholds", "🧠 Feature Importance", "📈 Streaks",
        "🎲 Monte Carlo", "📊 Pattern Analysis", "🔍 Discrepancies"
    ])
    
    def call_optimizer(endpoint):
        try:
            params = {"type_filter": type_param} if type_param else None
            resp = requests.get(f"{BACKEND_URL}{endpoint}", params=params, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                if data and data.get("text"):
                    st.text(data["text"])
                else:
                    st.warning("Δεν βρέθηκαν αποτελέσματα για αυτή την ανάλυση.")
            else:
                st.error(f"Σφάλμα: {resp.text}")
        except Exception as e:
            st.error(f"Αδυναμία σύνδεσης: {e}")
    
    with tab1:
        st.subheader("📊 Threshold Analysis")
        if st.button("🚀 Find Patterns", key="btn_thresholds"):
            with st.spinner("Ανάλυση κατωφλίων σε εξέλιξη..."):
                call_optimizer("/optimizer/thresholds")
    
    with tab2:
        st.subheader("🧠 Feature Importance (RF vs XGBoost)")
        if st.button("🚀 Compare Models", key="btn_fi"):
            with st.spinner("Εκπαίδευση μοντέλων..."):
                call_optimizer("/optimizer/feature-importance")
    
    with tab3:
        st.subheader("📈 Streak & Drawdown Analyzer")
        if st.button("🚀 Analyze Streaks", key="btn_streaks"):
            with st.spinner("Ανάλυση σερί..."):
                call_optimizer("/optimizer/streaks")
    
    with tab4:
        st.subheader("🎲 Monte Carlo Simulation")
        if st.button("🎲 Run Monte Carlo", key="btn_mc"):
            with st.spinner("Προσομοίωση..."):
                call_optimizer("/optimizer/monte-carlo")
    
    with tab5:
        st.subheader("📊 Pattern Analysis – Στατιστικά ανά Αγορά")
        if st.button("🚀 Analyze Patterns", key="btn_patterns"):
            with st.spinner("Ανάλυση PATTERN picks..."):
                call_optimizer("/optimizer/patterns")
    
    with tab6:
        st.subheader("🔍 Discrepancy Resolution")
        if st.button("🚀 Analyze Discrepancies", key="btn_disc"):
            with st.spinner("Ανάλυση αντιφάσεων..."):
                call_optimizer("/optimizer/discrepancies")
