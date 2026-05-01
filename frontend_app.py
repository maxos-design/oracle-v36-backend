import streamlit as st
import pandas as pd
import requests
import numpy as np

# ───────────── ΡΥΘΜΙΣΕΙΣ (ΑΛΛΑΞΕ ΤΟ) ─────────────
BACKEND_URL = "https://oracle-v36-backend.onrender.com"  # <-- ΒΑΛΕ ΤΟ URL ΣΟΥ

# ───────────── ΣΕΛΙΔΑ ─────────────
st.set_page_config(
    page_title="Oracle Suite V36",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Πλάγια μπάρα
st.sidebar.image("https://i.imgur.com/7q8Q9vC.png", width=80)
st.sidebar.title("ORACLE SUITE V36")
st.sidebar.markdown("Professional Betting Analytics")
menu = st.sidebar.radio("Μενού", ["🏠 Dashboard", "📜 Ιστορικό Ledger"])

# ───────────── DASHBOARD ─────────────
if menu == "🏠 Dashboard":
    st.title("🏠 Dashboard")
    st.markdown("Καλώς ήρθατε στο Oracle Suite V36 Cloud Edition.")

    try:
        response = requests.get(f"{BACKEND_URL}/stats")
        stats = response.json()

        col1, col2, col3 = st.columns(3)
        col1.metric("Συνολικά Στοιχήματα", stats.get('total_bets', 0))
        col2.metric("Συνολικό PnL", f"€{stats.get('total_pnl', 0.0):.2f}")
        col3.metric("Win Rate", f"{stats.get('win_rate', 0.0):.1f}%")

        st.success("Τα δεδομένα αντλήθηκαν επιτυχώς από το cloud!")
    except Exception as e:
        st.error(f"Αποτυχία σύνδεσης με το backend: {e}")

# ───────────── ΙΣΤΟΡΙΚΟ LEDGER ─────────────
elif menu == "📜 Ιστορικό Ledger":
    st.title("📜 Ιστορικό Στοιχημάτων")

    try:
        response = requests.get(f"{BACKEND_URL}/ledger?limit=100")
        data = response.json()
        df = pd.DataFrame(data)

        if not df.empty:
            st.dataframe(
                df[['date', 'match', 'market', 'odds', 'result', 'pnl']],
                use_container_width=True,
                hide_index=True,
            )
            
            df['pnl'] = pd.to_numeric(df['pnl'])
            df['cumulative_pnl'] = df['pnl'].cumsum()
            
            st.subheader("Γράφημα Κέρδους (PnL)")
            st.line_chart(df, x='date', y='cumulative_pnl')
        else:
            st.info("Δεν υπάρχουν δεδομένα στο Ledger ακόμα.")
    except Exception as e:
        st.error(f"Αποτυχία φόρτωσης δεδομένων: {e}")
