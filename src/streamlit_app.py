# -*- coding: utf-8 -*-
import json
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ===================== Config globale =====================
ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "src" / "data" / "processed"
CONFIG_PATH = ROOT / "products_config.json"
LOGO_PATH = ROOT / "src" / "images" / "logo-prisme.png"

# ===================== Page Streamlit =====================
st.set_page_config(
    page_title="Prisme - Analyse ETF",
    page_icon=str(LOGO_PATH),
    layout="wide",
)

col_logo, col_title = st.columns([1,16])
with col_logo:
    st.image(str(LOGO_PATH))
with col_title:
    """
    # Prisme - Analyse ETF
    """
"""
Comparez facilement vos ETF et visualisez leurs performances.
"""
"" 

# ===================== Chargement des données =====================
@st.cache_data
def load_ticker_list():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        products = json.load(f)
    return {name: meta["ticker"] for name, meta in products.items()}

ticker_list = load_ticker_list()

# ===================== Sidebar =====================
with st.sidebar:
    st.header("Constructeur de Portefeuille")

    sel_tickers = st.multiselect(
        "ETF",
        options=list(ticker_list.keys()),
        placeholder="Search ETF"
    )

    sel_tickers_dict = {k: ticker_list[k] for k in sel_tickers}

    horizon_map = {
        "1 Mois": 30,
        "3 Mois": 90,
        "6 Mois": 180,
        "1 An": 365,
        "5 Ans": 5*365,
        # "10 Ans": 10*365,
        # "20 Ans": 20*365,
    }

    horizon = st.pills(
        "Depuis",
        options=list(horizon_map.keys()),
        default="6 Mois",
    )

    today = dt.date.today()
    days_back = horizon_map[horizon]
    start_date = today - dt.timedelta(days=days_back)

# ===================== Contenu principal =====================
if not sel_tickers:
    st.info("Sélectionnez au moins un ETF pour afficher les graphiques.")
    st.stop()

# ===================== Chargement des prix et infos =====================
price_dfs, info_dfs = {}, {}
for name in sel_tickers_dict:
    price_dfs[name] = pd.read_parquet(PROCESSED_DIR / f"{name}_data.parquet")
    info_dfs[name] = pd.read_parquet(PROCESSED_DIR / f"{name}_infos.parquet")

# ===================== Préparation graphique =====================
plot_df = []
for name, df in price_dfs.items():
    if "Close" in df.columns:
        tmp = df[["Close"]].reset_index()
        tmp["ETF"] = name
        tmp = tmp[tmp["Date"].dt.date >= start_date]
        plot_df.append(tmp)
plot_df = pd.concat(plot_df, ignore_index=True)

# ===================== Page =====================
top_cols = st.columns([1, 3])

# Metrics
with top_cols[0]:
    st.subheader("Performance des ETF")
    metrics_cols = st.columns(3)
    for i, (name, df) in enumerate(price_dfs.items()):
        df_period = df[df.index.date >= start_date]
        if "Close" not in df.columns or len(df_period) < 2:
            continue
        start_price = df_period["Close"].iloc[0]
        end_price = df_period["Close"].iloc[-1]
        perf = (end_price / start_price - 1) * 100
        metrics_cols[i % 3].metric(label=name, value=f"{perf:.2f} %", delta=f"{perf:.2f} %")

# Graphique prix
# Normalisation des prix
plot_df_normalized = plot_df.copy()
plot_df_normalized["NormClose"] = plot_df_normalized.groupby("ETF")["Close"].transform(
    lambda x: 100 * x / x.iloc[0]
)

with top_cols[1]:
    st.subheader("Évolution des ETF")

    # Onglets pour choisir le type de donnée
    tab_price, tab_norm = st.tabs(["Prix (€)", "Normalisé (indice 100)"])

    # Graphique prix absolu
    with tab_price:
        chart_prices = (
            alt.Chart(plot_df)
            .mark_line()
            .encode(
                x=alt.X("Date:T", title="Date"),
                y=alt.Y("Close:Q", title="Prix (€)"),
                color=alt.Color("ETF:N", title="ETF"),
                tooltip=["ETF", "Date:T", "Close:Q"]
            )
            .properties(height=400)
            .interactive()
        )
        st.altair_chart(chart_prices, use_container_width=True)

    # Graphique prix normalisé
    with tab_norm:
        chart_norm = (
            alt.Chart(plot_df_normalized)
            .mark_line()
            .encode(
                x=alt.X("Date:T", title="Date"),
                y=alt.Y("NormClose:Q", title="Indice 100"),
                color=alt.Color("ETF:N", title="ETF"),
                tooltip=["ETF", "Date:T", "NormClose:Q"]
            )
            .properties(height=400)
            .interactive()
        )
        st.altair_chart(chart_norm, use_container_width=True)



# Risque / Rendement
st.subheader("Profil Risque / Rendement")
risk_data = []
for name, df in price_dfs.items():
    prices = df[df.index.date >= start_date]["Close"]
    returns = prices.pct_change().dropna()
    perf = (prices.iloc[-1] / prices.iloc[0] - 1) * 100
    risk = returns.std() * np.sqrt(252) * 100
    risk_data.append({"ETF": name, "Rendement": perf, "Risque": risk})
risk_df = pd.DataFrame(risk_data)

chart_risk = (
    alt.Chart(risk_df)
    .mark_circle(size=160, opacity=0.8)
    .encode(
        x=alt.X("Rendement:Q", title="Rendement (%)"),
        y=alt.Y("Risque:Q", title="Risque (%)"),
        color=alt.Color("ETF:N"),
        tooltip=["ETF", "Rendement", "Risque"]
    )
    .properties(height=450)
)

zero_lines = alt.Chart(pd.DataFrame({"x": [0], "y": [0]})).mark_rule(color="gray", strokeDash=[4, 4])
chart_risk = chart_risk + zero_lines.encode(x="x") + zero_lines.encode(y="y")
st.altair_chart(chart_risk, use_container_width=True)

# Répartition par Fund Family
st.subheader("Répartition par Fund Family")
repartition = []
for name, df in info_dfs.items():
    family = df["fundFamily"].iloc[0] if "fundFamily" in df.columns else "Inconnu"
    repartition.append({"ETF": name, "FundFamily": family})

repart_df = pd.DataFrame(repartition)
count_df = repart_df.groupby("FundFamily").size().reset_index(name="count")

pie_chart = (
    alt.Chart(count_df)
    .mark_arc(innerRadius=50)
    .encode(
        theta=alt.Theta("count:Q"),
        color=alt.Color("FundFamily:N"),
        tooltip=["FundFamily", "count"]
    )
    .properties(height=400)
)
st.altair_chart(pie_chart, use_container_width=True)
