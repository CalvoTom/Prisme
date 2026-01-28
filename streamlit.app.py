# ======================================================================
# Imports
# ======================================================================
import json
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

import streamlit as st
import altair as alt
import matplotlib.pyplot as plt
import seaborn as sns

# ======================================================================
# Global config
# ======================================================================
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 5)

ROOT = Path.cwd().resolve()
PROCESSED_DIR = ROOT / "src" / "data" / "processed"
CONFIG_PATH = ROOT / "products_config.json"

# ======================================================================
# Streamlit page config
# ======================================================================
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Prisme")

# ======================================================================
# Data loading
# ======================================================================
@st.cache_data
def load_ticker_list():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        products = json.load(f)

    return {name: meta["ticker"] for name, meta in products.items()}

ticker_list = load_ticker_list()

# ======================================================================
# Sidebar
# ======================================================================
with st.sidebar:
    st.subheader("Portfolio Builder")

    sel_tickers = st.multiselect(
        "ETF",
        options=list(ticker_list.keys()),
        placeholder="Search ETF"
    )

    sel_tickers_dict = {k: ticker_list[k] for k in sel_tickers}

    horizon_map = {
        "1 Months": 30,
        "3 Months": 90,
        "6 Months": 180,
        "1 Year": 365,
        # "5 Years": 5*365,
        # "10 Years": 10*365,
        # "20 Years": 20*365,
    }


    horizon = st.pills(
        "Time horizon",
        options=list(horizon_map.keys()),
        default="6 Months",
    )

    today = dt.date.today()
    days_back = horizon_map[horizon]
    start_date = today - dt.timedelta(days=days_back)
# ======================================================================
# Tabs
# ======================================================================
if not sel_tickers:
    st.info("Select ETF to view plots")

else:
    st.subheader("All Stocks")

    # --------------------------------------------------------------
    # Load price data
    # --------------------------------------------------------------
    price_dfs: dict[str, pd.DataFrame] = {}

    for name in sel_tickers_dict:
        path = PROCESSED_DIR / f"{name}_data.parquet"
        df = pd.read_parquet(path)
        price_dfs[name] = df

    # --------------------------------------------------------------
    # Prepare data for plotting
    # --------------------------------------------------------------
    plot_df = []

    for name, df in price_dfs.items():
        if "Close" in df.columns:
            tmp = df[["Close"]].reset_index()
            tmp["ETF"] = name
            tmp = tmp[tmp["Date"].dt.date >= start_date]
            plot_df.append(tmp)

    plot_df = pd.concat(plot_df, ignore_index=True)

    # --------------------------------------------------------------
    # Altair chart
    # --------------------------------------------------------------
    chart = (
        alt.Chart(plot_df)
        .mark_line()
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("Close:Q", title="Price"),
            color=alt.Color("ETF:N", title="ETF"),
            tooltip=["ETF", "Date:T", "Close:Q"],
        )
        .properties(
            title="Close price comparison (RAW)",
            height=400
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)

    st.write("### ETF Performance (%)")

    cols = st.columns(3)  # 3 metrics par ligne
    i = 0

    for name, df in price_dfs.items():
        if "Close" not in df.columns:
            continue

        df_period = df[df.index.date >= start_date]

        if len(df_period) < 2:
            continue

        start_price = df_period["Close"].iloc[0]
        end_price = df_period["Close"].iloc[-1]

        perf = (end_price / start_price - 1) * 100

        cols[i % 3].metric(
            label=name,
            value=f"{perf:.2f} %",
            delta=f"{perf:.2f} %"
        )

        i += 1
