import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from pathlib import Path

# --------------------
# PATHS
# --------------------
ROOT_DIR = Path(__file__).resolve().parents[0]  # src/
DATA_DIR = ROOT_DIR / "data" / "processed"

# --------------------
# CONFIG
# --------------------
st.set_page_config(
    page_title="Patrimonia Capital – Analyse ETF",
    layout="wide"
)

# --------------------
# LOAD DATA (ETFs TIME SERIES)
# --------------------
@st.cache_data
def load_data():
    data_files = list(DATA_DIR.glob("*_data.parquet"))
    if not data_files:
        st.error(f"Aucun fichier trouvé dans {DATA_DIR}")
        st.stop()

    dfs = []

    for file in data_files:
        # 1️⃣ Tentative lecture parquet
        try:
            df = pd.read_parquet(file)
        except Exception:
            # 2️⃣ Fallback JSON lines (TON CAS)
            try:
                df = pd.read_json(file, lines=True)
            except Exception as e:
                st.warning(f"Impossible de lire {file.name} ({e})")
                continue

        # Nettoyage colonnes
        df.columns = [c.strip().lower() for c in df.columns]

        # Vérifications
        if "date" not in df.columns:
            st.warning(f"Colonne 'date' manquante dans {file.name}")
            continue

        if "close" not in df.columns:
            st.warning(f"Colonne 'close' manquante dans {file.name}")
            continue

        # Conversion timestamp (ms → datetime)
        if np.issubdtype(df["date"].dtype, np.number):
            df["date"] = pd.to_datetime(df["date"], unit="ms", errors="coerce")
        else:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        df = df.dropna(subset=["date"])

        # Nom ETF
        df["etf"] = file.stem.replace("_data", "")

        dfs.append(df)

    if not dfs:
        st.error("Aucun fichier data correct trouvé")
        st.stop()

    df_all = pd.concat(dfs, ignore_index=True)

    # Calcul rendements
    df_all = df_all.sort_values(["etf", "date"])
    df_all["return"] = df_all.groupby("etf")["close"].pct_change().fillna(0)

    return df_all


df = load_data()

# --------------------
# LOAD ETF INFOS
# --------------------
@st.cache_data
def load_info():
    info_files = list(DATA_DIR.glob("*_infos.parquet"))
    if not info_files:
        st.warning("Aucun fichier infos trouvé")
        return pd.DataFrame()

    dfs = []
    for file in info_files:
        df_info = pd.read_parquet(file)
        if "symbol" in df_info.columns:
            df_info.rename(columns={"symbol": "etf"}, inplace=True)
        dfs.append(df_info)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

df_info = load_info()

# --------------------
# SQL ANALYSIS (DuckDB)
# --------------------
con = duckdb.connect()
con.register("etf", df)

stats = con.execute("""
SELECT
    etf,
    AVG(return) AS mean_return,
    STDDEV(return) AS volatility,
    SUM(return) AS cumulative_return
FROM etf
GROUP BY etf
ORDER BY cumulative_return DESC
""").df()

# --------------------
# SIDEBAR
# --------------------
st.sidebar.title("Patrimonia Capital")
page = st.sidebar.radio(
    "Navigation",
    [
        "Présentation",
        "Vue globale du marché",
        "Risque & Rendement",
        "Profils investisseurs",
        "Recommandation CGP"
    ]
)

selected_etf = st.sidebar.multiselect(
    "Sélection des ETF",
    options=df["etf"].unique(),
    default=df["etf"].unique()
)

df_filtered = df[df["etf"].isin(selected_etf)]

# --------------------
# PAGE 1 – PRESENTATION
# --------------------
if page == "Présentation":
    st.title("📊 Analyse ETF – Conseil en Gestion de Patrimoine")
    st.markdown("""
    **Patrimonia Capital** est un cabinet fictif de conseil en gestion de patrimoine
    spécialisé dans l’investissement boursier via les ETF.

    ### 🎯 Objectif
    Aider des clients à fort patrimoine à :
    - Comprendre les marchés financiers
    - Comparer les grands indices
    - Construire une allocation adaptée à leur profil de risque
    """)
    st.info("Les données utilisées sont réelles et issues des marchés financiers.")

# --------------------
# PAGE 2 – VUE GLOBALE
# --------------------
elif page == "Vue globale du marché":
    st.title("🌍 Vue globale des ETF")

    df_cum = df_filtered.copy()
    df_cum["cumulative_return"] = df_cum.groupby("etf")["return"].cumsum()

    fig = px.line(
        df_cum,
        x="date",
        y="cumulative_return",
        color="etf",
        title="Performance cumulée des ETF"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Statistiques clés")
    st.dataframe(stats)

# --------------------
# PAGE 3 – RISQUE & RENDEMENT
# --------------------
elif page == "Risque & Rendement":
    st.title("⚖️ Analyse Risque / Rendement")

    fig = px.scatter(
        stats,
        x="volatility",
        y="mean_return",
        size="cumulative_return",
        color="etf",
        title="Risque vs Rendement"
    )
    st.plotly_chart(fig, use_container_width=True)

    corr = df_filtered.pivot_table(
        values="return",
        index="date",
        columns="etf"
    ).corr()

    fig_corr = px.imshow(
        corr,
        text_auto=True,
        title="Corrélation entre ETF"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# --------------------
# PAGE 4 – PROFILS INVESTISSEURS
# --------------------
elif page == "Profils investisseurs":
    st.title("👥 Profils d’investisseurs")

    profil = st.selectbox(
        "Choisissez un profil",
        ["Défensif", "Équilibré", "Dynamique"]
    )

    if profil == "Défensif":
        reco = stats.sort_values("volatility").head(3)
    elif profil == "Équilibré":
        reco = stats.sort_values("mean_return", ascending=False).iloc[1:4]
    else:
        reco = stats.sort_values("mean_return", ascending=False).head(3)

    st.subheader("ETF recommandés")
    st.dataframe(reco)

    if not df_info.empty:
        st.subheader("ℹ️ Informations ETF")
        merged = pd.merge(reco, df_info, on="etf", how="left")
        st.dataframe(merged[["etf", "currency", "fundFamily", "netAssets", "ytdReturn"]])

# --------------------
# PAGE 5 – RECOMMANDATION CGP
# --------------------
elif page == "Recommandation CGP":
    st.title("💼 Recommandation patrimoniale")

    allocation = {
        "ETF Monde": 40,
        "ETF US": 30,
        "ETF Europe": 20,
        "ETF Obligataire": 10
    }

    alloc_df = pd.DataFrame(
        allocation.items(),
        columns=["Classe d’actif", "Poids (%)"]
    )

    fig = px.pie(
        alloc_df,
        names="Classe d’actif",
        values="Poids (%)",
        title="Allocation type conseillée"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.success("""
    ✅ Cette allocation permet :
    - Une diversification géographique
    - Une maîtrise du risque
    - Une performance long terme adaptée aux clients patrimoniaux
    """)

# --------------------
# OPTIONNEL : Sidebar infos d’un ETF
# --------------------
if not df_info.empty:
    selected_etf_sidebar = st.sidebar.selectbox(
        "Voir infos d’un ETF",
        df_filtered["etf"].unique()
    )
    info = df_info[df_info["etf"] == selected_etf_sidebar]
    if not info.empty:
        st.sidebar.markdown(f"**Nom fonds**: {info['longName'].values[0]}")
        st.sidebar.markdown(f"**Devise**: {info['currency'].values[0]}")
        st.sidebar.markdown(f"**Fonds**: {info['fundFamily'].values[0]}")
        st.sidebar.markdown(f"**YTD Return**: {info['ytdReturn'].values[0]:.2f}%")
