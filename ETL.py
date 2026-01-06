import yfinance as yf
import pandas as pd
import os
import json

# --- 1. CONFIGURATION ---
TICKERS_MAP = {
    "S&P500_PEA": "PE500.PA",
    "NASDAQ_PEA": "PUST.PA",
    "CAC40_ETF": "C40.PA",
    "EMERGING_PEA": "PAEEM.PA",
    "EUROSTOXX_ETF": "C50.PA"
}

# Liste des champs que l'on veut garder pour le dashboard
KEYS_TO_KEEP = [
    # --- IDENTITÉ DU FONDS ---
    "symbol",                  # Le ticker unique (ex: PE500.PA)
    "shortName",               # Nom court (ex: Amundi PEA S&P 500)
    "longName",                # Nom complet avec détails (Capitalisant/Distribuant)
    "fundFamily",              # La société de gestion (ex: Amundi, BlackRock)
    "legalType",               # Type juridique ("Exchange Traded Fund")

    # --- VALORISATION & DEVISE ---
    "currency",                # Devise de cotation (Doit être "EUR" pour nous)
    "netAssets",               # Actifs sous gestion (Taille du fonds en €)
    "navPrice",                # Valeur Nette d'Inventaire (Vraie valeur calculée du fonds)
    "regularMarketPrice",      # Dernier prix payé sur le marché (Cotation temps réel)

    # --- PERFORMANCE (KPIs) ---
    "ytdReturn",               # Performance depuis le 1er Janvier (Year-To-Date)
    "threeYearAverageReturn",  # Rendement moyen annualisé sur 3 ans (Tendance moyen terme)
    "fiveYearAverageReturn",   # Rendement moyen annualisé sur 5 ans (Tendance long terme)
    "beta3Year",               # Volatilité par rapport au marché ( >1 = plus risqué/agressif)

    # --- DIVIDENDES ---
    "yield",                   # Rendement courant (Souvent 0 pour les ETF Capitalisants)
    "dividendYield",           # Taux de dividende annuel en % (KPI important pour le rendement cash)

    # --- CONTEXTE DE PRIX (Jauges) ---
    "fiftyTwoWeekLow",         # Prix le plus bas atteint sur 1 an (Support)
    "fiftyTwoWeekHigh"         # Prix le plus haut atteint sur 1 an (Résistance)
]

BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

def setup_directories():
    data_dir = os.path.join(BASE_DIR, "data")
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    for directory in [RAW_DIR, PROCESSED_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)

def fetch_and_process_data():
    print(f"--- DÉBUT DE L'ETL ---")
    setup_directories()
    
    for name, ticker_symbol in TICKERS_MAP.items():
        print(f"\nTraitement de : {name} ({ticker_symbol})")
        
        # --- A. EXTRACTION ---
        asset = yf.Ticker(ticker_symbol)
        
        # 1. Récupération historique
        df_history = asset.history(period="5y")
        
        # 2. Récupération des infos
        asset_info = asset.info

        # --- B. CHARGEMENT RAW (Sauvegarde brute pour traçabilité) ---
        
        # Sauvegarde Prix (CSV)
        if not df_history.empty:
            raw_price_path = os.path.join(RAW_DIR, f"{name}_raw_prices.csv")
            df_history.to_csv(raw_price_path)
            print(f"   [RAW] Prix sauvegardés.")
        else:
            print(f"   [ERREUR] Pas d'historique de prix pour {name}")

        # Sauvegarde Infos (JSON)
        raw_info_path = os.path.join(RAW_DIR, f"{name}_raw_infos.json")
        with open(raw_info_path, 'w', encoding='utf-8') as f:
            json.dump(asset_info, f, indent=4)
        print(f"   [RAW] Infos sauvegardées (JSON).")

        # --- C. TRANSFORMATION ---
        
        if df_history.empty: continue

        # 1. Nettoyage Prix
        df_history.index = df_history.index.tz_localize(None)
        df_clean = df_history[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        
        # 2. Nettoyage Infos
        # On crée un dictionnaire filtré en ne prenant que les clés présentes dans KEYS_TO_KEEP
        clean_info_data = {k: asset_info.get(k, None) for k in KEYS_TO_KEEP}
        
        # Conversion en DataFrame pour le stockage Parquet
        df_info_clean = pd.DataFrame([clean_info_data])

        # 3. Gestion Dividendes
        # Dans notre cas, pas de dividendes car ETF capitalisant et non distribuant
        df_div = pd.DataFrame(asset.dividends)
        if not df_div.empty:
            df_div.index = df_div.index.tz_localize(None)
            df_div.columns = ['Dividends']

        # --- D. CHARGEMENT PROCESSED ---
        
        # Stockage Prix
        price_path = os.path.join(PROCESSED_DIR, f"{name}_data.parquet")
        df_clean.to_parquet(price_path)
        
        # Stockage Infos Nettoyées
        info_path = os.path.join(PROCESSED_DIR, f"{name}_infos.parquet")
        df_info_clean.to_parquet(info_path)
        print(f"   [PROCESSED] Données nettoyées sauvegardées.")
        
        # Stockage Dividendes
        if not df_div.empty:
            div_path = os.path.join(PROCESSED_DIR, f"{name}_dividends.parquet")
            df_div.to_parquet(div_path)

if __name__ == "__main__":
    fetch_and_process_data()
    print("\n--- ETL TERMINÉ ---")