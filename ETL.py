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
    # Identité
    "symbol", "shortName", "longName", "fundFamily", "legalType",
    # Devise & Prix
    "currency", "netAssets", "navPrice", "regularMarketPrice",
    # Performance
    "ytdReturn",                # Rendement depuis le 1er Janvier
    "threeYearAverageReturn",   # Rendement moyen 3 ans
    "fiveYearAverageReturn",    # Rendement moyen 5 ans
    "beta3Year",                # Indicateur de volatilité/risque
    # Dividende
    "yield", "dividendYield",
    # Limites
    "fiftyTwoWeekLow", "fiftyTwoWeekHigh"
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