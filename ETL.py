import argparse
import json
import os
from datetime import datetime

import pandas as pd
import yfinance as yf

BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
INTERIM_DIR = os.path.join(BASE_DIR, "data", "interim")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


# Champs info que l'on garde pour le dashboard
KEYS_TO_KEEP = [
    # Identité
    "symbol",
    "shortName",
    "longName",
    "fundFamily",
    "legalType",
    # Valorisation / devise
    "currency",
    "netAssets",
    "navPrice",
    "regularMarketPrice",
    # Performance
    "ytdReturn",
    "threeYearAverageReturn",
    "fiveYearAverageReturn",
    "beta3Year",
    # Dividendes
    "yield",
    "dividendYield",
    # Contexte de prix
    "fiftyTwoWeekLow",
    "fiftyTwoWeekHigh",
]

def setup_directories() -> None:
    """Crée les dossiers data/raw, data/interim et data/processed si besoin."""
    data_dir = os.path.join(BASE_DIR, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    for directory in [RAW_DIR, INTERIM_DIR, PROCESSED_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_universe(config_path: str | None = None) -> dict:
    """Charge la liste des produits à traiter à partir d'un JSON ou d'un univers par défaut."""
    if config_path and os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {name: product["ticker"] for name, product in data.items()}

    # Univers par défaut (équivalent à ton TICKERS_MAP actuel)
    return {
        "S&P500_PEA": "PE500.PA",
        "NASDAQ_PEA": "PUST.PA",
        "CAC40_ETF": "C40.PA",
        "EMERGING_PEA": "PAEEM.PA",
        "EUROSTOXX_ETF": "C50.PA",
    }


def extract_asset(ticker_symbol: str, period: str = "5y") -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    """Récupère l'historique, les infos et les dividendes depuis yfinance."""
    asset = yf.Ticker(ticker_symbol)

    # 1. Historique de prix
    df_history = asset.history(period=period)

    # 2. Infos statiques / fondamentales
    asset_info = asset.info

    # 3. Dividendes
    df_div = pd.DataFrame(asset.dividends)

    return df_history, asset_info, df_div


def save_raw(name: str, df_history: pd.DataFrame, asset_info: dict) -> None:
    """Sauvegarde brute : CSV pour les prix, JSON pour les infos."""
    if not df_history.empty:
        raw_price_path = os.path.join(RAW_DIR, f"{name}_raw_prices.csv")
        df_history.to_csv(raw_price_path)
        print(f"   [RAW] Prix sauvegardés -> {raw_price_path}")
    else:
        print(f"   [WARN] Pas d'historique de prix pour {name}")

    # Sauvegarde Infos (JSON)
    raw_info_path = os.path.join(RAW_DIR, f"{name}_raw_infos.json")
    with open(raw_info_path, "w", encoding="utf-8") as f:
        json.dump(asset_info, f, indent=4)
    print(f"   [RAW] Infos sauvegardées (JSON) -> {raw_info_path}")


def build_interim_from_raw(
    df_history: pd.DataFrame,
    asset_info: dict,
    df_div: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """
    Construit des versions intermédiaires :
    - prix avec index sans timezone
    - infos sous forme de DataFrame
    - dividendes avec index sans timezone
    """
    # Prix intermédiaires : même colonnes que la source, juste sans timezone
    df_hist_interim = df_history.copy()
    if not df_hist_interim.empty and df_hist_interim.index.tz is not None:
        df_hist_interim.index = df_hist_interim.index.tz_localize(None)

    # Infos intermédiaires : une ligne avec toutes les infos brutes
    df_info_interim = pd.DataFrame([asset_info])

    # Dividendes intermédiaires : même idée, juste sans timezone
    df_div_interim = None
    if df_div is not None and not df_div.empty:
        df_div_interim = df_div.copy()
        if df_div_interim.index.tz is not None:
            df_div_interim.index = df_div_interim.index.tz_localize(None)

    return df_hist_interim, df_info_interim, df_div_interim


def save_interim(
    name: str,
    df_hist_interim: pd.DataFrame,
    df_info_interim: pd.DataFrame,
    df_div_interim: pd.DataFrame | None,
) -> None:
    """
    Sauvegarde intermédiaire :
    - prix en CSV
    - infos en JSON
    - dividendes en CSV
    (pas de Parquet à ce niveau)
    """
    if not df_hist_interim.empty:
        price_path = os.path.join(INTERIM_DIR, f"{name}_prices_interim.csv")
        df_hist_interim.to_csv(price_path)
        print(f"   [INTERIM] Prix sauvegardés -> {price_path}")

    info_path = os.path.join(INTERIM_DIR, f"{name}_infos_interim.json")
    info_record = df_info_interim.to_dict(orient="records")[0] if not df_info_interim.empty else {}
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info_record, f, indent=4)
    print(f"   [INTERIM] Infos sauvegardées (JSON) -> {info_path}")

    if df_div_interim is not None and not df_div_interim.empty:
        div_path = os.path.join(INTERIM_DIR, f"{name}_dividends_interim.csv")
        df_div_interim.to_csv(div_path)
        print(f"   [INTERIM] Dividendes sauvegardés -> {div_path}")


def transform_history_and_info(df_history: pd.DataFrame, asset_info: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Nettoie les prix et filtre les infos utiles (niveau processed)."""
    if df_history.empty:
        raise ValueError("DataFrame d'historique vide, impossible de transformer.")

    df_history = df_history.copy()
    if df_history.index.tz is not None:
        df_history.index = df_history.index.tz_localize(None)
    df_clean = df_history[["Open", "High", "Low", "Close", "Volume"]].copy()

    clean_info_data = {k: asset_info.get(k, None) for k in KEYS_TO_KEEP}
    df_info_clean = pd.DataFrame([clean_info_data])

    return df_clean, df_info_clean


def transform_dividends(df_div: pd.DataFrame) -> pd.DataFrame | None:
    """Nettoie les dividendes si disponibles (niveau processed)."""
    if df_div is None or df_div.empty:
        return None

    df_div = df_div.copy()
    if df_div.index.tz is not None:
        df_div.index = df_div.index.tz_localize(None)
    df_div.columns = ["Dividends"]
    return df_div


def load_processed(name: str, df_clean: pd.DataFrame, df_info_clean: pd.DataFrame, df_div: pd.DataFrame | None) -> None:
    """Sauvegarde les données propres en Parquet."""
    price_path = os.path.join(PROCESSED_DIR, f"{name}_data.parquet")
    df_clean.to_parquet(price_path)

    info_path = os.path.join(PROCESSED_DIR, f"{name}_infos.parquet")
    df_info_clean.to_parquet(info_path)

    if df_div is not None and not df_div.empty:
        div_path = os.path.join(PROCESSED_DIR, f"{name}_dividends.parquet")
        df_div.to_parquet(div_path)

    print(f"   [PROCESSED] Données nettoyées sauvegardées (prix/infos/dividendes).")


def run_etl_for_universe(
    config_path: str | None = None,
    period: str = "5y",
) -> None:
    """Boucle principale : lance l'ETL pour chaque produit."""
    print(f"--- DÉBUT DE L'ETL ({datetime.now().isoformat(timespec='seconds')}) ---")
    setup_directories()

    tickers_map = load_universe(config_path)

    for name, ticker_symbol in tickers_map.items():
        print(f"\nTraitement de : {name} ({ticker_symbol})")
        try:
            # Extraction depuis yfinance
            df_history, asset_info, df_div_raw = extract_asset(ticker_symbol, period=period)

            # Niveau RAW
            save_raw(name, df_history, asset_info)

            if df_history.empty:
                continue

            # Niveau INTERIM (transformé mais encore proche du brut)
            df_hist_interim, df_info_interim, df_div_interim = build_interim_from_raw(
                df_history=df_history,
                asset_info=asset_info,
                df_div=df_div_raw,
            )
            save_interim(name, df_hist_interim, df_info_interim, df_div_interim)

            # Niveau PROCESSED (nettoyé et structuré pour le dashboard)
            df_clean, df_info_clean = transform_history_and_info(df_hist_interim, asset_info)
            df_div_processed = transform_dividends(df_div_interim if df_div_interim is not None else pd.DataFrame())

            load_processed(name, df_clean, df_info_clean, df_div_processed)

        except Exception as e:
            print(f"   [ERREUR] Échec du traitement pour {name} ({ticker_symbol}) : {e}")

    print(f"\n--- ETL TERMINÉ ({datetime.now().isoformat(timespec='seconds')}) ---")


def parse_args() -> argparse.Namespace:
    """Parse les options de ligne de commande (config JSON, période)."""
    parser = argparse.ArgumentParser(description="Pipeline ETL produits financiers (yfinance -> Parquet).")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Chemin vers un fichier JSON de configuration des produits (facultatif).",
    )
    parser.add_argument(
        "--period",
        type=str,
        default="5y",
        help='Période d\'historique à récupérer (ex: "1y", "5y", "max").',
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_etl_for_universe(config_path=args.config, period=args.period)