import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


KEYS_TO_KEEP = [
    "symbol",
    "shortName",
    "longName",
    "fundFamily",
    "legalType",
    "currency",
    "netAssets",
    "navPrice",
    "regularMarketPrice",
    "ytdReturn",
    "threeYearAverageReturn",
    "fiveYearAverageReturn",
    "beta3Year",
    "yield",
    "dividendYield",
    "fiftyTwoWeekLow",
    "fiftyTwoWeekHigh",
]


def setup_directories() -> None:
    (PROJECT_ROOT / "data").mkdir(exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_universe(config_path: str | None = None) -> dict[str, str]:
    if config_path:
        p = Path(config_path)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            return {name: product["ticker"] for name, product in data.items()}

    return {
        "S&P500_PEA": "PE500.PA",
        "NASDAQ_PEA": "PUST.PA",
        "CAC40_ETF": "C40.PA",
        "EMERGING_PEA": "PAEEM.PA",
        "EUROSTOXX_ETF": "C50.PA",
    }


def extract_asset(ticker_symbol: str, period: str = "5y") -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    asset = yf.Ticker(ticker_symbol)
    df_history = asset.history(period=period)
    asset_info = asset.info
    df_div = pd.DataFrame(asset.dividends)
    return df_history, asset_info, df_div


def save_raw(name: str, df_history: pd.DataFrame, asset_info: dict) -> None:
    if not df_history.empty:
        raw_price_path = RAW_DIR / f"{name}_raw_prices.csv"
        df_history.to_csv(raw_price_path)
        print(f"   [RAW] Prix sauvegardés -> {raw_price_path}")
    else:
        print(f"   [WARN] Pas d'historique de prix pour {name}")

    raw_info_path = RAW_DIR / f"{name}_raw_infos.json"
    raw_info_path.write_text(json.dumps(asset_info, indent=4), encoding="utf-8")
    print(f"   [RAW] Infos sauvegardées (JSON) -> {raw_info_path}")


def build_interim_from_raw(
    df_history: pd.DataFrame,
    asset_info: dict,
    df_div: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    df_hist_interim = df_history.copy()
    if not df_hist_interim.empty and df_hist_interim.index.tz is not None:
        df_hist_interim.index = df_hist_interim.index.tz_localize(None)

    df_info_interim = pd.DataFrame([asset_info])

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
    if not df_hist_interim.empty:
        price_path = INTERIM_DIR / f"{name}_prices_interim.csv"
        df_hist_interim.to_csv(price_path)
        print(f"   [INTERIM] Prix sauvegardés -> {price_path}")

    info_path = INTERIM_DIR / f"{name}_infos_interim.json"
    info_record = df_info_interim.to_dict(orient="records")[0] if not df_info_interim.empty else {}
    info_path.write_text(json.dumps(info_record, indent=4), encoding="utf-8")
    print(f"   [INTERIM] Infos sauvegardées (JSON) -> {info_path}")

    if df_div_interim is not None and not df_div_interim.empty:
        div_path = INTERIM_DIR / f"{name}_dividends_interim.csv"
        df_div_interim.to_csv(div_path)
        print(f"   [INTERIM] Dividendes sauvegardés -> {div_path}")


def transform_history_and_info(df_history: pd.DataFrame, asset_info: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    if df_div is None or df_div.empty:
        return None
    df_div = df_div.copy()
    if df_div.index.tz is not None:
        df_div.index = df_div.index.tz_localize(None)
    df_div.columns = ["Dividends"]
    return df_div


def load_processed(name: str, df_clean: pd.DataFrame, df_info_clean: pd.DataFrame, df_div: pd.DataFrame | None) -> None:
    price_path = PROCESSED_DIR / f"{name}_data.parquet"
    df_clean.to_parquet(price_path)

    info_path = PROCESSED_DIR / f"{name}_infos.parquet"
    df_info_clean.to_parquet(info_path)

    if df_div is not None and not df_div.empty:
        div_path = PROCESSED_DIR / f"{name}_dividends.parquet"
        df_div.to_parquet(div_path)

    print("   [PROCESSED] Données nettoyées sauvegardées (prix/infos/dividendes).")


def run_etl_for_universe(config_path: str | None = None, period: str = "5y") -> None:
    print(f"--- DÉBUT DE L'ETL ({datetime.now().isoformat(timespec='seconds')}) ---")
    setup_directories()

    tickers_map = load_universe(config_path)
    for name, ticker_symbol in tickers_map.items():
        print(f"\nTraitement de : {name} ({ticker_symbol})")
        try:
            df_history, asset_info, df_div_raw = extract_asset(ticker_symbol, period=period)

            save_raw(name, df_history, asset_info)
            if df_history.empty:
                continue

            df_hist_interim, df_info_interim, df_div_interim = build_interim_from_raw(
                df_history=df_history,
                asset_info=asset_info,
                df_div=df_div_raw,
            )
            save_interim(name, df_hist_interim, df_info_interim, df_div_interim)

            df_clean, df_info_clean = transform_history_and_info(df_hist_interim, asset_info)
            df_div_processed = transform_dividends(df_div_interim if df_div_interim is not None else pd.DataFrame())

            load_processed(name, df_clean, df_info_clean, df_div_processed)

        except Exception as e:
            print(f"   [ERREUR] Échec du traitement pour {name} ({ticker_symbol}) : {e}")

    print(f"\n--- ETL TERMINÉ ({datetime.now().isoformat(timespec='seconds')}) ---")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline ETL produits financiers (yfinance -> RAW/INTERIM/PROCESSED).")
    parser.add_argument("--config", type=str, default=None, help="Chemin vers le JSON de config (ex: products_config.json).")
    parser.add_argument("--period", type=str, default="5y", help='Historique à récupérer (ex: "1y", "5y", "max").')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_etl_for_universe(config_path=args.config, period=args.period)


if __name__ == "__main__":
    main()


