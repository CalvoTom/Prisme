from pathlib import Path
from scripts.pipeline_etl_finance import run_etl_for_universe


DEFAULT_CONFIG = Path(__file__).parents[1] / 'products_config.json'

def ask_with_default(question: str, default: str) -> str:
    answer = input(f"{question} [{default}] : ").strip()
    return answer or default


def main() -> None:
    print("=== STARTING PRISME ETL ===")

    period = ask_with_default("Période à récupérer (yfinance, ex: 1y, 5y, max)", "5y")
    config_use_default = "n"
    config_path_str: str | None = None

    if DEFAULT_CONFIG.exists():
        config_use_default = ask_with_default(
            f"Utiliser le fichier de config par défaut ? ({DEFAULT_CONFIG}) (y/n)",
            "y",
        ).lower()

    if config_use_default == "y" and DEFAULT_CONFIG.exists():
        config_path_str = str(DEFAULT_CONFIG)
    else:
        other = ask_with_default(
            "Chemin vers un autre fichier de config (laisser vide pour univers par défaut)",
            "",
        )
        config_path_str = other or None

    print("\n--- Récapitulatif ---")
    print(f"Période         : {period}")
    print(f"Fichier config  : {config_path_str or '(univers par défaut intégré)'}")
    confirm = ask_with_default("Confirmer le lancement ? (y/n)", "y").lower()

    if confirm != "y":
        print("Annulé.")
        return

    run_etl_for_universe(config_path=config_path_str, period=period)


if __name__ == "__main__":
    main()


