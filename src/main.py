# -*- coding: utf-8 -*-
from pathlib import Path
import subprocess
import sys
from scripts.pipeline_etl_finance import run_etl_for_universe

DEFAULT_CONFIG = Path(__file__).parents[1] / 'products_config.json'
STREAMLIT_SCRIPT = Path(__file__).parents[1] / "src" / "streamlit_app.py"
PERIOD_DEFAULT = "5y"

def ask_with_default(question: str, default: str) -> str:
    """Fonction interactive avec valeur par défaut."""
    answer = input(f"{question} [{default}] : ").strip()
    return answer or default

def run_pipeline_interactive() -> None:
    """Lancer le pipeline en mode interactif (console)."""
    print("=== STARTING PRISME ETL ===")

    period = ask_with_default("Période à récupérer (yfinance, ex: 1y, 5y, max)", PERIOD_DEFAULT)
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
    print("=== ETL TERMINÉ ===")

def run_pipeline_auto(period: str = PERIOD_DEFAULT, config_path: str | None = None) -> None:
    """Lancer le pipeline automatiquement sans interactivité."""
    print("=== STARTING PRISME ETL (AUTO) ===")
    run_etl_for_universe(config_path=config_path or str(DEFAULT_CONFIG), period=period)
    print("=== ETL TERMINÉ ===")

def launch_streamlit() -> None:
    """Lancer Streamlit pour l'interface."""
    print("=== DÉMARRAGE DE L'APPLICATION STREAMLIT ===")
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(STREAMLIT_SCRIPT)])

def main(interactive: bool = True) -> None:
    """Point d'entrée principal."""
    if interactive:
        run_pipeline_interactive()
    else:
        run_pipeline_auto()

    # Lancer Streamlit après le pipeline
    launch_streamlit()

if __name__ == "__main__":
    # Par défaut, le mode interactif est True
    main(interactive=True)
