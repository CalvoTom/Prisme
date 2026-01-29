import pandas as pd
from pathlib import Path

DATA_DIR = Path("src/data/processed")

converted = 0

for file in DATA_DIR.glob("*_data.parquet"):
    print(f"Traitement : {file.name}")

    try:
        df = pd.read_parquet(file)
    except Exception as e:
        print(f"❌ Lecture impossible : {e}")
        continue

    # normalisation colonnes
    df.columns = [c.strip().lower() for c in df.columns]

    # 🧠 CAS 1 : la date est dans l’index
    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            df.rename(columns={"index": "date"}, inplace=True)

        elif df.index.name and "date" in df.index.name.lower():
            df = df.reset_index()
            df.rename(columns={df.columns[0]: "date"}, inplace=True)

        else:
            print("⚠️ Colonne 'date' absente → ignoré")
            continue

    # conversion timestamp → datetime
    if pd.api.types.is_numeric_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], unit="ms", errors="coerce")
    else:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["date"])

    # sauvegarde parquet propre
    df.to_parquet(file, index=False)
    converted += 1
    print("✅ Normalisé")

print(f"\n🎉 {converted} fichiers data normalisés")
