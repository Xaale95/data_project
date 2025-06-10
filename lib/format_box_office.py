import os
import json
import pandas as pd
from datetime import date


def format_box_office():
    today = date.today().strftime("%Y%m%d")
    input_path = f"datalake/raw/boxoffice/{today}/boxoffice.json"
    output_path = f"datalake/formatted/boxoffice/{today}"
    os.makedirs(output_path, exist_ok=True)

    with open(input_path) as f:
        raw = json.load(f)

    df = pd.json_normalize(raw["results"])

    # Normalisation simple (on garde que quelques colonnes utiles ici)
    df = df[["id", "title", "release_date", "popularity", "vote_average", "vote_count"]]

    # Conversion des dates
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    # Sauvegarde
    df.to_parquet(f"{output_path}/boxoffice.parquet", index=False)

    print(f"✅ Box office formatted and saved to {output_path}")

# Local Test
#if __name__ == "__main__":
#    format_box_office()
