import os
import json
import pandas as pd
from datetime import date

def format_movie_details():
    today = date.today().strftime("%Y%m%d")
    input_path = f"datalake/raw/movie_details/{today}/movie_details.json"
    output_path = f"datalake/formatted/movie_details/{today}"
    os.makedirs(output_path, exist_ok=True)

    with open(input_path) as f:
        raw = json.load(f)

    df = pd.json_normalize(raw)

    # Normalisation simple
    df = df[["imdbID", "Title", "Released", "Runtime", "Genre", "imdbRating", "Director", "Actors"]]

    # Conversion des types
    df["Released"] = pd.to_datetime(df["Released"], errors="coerce")
    df["Runtime"] = df["Runtime"].str.extract(r'(\d+)').astype(float)

    df.to_parquet(f"{output_path}/movie_details.parquet", index=False)

    print(f"✅ Movie details formatted and saved to {output_path}")

# Local Test
#if __name__ == "__main__":
#    format_movie_details()
