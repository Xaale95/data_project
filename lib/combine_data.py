import pandas as pd
import os
from datetime import date

def combine_data():
    today = date.today().strftime("%Y%m%d")

    boxoffice_path = f"datalake/formatted/boxoffice/{today}/boxoffice.parquet"
    movie_details_path = f"datalake/formatted/movie_details/{today}/movie_details.parquet"
    output_path = f"datalake/formatted/combined/{today}"
    os.makedirs(output_path, exist_ok=True)

    # Chargement des données
    df_box = pd.read_parquet(boxoffice_path)
    df_details = pd.read_parquet(movie_details_path)

    # Fusion sur le titre (ou 'id' si tu l’as)
    df = pd.merge(df_box, df_details, left_on="title", right_on="Title", how="inner")

    # Statistiques
    df["vote_score"] = df["vote_average"] * df["vote_count"]
    df["imdbRating"] = pd.to_numeric(df["imdbRating"], errors="coerce")
    df["Runtime"] = pd.to_numeric(df["Runtime"], errors="coerce")

    # Agrégats simples
    avg_votes = df["vote_average"].mean()
    top_genres = df["Genre"].str.split(", ").explode().value_counts().head(3)

    # Recommandation simple
    df["is_top_genre"] = df["Genre"].apply(lambda g: any(tg in g for tg in top_genres.index))

    # Export final
    df.to_parquet(f"{output_path}/cinema_combined.parquet", index=False)

    print(f"✅ Données combinées enregistrées dans : {output_path}")
    print(f"📊 Moyenne vote TMDB : {avg_votes:.2f}")
    print(f"⭐ Top genres : {list(top_genres.index)}")

#Local Test
#if __name__ == "__main__":
#    combine_data()
