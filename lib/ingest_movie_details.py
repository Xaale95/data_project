import os
import json
import requests
import yaml
from datetime import date
import urllib.parse

def fetch_movie_details():
    with open("C:/Users/axoud/.api_keys.yaml") as f:
        keys = yaml.safe_load(f)
    omdb_key = keys["omdb"]["key"]

    today = date.today().strftime("%Y%m%d")
    with open(f"datalake/raw/boxoffice/{today}/boxoffice.json") as f:
        movies = json.load(f)

    results = []
    for movie in movies.get("results", []):
        title = movie.get("title")
        if title:
            query = urllib.parse.quote(title)
            url = f"http://www.omdbapi.com/?t={query}&apikey={omdb_key}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json",
                "Connection": "keep-alive"
            }
            r = requests.get(url, headers=headers, timeout=10)
            print(f"{r.status_code} | {r.url}")

            if r.status_code == 200:
                data = r.json()
                if data.get("Response") == "True":
                    results.append(data)
                else:
                    print(f"❌ Film introuvable : {title} | OMDB Error: {data.get('Error')}")

    path = f"datalake/raw/movie_details/{today}"
    os.makedirs(path, exist_ok=True)

    with open(f"{path}/movie_details.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"✅ Movie details saved to {path}")

# Local Test
#if __name__ == "__main__":
#    fetch_movie_details()
