import os
import json
import requests
import yaml
from datetime import date

def fetch_box_office():
    with open("C:/Users/axoud/.api_keys.yaml") as f:
        keys = yaml.safe_load(f)
    tmdb_key = keys["tmdb"]["key"]

    url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}"
    response = requests.get(url)
    data = response.json()

    today = date.today().strftime("%Y%m%d")
    path = f"datalake/raw/boxoffice/{today}"
    os.makedirs(path, exist_ok=True)

    with open(f"{path}/boxoffice.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Box office saved to {path}")

# Local Test
#if __name__ == "__main__":
#    fetch_box_office()