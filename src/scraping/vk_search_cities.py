import requests
import pandas as pd
import time
from tqdm import tqdm
import sys

TOKEN = sys.argv[1]
VK_API_VERSION = "5.103"
OUTPUT_FILE = "../../data/cities_vk.csv"
RETRY_LIMIT = 3
SLEEP_BETWEEN_REQUESTS = 3

def get_cities(city):
    url = f"https://api.vk.com/method/database.getCities"
    params = {
        "q": city,
        "access_token": TOKEN,
        "need_all": 0,
        "count": 1,
        "v": VK_API_VERSION,
    }

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            if "response" in data and "items" in data["response"]:
                return data["response"]["items"]
            else:
                print(f"⚠ Ошибка: нет данных для {city} | Ответ: {data}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса {city} (попытка {attempt + 1}): {e}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    return []

def main():
    settlements = pd.read_csv("../../data/PopulationData.csv")
    settlements = settlements[settlements["population"] > 30000].reset_index(drop=True)
    data_cities = []

    for city in tqdm(settlements["settlement"], desc="🔍 Обрабатываем города"):
        cities_info = get_cities(city)
        if cities_info:
            data_cities.extend(cities_info)

    df_cities = pd.DataFrame(data_cities)
    df_cities.rename(columns={"id": "CityID", "title": "CityName"}, inplace=True)

    df_cities.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Данные сохранены в {OUTPUT_FILE}")

if __name__ == "__main__":
    main()