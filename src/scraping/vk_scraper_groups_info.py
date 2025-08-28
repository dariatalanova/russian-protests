import pandas as pd
import requests
import json
import time
from tqdm import tqdm
import sys

TOKEN = sys.argv[1]
VK_API_VERSION = "5.139"
OUTPUT_FILE = "../../data/groups_info.jsonl"
BATCH_SIZE = 500
MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 1

def GetIDs():
    df = pd.read_csv('../../data/posts_vk_filtered.csv')
    ids = list(set(df['OwnerID'].to_list()))
    public_ids = [abs(num) for num in ids if num < 0]
    return public_ids

def fetch_public_info(batch):
    url = "https://api.vk.com/method/groups.getById"
    params = {
        "group_ids": batch,
        "fields": "description,city",
        "access_token": TOKEN,
        "v": VK_API_VERSION
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "response" in data:
                return data["response"]["groups"]

            print(f"⚠️ Ошибка API: {data}. Повтор {attempt}/{MAX_RETRIES}")
            time.sleep(5 * attempt)

        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса: {e}. Повтор {attempt}/{MAX_RETRIES}")
            time.sleep(5 * attempt)

    return []

def GetPublics():
    public_ids = GetIDs()
    if not public_ids:
        print("⚠️ Нет пабликов для обработки.")
        return

    print(f"📥 Начинаем сбор данных о {len(public_ids)} пабликах...")

    batches = [public_ids[i:i + BATCH_SIZE] for i in range(0, len(public_ids), BATCH_SIZE)]
    batch_strings = [",".join(map(str, batch)) for batch in batches]

    with tqdm(total=len(batch_strings), desc="Обработка батчей", unit="batch") as pbar:
        for batch in batch_strings:
            publics = fetch_public_info(batch)
            if not publics:
                pbar.write(f"⚠️ Пропускаем батч, данных нет.")
                pbar.update(1)
                continue

            with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
                for public in publics:
                    res = {
                        "OwnerID": -abs(public["id"]),
                        "PublicName": public["name"],
                        "CityID": public.get("city", {}).get("id"),
                        "CityName": public.get("city", {}).get("title"),
                        "PublicDescription": public.get("description")
                    }
                    json.dump(res, file, ensure_ascii=False)
                    file.write("\n")

            time.sleep(SLEEP_BETWEEN_REQUESTS)
            pbar.update(1)

    print("✅ Сбор данных завершен!")


if __name__ == "__main__":
    GetPublics()