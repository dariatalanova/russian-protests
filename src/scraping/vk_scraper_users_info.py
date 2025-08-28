import pandas as pd
import requests
import json
import time
from tqdm import tqdm
import sys

TOKEN = sys.argv[1]
VK_API_VERSION = "5.139"
OUTPUT_FILE = "../../data/users_info.jsonl"
BATCH_SIZE = 500
MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 1

def GetIDs():
    df = pd.read_csv('../../data/posts_vk_filtered.csv')
    ids = list(set(df['OwnerID'].to_list()))
    user_ids = [num for num in ids if num > 0]
    return user_ids

def fetch_user_info(batch):
    url = "https://api.vk.com/method/users.get"
    params = {
        "user_ids": batch,
        "fields": "city",
        "access_token": TOKEN,
        "v": VK_API_VERSION
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "response" in data:
                return data["response"]

            print(f"⚠️ Ошибка API: {data}. Повтор {attempt}/{MAX_RETRIES}")
            time.sleep(5 * attempt)

        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса: {e}. Повтор {attempt}/{MAX_RETRIES}")
            time.sleep(5 * attempt)

    return []

def GetUsers():
    print("📥 Начинаем сбор данных о юзерах...")
    ids = GetIDs()
    batches = [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]

    print(f"🔹 Всего {len(batches)} батчей для обработки.")

    for batch in tqdm(batches, desc="Обработка батчей", unit="batch"):
        batch_str = ",".join(map(str, batch))
        users = fetch_user_info(batch_str)

        if not users:
            continue

        with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
            for user in users:
                res = {
                    "OwnerID": user["id"],
                    "CityID": user.get("city", {}).get("id"),
                    "CityName": user.get("city", {}).get("title"),
                    'FirstName': user.get("first_name"),
                    'LastName': user.get("last_name")
                }
                json.dump(res, file, ensure_ascii=False)
                file.write("\n")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print("✅ Сбор данных завершен!")


if __name__ == "__main__":
    GetUsers()