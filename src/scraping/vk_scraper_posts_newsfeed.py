import json
import requests
import time
from datetime import datetime
import threading
import concurrent.futures
from tqdm import tqdm
import sys

TOKENS = sys.argv[1:]
VK_API_VERSION = "5.103"
OUTPUT_FILE = '../../data/posts_vk.jsonl'
RETRY_LIMIT = 3
SLEEP_BETWEEN_REQUESTS = 60

write_lock = threading.Lock()

def retry_request(url, params, retries=RETRY_LIMIT, delay=SLEEP_BETWEEN_REQUESTS, word=""):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f'⚠️ [{datetime.now().strftime("%H:%M:%S")}] Ошибка запроса для "{word}" (попытка {attempt}/{retries}): {e}')
            if attempt < retries:
                time.sleep(delay)
    print(f'❌ [{datetime.now().strftime("%H:%M:%S")}] Пропуск "{word}" после {retries} попыток')
    return None


def GetNews(start_time, end_time, word, next_from, token):
    url = 'https://api.vk.com/method/newsfeed.search'
    params = {
        "q": word,
        "extended": 1,
        "count": 200,
        "access_token": token,
        "start_time": start_time,
        "end_time": end_time,
        "start_from": next_from,
        "v": VK_API_VERSION,
    }

    for attempt in range(1, 4):  # до 3 попыток при пустых постах
        response = retry_request(url, params, word=word)
        if not response:
            return None, end_time + 1

        try:
            response_data = response.json().get('response', {})
        except json.JSONDecodeError:
            print(f'❌ [{datetime.now().strftime("%H:%M:%S")}] Некорректный JSON для "{word}"')
            return None, end_time + 1

        items = response_data.get('items', [])
        next_from = response_data.get('next_from', '')

        if items:
            with write_lock:
                with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
                    for item in items:
                        res = {
                            'KeyWord': word,
                            'OwnerID': item['owner_id'],
                            'PostText': item.get('text', ''),
                            'PostID': item['id'],
                            'PostDate': datetime.fromtimestamp(item.get('date', end_time)).strftime('%d-%m-%Y')
                        }
                        json.dump(res, f, ensure_ascii=False)
                        f.write('\n')

            if next_from:
                return next_from, end_time

            last_post_time = items[-1].get('date', end_time)
            return None, last_post_time

        else:
            print(f'⚠️ [{datetime.now().strftime("%H:%M:%S")}] Нет постов для "{word}" (попытка {attempt}/3), ждем {SLEEP_BETWEEN_REQUESTS} сек...')
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f'⏭ [{datetime.now().strftime("%H:%M:%S")}] Пропуск "{word}" после 3 попыток без постов')
    return None, end_time + 1

def process_word(word, token, progress_bar):
    print(f"🔍 Начинаем поиск по слову: {word}")
    next_from = ''
    start_time = 1735726298
    end_time = 1753767181

    while True:
        prev_end_time = end_time
        next_from, end_time = GetNews(start_time, end_time, word, next_from, token)

        if not next_from:
            if prev_end_time == end_time:
                print(f'🏁 Завершили поиск по слову "{word}"')
                break
            else:
                print(f'🔽 "{word}": следующий диапазон {datetime.fromtimestamp(end_time).strftime("%d-%m-%Y")}')

        progress_bar.update(1)


def main():
    words = ["пикет", "обращение к президенту"]

    if len(TOKENS) >= len(words):
        tokens = TOKENS[:len(words)]
    else:
        tokens = [TOKENS[i % len(TOKENS)] for i in range(len(words))]

    progress_bars = [
        tqdm(desc=word, position=i, leave=True, mininterval=1.0, dynamic_ncols=True)
        for i, word in enumerate(words)
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(words)) as executor:
        futures = [
            executor.submit(process_word, words[i], tokens[i], progress_bars[i])
            for i in range(len(words))
        ]
        concurrent.futures.wait(futures)

    for bar in progress_bars:
        bar.close()

if __name__ == "__main__":
    main()