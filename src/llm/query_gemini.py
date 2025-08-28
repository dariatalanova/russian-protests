import google.generativeai as genai
import pandas as pd
import json
import concurrent.futures
import time
import threading
import sys
from tqdm import tqdm

TOKENS = sys.argv[1:]
MAX_RETRIES = 3
ERROR_DELAY = 60
OUTPUT_FILE = '../../data/posts_vk_final.jsonl'

write_lock = threading.Lock()

def generate_response(api_key, text):
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash-lite")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = model.generate_content(f"Тебе дан текст: {text}."
                                              f"Если в тексте есть информация о протестной акции,"
                                              f"то верни 1")
            return response.text.strip()
        except Exception as e:
            print(f"❌ Ошибка запроса (попытка {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(ERROR_DELAY * attempt)
            else:
                return "error"

def process_row(row, index, api_key, progress):
    row_dict = dict(row)
    text_to_send = str(row.get('PostText', ''))[:3000]
    response = generate_response(api_key, text_to_send)
    row_dict['response'] = response

    with write_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
            json.dump(row_dict, file, ensure_ascii=False)
            file.write("\n")
            file.flush()

    progress.update(1)

def main():
    df = pd.read_csv('../../data/posts_vk_clusters.csv')
    num_rows = len(df)

    tokens_for_rows = [TOKENS[i % len(TOKENS)] for i in range(num_rows)]

    with tqdm(total=num_rows, desc="Обработка постов с AI") as progress:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TOKENS)) as executor:
            futures = []
            for i, row in df.iterrows():
                api_key = tokens_for_rows[i]
                futures.append(executor.submit(process_row, row, i, api_key, progress))
            concurrent.futures.wait(futures)

    print("✅ Обработка завершена")

if __name__ == "__main__":
    main()