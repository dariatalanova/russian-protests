import pandas as pd
import numpy as np
from openai import OpenAI
import tiktoken
import os
import json
from tqdm import tqdm
import sys

TOKEN = sys.argv[1]
OUTPUT_FILE = "../../data/posts_vk_embeddings.csv"

def CleanDF():
    def count_tokens(text, model="cl100k_base"):
        encoder = tiktoken.get_encoding(model)
        return len(encoder.encode(text))

    df = pd.read_csv("../../data/posts_vk_filtered_merged.csv", low_memory=False)

    token_limit = 8192
    df["num_tokens"] = df["PostText"].apply(count_tokens)
    df = df[df["num_tokens"] <= token_limit].drop(columns=["num_tokens"])

    df["embedding"] = np.nan

    print("✔ DataFrame очищен и готов к обработке")
    return df

def get_embedding(texts, model="text-embedding-3-small"):
    texts = [text.replace("\n", " ").strip() for text in texts]
    texts = [t if len(t) > 0 else "none" for t in texts]

    client = OpenAI(api_key=TOKEN)
    data = client.embeddings.create(input=texts, model=model).data
    embeddings = [np.round(d.embedding, 8).tolist() for d in data]

    return embeddings

def main():
    batch_size = 100
    df = CleanDF()

    if not os.path.exists(OUTPUT_FILE):
        df.head(0).to_csv(OUTPUT_FILE, index=False)

    num_batches = (len(df) + batch_size - 1) // batch_size

    with tqdm(total=num_batches, desc="📊 Обработка батчей", unit="batch") as pbar:
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size].copy()
            batch_texts = batch_df["PostText"].tolist()
            batch_embeddings = get_embedding(batch_texts)

            if len(batch_embeddings) != len(batch_df):
                print(
                    f"⚠️ Ошибка: {len(batch_embeddings)} эмбеддингов на {len(batch_df)} строк. Пропускаем."
                )
                continue

            batch_df["embedding"] = batch_embeddings
            batch_df["embedding"] = batch_df["embedding"].apply(json.dumps)

            batch_df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

            pbar.update(1)
            pbar.set_postfix({"готово строк": i + len(batch_df)})

    print("✅ Готово! Эмбеддинги сохранены")


if __name__ == "__main__":
    main()