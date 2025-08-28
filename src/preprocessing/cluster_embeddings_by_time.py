import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import json
from numba import jit
from tqdm import tqdm

def GetClusters(input_file='data/posts_vk_embeddings.csv',
                output_file='data/posts_vk_clusters.csv', window_days=10, threshold=0.75):
    print("Загружаем данные...")
    df = pd.read_csv(input_file, low_memory=False)
    df['date'] = pd.to_datetime(df['PostDate'], format='%d-%m-%Y', errors='coerce')
    df = df.dropna(subset=['date'])
    df.sort_values(by='date', inplace=True)
    df.reset_index(drop=True, inplace=True)

    print("Конвертируем эмбеддинги...")
    df['embedding'] = df['embedding'].map(json.loads)
    tfidf_matrix = np.array(df["embedding"].to_list())

    print("Разбиваем на временные окна...")
    min_date, max_date = df['date'].min(), df['date'].max()
    data, current_date = [], min_date
    with tqdm(total=(max_date - min_date).days // window_days + 1, desc="Разбиение по датам") as pbar:
        while current_date <= max_date:
            next_date = current_date + pd.Timedelta(days=window_days)
            df_group = df[(df['date'] >= current_date) & (df['date'] < next_date)]
            if not df_group.empty:
                data.append(df_group.index.to_numpy())
            current_date = next_date
            pbar.update(1)

    cluster_labels = np.full(len(df), -1, dtype=int)
    cluster_counter = 0

    @jit(nopython=True)
    def assign_clusters(similarity_matrix, threshold):
        n = len(similarity_matrix)
        labels = np.full(n, -2, dtype=int)
        cluster_id = 0
        for ind in range(n):
            if labels[ind] == -2:
                similar_posts = np.where(similarity_matrix[ind] > threshold)[0]
                has_doubles = False
                for i in similar_posts:
                    if i != ind and labels[i] == -2:
                        has_doubles = True
                        labels[i] = cluster_id
                if has_doubles:
                    labels[ind] = cluster_id
                    cluster_id += 1
                else:
                    labels[ind] = -1
            else:
                cluster_id_existing = labels[ind]
                similar_posts = np.where(similarity_matrix[ind] > threshold)[0]
                for i in similar_posts:
                    if i != ind and labels[i] == -2:
                        labels[i] = cluster_id_existing
        return labels

    print("Начинаем кластеризацию...")
    with tqdm(total=len(data), desc="Кластеризация") as pbar:
        for inds in data:
            embeddings = np.array([tfidf_matrix[i] for i in inds])
            similarity_matrix = cosine_similarity(embeddings)
            labels = assign_clusters(similarity_matrix, threshold)
            for i, lbl in enumerate(labels):
                if lbl != -1:
                    cluster_labels[inds[i]] = cluster_counter + lbl
            cluster_counter += len(set(labels)) - 1
            pbar.update(1)

    df["cluster"] = cluster_labels
    df.drop(columns=['embedding'], inplace=True)
    df.to_csv(output_file, index=False)
    print(f"Файл с кластерами сохранен: {output_file}")

def main():
    GetClusters()

if __name__ == "__main__":
    main()