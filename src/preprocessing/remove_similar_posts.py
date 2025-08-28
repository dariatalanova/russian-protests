import pandas as pd

def main():
    df = pd.read_csv('../../data/posts_vk_clusters.csv', low_memory=False)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')


    df_clusters = df[df['cluster'] != -1]
    df_no_cluster = df[df['cluster'] == -1]

    df_clusters_sorted = df_clusters.sort_values(by='date')
    df_clusters_unique = df_clusters_sorted.drop_duplicates(subset=['cluster'], keep='first')

    df_final = pd.concat([df_no_cluster, df_clusters_unique]).sort_values(by='date').reset_index(drop=True)

    df_final.to_csv('data/posts_vk_deduplicated.csv', index=False)

    print(f"✅ Удалены дубликаты! Итоговое количество строк: {len(df_final)}")

if __name__ == "__main__":
    main()