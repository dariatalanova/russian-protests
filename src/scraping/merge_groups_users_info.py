import pandas as pd

print("Начинаем объединение информации о группах и юзерах...")

OUTPUT_FILE = "../../data/posts_vk_filtered_merged.csv"

df_posts = pd.read_csv("../../data/posts_vk_filtered.csv")
df_groups_info = pd.read_json("../../data/groups_info.jsonl", lines=True)
df_users_info = pd.read_json("../../data/users_info.jsonl", lines=True)

df_groups_info = df_groups_info[["OwnerID", "PublicName", "CityID", "CityName", "PublicDescription"]].rename(
    columns={"CityName": "CityName_Public", "CityID": "CityID_Public"}
)
df_users_info = df_users_info[["OwnerID", "CityID", "CityName", "FirstName", "LastName"]].rename(
    columns={"CityName": "CityName_User", "CityID": "CityID_User"}
)


df_merged = (
    df_posts
    .merge(df_groups_info, on="OwnerID", how="left")
    .merge(df_users_info, on="OwnerID", how="left")
).drop_duplicates(['OwnerID', 'PostID'])

df_merged.to_csv(OUTPUT_FILE, index=False)
print(f"Таблица сохранена в {OUTPUT_FILE}")