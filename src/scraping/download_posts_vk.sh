if [ ! -d "../../venv" ]; then
    cd ../.. && python3 -m venv venv && cd - > /dev/null
fi

source ../../venv/bin/activate

if [ -f "../../requirements.txt" ]; then
    pip install -r ../../requirements.txt
fi

tokens=()
i=1
while true; do
    echo -n "Токен ВК №$i (Enter - завершить): "
    read t
    [[ -z "$t" ]] && break
    tokens+=("$t")
    ((i++))
done

# Загрузка постов из пабликов
python3 vk_search_cities.py ${tokens[1]};
python3 vk_search_groups.py ${tokens[1]};
python3 vk_scraper_posts_groups.py ${tokens[@]};

# Загрузка постов из ленты новостей
python3 vk_scraper_posts_newsfeed.py ${tokens[@]};

# Очистка постов
python3 clean_posts_by_keywords.py;

# Загрузка и объединение информации о группах и юзерах
python3 vk_scraper_groups_info.py ${tokens[1]};
python3 vk_scraper_users_info.py ${tokens[1]};
python3 merge_groups_users_info.py