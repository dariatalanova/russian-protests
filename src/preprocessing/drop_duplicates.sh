if [ ! -d "../../venv" ]; then
    cd ../.. && python3 -m venv venv && cd - > /dev/null
fi

source ../../venv/bin/activate

if [ -f "../../requirements.txt" ]; then
    pip install -r ../../requirements.txt
fi

echo -n "Токен OpenAI: "
read token

python3 posts_to_embeddings.py $token;
python3 cluster_embeddings_by_time.py;
python3 remove_similar_posts.py