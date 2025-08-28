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
    echo -n "Токен Gemini №$i (Enter - завершить): "
    read t
    [[ -z "$t" ]] && break
    tokens+=("$t")
    ((i++))
done

python3 query_gemini.py ${tokens[@]}