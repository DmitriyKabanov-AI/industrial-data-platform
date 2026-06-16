import json
import psycopg2
import re

# Читаем JSON с правильной обработкой
with open('rts_index.json', 'r', encoding='utf-8') as f:
    content = f.read()

# Парсим JSON (несмотря на странные символы)
try:
    data = json.loads(content)
except Exception:
    # Если не парсится, пробуем игнорировать ошибки
    
    content_clean = re.sub(r'[^\x00-\x7F]+', ' ', content)
    data = json.loads(content_clean)

records = data.get('data', [])
print(f"Найдено {len(records)} записей")

# Подключаемся к БД
conn = psycopg2.connect(
    host='localhost', port=5433, user='postgres',
    password='postgres', database='moex'
)
cur = conn.cursor()
cur.execute('TRUNCATE TABLE moex_prices')

count = 0
for item in records:
    trade_date = item.get('TRADEDATE')
    close = item.get('CLOSE')
    if trade_date and close:
        cur.execute(
            'INSERT INTO moex_prices (trade_date, close) VALUES (%s, %s) ON CONFLICT DO NOTHING',
            (trade_date, close)
        )
        count += 1

conn.commit()
print(f"✅ Загружено {count} записей")
cur.close()
conn.close()