import os
import json
import requests
from datetime import datetime, timedelta
from io import BytesIO
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_values

# === Настройки ===
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_RAW = "bronze"

PG_HOST = "postgres_data"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DATABASE = "moex"


def fetch_and_upload_moex(**context):
    """Загружает данные по годам и склеивает"""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    url = "https://iss.moex.com/iss/history/engines/stock/markets/index/boards/RTSI/securities/RTSI.json"
    
    all_records = []
    years = range(2020, datetime.now().year + 1)
    
    for year in years:
        params = {
            "from": f"{year}-01-01",
            "till": f"{year}-12-31",
            "limit": 100
        }
        
        try:
            print(f"Загрузка за {year} год...")
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            history = data.get('history', {})
            columns = history.get('columns', [])
            rows = history.get('data', [])
            
            for row in rows:
                record = dict(zip(columns, row))
                all_records.append(record)
            
            print(f"  Загружено {len(rows)} записей за {year}")
            
        except Exception as e:
            print(f"  Ошибка за {year}: {e}")
            continue

    if not all_records:
        print("Нет данных")
        return

    # Удаляем дубликаты по дате
    unique_records = {}
    for record in all_records:
        date = record.get('TRADEDATE')
        if date and date not in unique_records:
            unique_records[date] = record
    
    final_records = list(unique_records.values())
    final_records.sort(key=lambda x: x.get('TRADEDATE', ''))

    # Сохраняем в MinIO
    payload = {
        "instrument": "rts_index",
        "assetcode": "RTSI",
        "fetched_at": datetime.now().isoformat(),
        "data": final_records
    }

    json_str = json.dumps(payload, ensure_ascii=False, indent=2)
    object_name = f"moex/{datetime.now().strftime('%Y/%m/%d')}/rts_index.json"

    client.put_object(
        BUCKET_RAW,
        object_name,
        data=BytesIO(json_str.encode('utf-8')),
        length=len(json_str),
        content_type='application/json'
    )
    print(f"✅ Сохранено {len(final_records)} уникальных записей в MinIO")

    context['task_instance'].xcom_push(key='minio_path', value=object_name)


def load_to_postgres(**context):
    """Загружает данные из JSON в PostgreSQL"""
    ti = context['task_instance']
    minio_path = ti.xcom_pull(key='minio_path', task_ids='fetch_and_upload_moex')

    if not minio_path:
        print("❌ Нет пути к файлу")
        return

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    try:
        obj = client.get_object(BUCKET_RAW, minio_path)
        content = obj.read()
        obj.close()
        data = json.loads(content.decode('utf-8'))
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return

    if not data.get('data'):
        print("Нет данных")
        return

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DATABASE
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS moex_prices (
            trade_date DATE PRIMARY KEY,
            close DECIMAL(10,2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    values = []
    for item in data['data']:
        trade_date = item.get('TRADEDATE')
        close = item.get('CLOSE')
        if trade_date and close:
            values.append((trade_date, close))

    if not values:
        print("Нет валидных данных")
        return

    cur.execute("TRUNCATE TABLE moex_prices")
    execute_values(cur,
        "INSERT INTO moex_prices (trade_date, close) VALUES %s",
        values
    )
    conn.commit()
    print(f"✅ Загружено {len(values)} записей")

    cur.close()
    conn.close()


default_args = {
    'owner': 'analytics_engineer',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='moex_futures_historical',
    default_args=default_args,
    description='Загрузка индекса РТС с MOEX (по годам)',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['moex', 'rts']
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_and_upload_moex',
        python_callable=fetch_and_upload_moex,
        provide_context=True
    )

    task_load_pg = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    task_fetch >> task_load_pg