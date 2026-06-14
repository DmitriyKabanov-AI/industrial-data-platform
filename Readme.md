# 🎯 О проекте

**End-to-end Data Platform** для сбора, обработки и визуализации финансовых данных (индекс РТС) с использованием современного стека Big Data технологий.

---

## 🏗️ Архитектура

```text
MOEX API → Airflow → MinIO → PostgreSQL → Grafana
(Источник) (Оркестрация) (Data Lake) (DWH) (BI)
```

### Компоненты
| Компонент | Технология | Назначение |
|---|---|---|
| **Orchestration** | Apache Airflow | ETL/ELT пайплайны |
| **Data Lake** | MinIO (S3) | Хранение сырых JSON |
| **Data Warehouse** | PostgreSQL | Аналитическое хранилище |
| **Visualization** | Grafana | Дашборды и мониторинг |
| **Streaming** | Apache Kafka + UI | Потоковая обработка *(готов)* |
| **Big Data** | Apache Spark | Распределённые вычисления *(готов)* |

---

## 📊 Бизнес-ценность

* 🤖 **Автоматизация отчётности** — ежедневная загрузка данных без ручного труда.
* 🗄️ **Data Lake** — единое хранилище сырых данных для повторного использования.
* 📉 **Исторический анализ** — данные с 2020 года для трендов и прогнозов.
* 📈 **Real-time мониторинг** — дашборды в Grafana для оперативного принятия решений.

---

## 🚀 Быстрый старт

### Требования
* Docker Desktop 4.30+
* WSL2 (для Windows)
* 8+ GB RAM

### Установка
```bash
# Клонировать репозиторий
git clone https://github.com/yourusername/industrial-data-platform
cd industrial-data-platform

# Запустить все сервисы
docker compose up -d

# Загрузить данные
docker exec -it industrial-data-platform-airflow-webserver-1 airflow dags trigger moex_futures_historical
```

### Доступ к сервисам
| Сервис | URL | Логин / Пароль |
|---|---|---|
| **Airflow** | http://localhost:8087 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9006 | `minioadmin` / `minioadmin` |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Kafka UI** | http://localhost:8086 | без пароля |
| **Spark Master** | http://localhost:8085 | без пароля |

---

## 🔄 ETL/ELT Пайплайн

```yaml
Extract:
  - Источник: MOEX ISS API
  - Формат: JSON
  - Период: 2020 - настоящее время

Transform:
  - Очистка данных
  - Отбор полей (TRADEDATE, CLOSE)
  - Дедупликация

Load:
  - MinIO (bronze): сырые JSON
  - PostgreSQL (moex_prices): аналитическая таблица
```

---

## 📁 Структура проекта

```text
industrial-data-platform/
├── dags/
│   └── moex_futures_pipeline.py    # Airflow DAG
├── spark_jobs/
│   └── process_moex.py             # Spark обработка
├── docker-compose.yml              # Инфраструктура
├── Dockerfile                      # Кастомный Airflow
├── .env                            # Переменные окружения
└── README.md
```

---

## 🛠️ Технологический стек

| Категория | Технологии |
|---|---|
| **Языки** | Python 3.12 |
| **Библиотеки** | `pandas`, `SQLAlchemy`, `requests`, `minio`, `psycopg2` |
| **Базы данных** | PostgreSQL, ClickHouse *(опционально)* |
| **Data Lake** | MinIO (S3-compatible) |
| **Оркестрация** | Apache Airflow |
| **Big Data** | Apache Spark, Apache Kafka |
| **Визуализация** | Grafana, Plotly |
| **Контейнеризация** | Docker, Docker Compose |

---

## 📊 Дашборд Grafana

![Grafana Dashboard](https://screenshots/grafana_dashboard.png)

**Запрос:**
```sql
SELECT trade_date as time, close::float as value 
FROM moex_prices 
ORDER BY trade_date;
```

---

## 🔍 Мониторинг и алерты

* 🌬️ **Airflow:** логи задач, retry policy, email уведомления.
* 📊 **Grafana:** визуальный мониторинг метрик.
* 📡 **Kafka UI:** мониторинг стриминга.

---

## 📈 Масштабирование

| Компонент | Текущий стек | При масштабировании |
|---|---|---|
| **Batch** | Airflow | Airflow + Spark |
| **Streaming** | - | Kafka + Spark Streaming |
| **Storage** | MinIO | MinIO Cluster / S3 |
| **DWH** | PostgreSQL | ClickHouse / Snowflake |

---

## 🐛 Известные проблемы и решения

| Проблема | Решение |
|---|---|
| MOEX API обрывает JSON | Постраничная загрузка, дедупликация |
| MinIO Console не открывается | Использовать `mc` CLI или перезапустить Docker |
| Airflow не отвечает | `docker compose restart airflow-webserver` |

---

## 🤝 Вклад в проект
Проект Open Source. Контрибьюции приветствуются!

## 📝 Лицензия
MIT