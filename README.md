# Weather ETL Pipeline with Apache Airflow & dbt

Production-ready ETL пайплайн для 
1. сбора, 
2. трансформации
3. анализа погодных данных с 
# стек технологий
1. **Apache Airflow** (оркестрация)
2. **dbt** (трансформация)
3. **PostgreSQL** (хранилище)


Всё завернуто в **Docker** для воспроизводимости.

## Архитектура проекта
 

OPENWEATHER API  -----> AIRFLOW  ----> PostgreSQL ------> DBT





## Что делает пайплайн

1. **Каждый час** запрашивает текущую погоду для 5 городов (Лондон, Париж, Берлин, Мадрид, Рим)
2. Сохраняет сырые данные в таблицу `raw_weather_data`
3. Трансформирует данные через dbt (очистка, агрегация, категоризация)
4. Создает аналитическую витрину `daily_weather_summary`
5. Данные готовы для визуализации в BI-инструментах




## Старт

### 1. Клонируйте репозиторий
```bash
git clone https://github.com/optimusPrime3800/dbt_airflow_pipeline.git

cd weather-pipeline

2. Настройте переменные окружения

Создайте файл .env:
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=weather_db

# OpenWeather API (получите ключ на https://openweathermap.org/api)
OPENWEATHER_API_KEY=your_api_key_here
OPENWEATHER_CITIES=London,Paris,Berlin,Madrid,Rome

3. Запустите контейнеры
docker-compose up -d

4. Инициализируйте Airflow (только при первом запуске)
# Инициализация базы данных
docker exec weather_airflow_scheduler airflow db init

# Создание пользователя
docker exec weather_airflow_scheduler airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

5. Откройте Airflow UI

    URL: http://localhost:8080

    Логин: admin

    Пароль: admin

6. Запустите DAG

Через UI:

    Нажмите на weather_pipeline

    Нажмите Play → Trigger DAG

Через CLI:
    docker exec weather_airflow_scheduler airflow dags trigger weather_pipeline 