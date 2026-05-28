
from datetime import datetime, timedelta
import requests
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_KEY = "61c95192200946683716b13166263f31" 
CITIES = ["London", "Paris", "Berlin", "Madrid", "Rome"]

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



def fetch_weather_data(**context):
    """загрузка данных из OpenWeather API в PostgreSQL"""
    execution_date = context['execution_date']
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            
            insert_sql = """
                INSERT INTO raw_weather_data 
                (city, date, temperature, feels_like, pressure, humidity, 
                 weather_main, weather_description, wind_speed, raw_data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (city, date) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    feels_like = EXCLUDED.feels_like,
                    pressure = EXCLUDED.pressure,
                    humidity = EXCLUDED.humidity,
                    weather_main = EXCLUDED.weather_main,
                    weather_description = EXCLUDED.weather_description,
                    wind_speed = EXCLUDED.wind_speed,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = NOW()
            """
            
            hook.run(insert_sql, parameters=[
                city,
                execution_date.date(),
                data['main']['temp'],
                data['main']['feels_like'],
                data['main']['pressure'],
                data['main']['humidity'],
                data['weather'][0]['main'],
                data['weather'][0]['description'],
                data['wind']['speed'],
                json.dumps(data)
            ])
            
            print(f" {city}: температура {data['main']['temp']}°C, {data['weather'][0]['description']}")
            
        except Exception as e:
            print(f"Ошибка для города {city}: {e}")
            raise


with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Пайплайн погодных данных из OpenWeather API',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'api', 'postgres']
) as dag:
    
  
    create_raw_table = PostgresOperator(
        task_id='create_raw_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                date DATE,
                temperature FLOAT,
                feels_like FLOAT,
                pressure INTEGER,
                humidity INTEGER,
                weather_main VARCHAR(50),
                weather_description VARCHAR(100),
                wind_speed FLOAT,
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP,
                UNIQUE(city, date)
            );
            
            CREATE INDEX IF NOT EXISTS idx_raw_weather_city_date 
            ON raw_weather_data(city, date);
            
            CREATE INDEX IF NOT EXISTS idx_raw_weather_date 
            ON raw_weather_data(date);
        """
    )
    
    
    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True,
    )
    
   
    check_data = PostgresOperator(
        task_id='check_data',
        postgres_conn_id='postgres_default',
        sql="""
            SELECT CASE 
                WHEN COUNT(*) > 0 THEN 'данные загружены: ' || COUNT(*) || ' записей'
                ELSE 'таблица пуста'
            END as status
            FROM raw_weather_data;
        """
    )
    
    
    create_raw_table >> fetch_weather >> check_data