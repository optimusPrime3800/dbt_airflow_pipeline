-- Создание схем для dbt моделей
CREATE SCHEMA IF NOT EXISTS dbt_models;
CREATE SCHEMA IF NOT EXISTS marts;

-- Предоставление прав пользователю airflow
GRANT ALL ON SCHEMA dbt_models TO airflow;
GRANT ALL ON SCHEMA marts TO airflow;

-- Установка прав по умолчанию для новых таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA dbt_models GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO airflow;

-- Создание расширения для JSON поддержки (если ещё не создано)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";