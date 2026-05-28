{{
    config(
        materialized='view',
        unique_key='city_date'
    )
}}

SELECT
    city,
    date,
    temperature,
    feels_like,
    pressure,
    humidity,
    weather_main,
    weather_description,
    wind_speed,
    -- Добавляем категории температуры
    CASE 
        WHEN temperature < 0 THEN 'Freezing'
        WHEN temperature < 10 THEN 'Cold'
        WHEN temperature < 20 THEN 'Mild'
        WHEN temperature < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    -- Добавляем временные метки
    created_at,
    updated_at
FROM {{ source('raw', 'weather_data') }}