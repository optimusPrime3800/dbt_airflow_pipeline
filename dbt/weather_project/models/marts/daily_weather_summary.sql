{{
    config(
        materialized='table',
        unique_key='city_date'
    )
}}

WITH weather_metrics AS (
    SELECT
        city,
        date,
        AVG(temperature) as avg_temperature,
        MIN(temperature) as min_temperature,
        MAX(temperature) as max_temperature,
        AVG(humidity) as avg_humidity,
        AVG(pressure) as avg_pressure,
        AVG(wind_speed) as avg_wind_speed,
        MODE() WITHIN GROUP (ORDER BY weather_main) as most_common_weather,
        COUNT(*) as measurements_count
    FROM {{ ref('stg_weather') }}
    GROUP BY city, date
)

SELECT
    city,
    date,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    avg_pressure,
    avg_wind_speed,
    most_common_weather,
    measurements_count,
    -- Категория дня на основе средней температуры
    CASE 
        WHEN avg_temperature < 0 THEN 'Freezing Day'
        WHEN avg_temperature < 10 THEN 'Cold Day'
        WHEN avg_temperature < 20 THEN 'Mild Day'
        WHEN avg_temperature < 30 THEN 'Warm Day'
        ELSE 'Hot Day'
    END as day_category,
    NOW() as updated_at
FROM weather_metrics