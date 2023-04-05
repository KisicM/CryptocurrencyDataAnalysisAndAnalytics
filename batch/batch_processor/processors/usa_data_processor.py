"""
4. Accidents by visibility condition
5. Accidents by road type

6. Top 10 cities with the highest number of accidents:
sql


7.Accidents per weekday:


8. Accidents and weather conditions
SELECT weather_condition, COUNT(*) AS accidents_per_weather_condition
FROM accidents
GROUP BY weather_condition;


9. Percentage of accidents that happened in case when the temperature is above average
WITH avg_temperature AS (
  SELECT AVG(temperature) AS value
  FROM accidents
),
accidents_above_avg_temp AS (
  SELECT COUNT(*) AS value
  FROM accidents
  WHERE temperature > (SELECT value FROM avg_temperature)
),
total_accidents AS (
  SELECT COUNT(*) AS value
  FROM accidents
)

SELECT
  (accidents_above_avg_temp.value::float / total_accidents.value::float) * 100 AS percentage_of_accidents_above_avg_temp
FROM accidents_above_avg_temp, total_accidents;
"""