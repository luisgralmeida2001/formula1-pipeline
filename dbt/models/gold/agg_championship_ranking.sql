-- models/gold/agg_championship_ranking.sql
--
-- Ranking estimado do campeonato baseado em posições finais.
-- Usa sistema de pontos real da F1:
-- 1º=25, 2º=18, 3º=15, 4º=12, 5º=10, 6º=8, 7º=6, 8º=4, 9º=2, 10º=1

{{ config(materialized='table') }}

WITH points_per_race AS (
    SELECT
        driver_number,
        name_acronym,
        full_name,
        team_name,
        circuit_short_name,
        date_start,
        final_position,
        CASE final_position
            WHEN 1  THEN 25
            WHEN 2  THEN 18
            WHEN 3  THEN 15
            WHEN 4  THEN 12
            WHEN 5  THEN 10
            WHEN 6  THEN 8
            WHEN 7  THEN 6
            WHEN 8  THEN 4
            WHEN 9  THEN 2
            WHEN 10 THEN 1
            ELSE 0
        END AS points
    FROM {{ ref('fct_race_results') }}
    WHERE final_position IS NOT NULL
)

SELECT
    driver_number,
    name_acronym,
    full_name,
    team_name,
    COUNT(circuit_short_name)   AS races_completed,
    SUM(points)                 AS total_points,
    SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN final_position <= 3 THEN 1 ELSE 0 END) AS podiums,
    SUM(CASE WHEN final_position <= 10 THEN 1 ELSE 0 END) AS points_finishes,
    ROUND(AVG(final_position), 2) AS avg_position,
    RANK() OVER (ORDER BY SUM(points) DESC) AS championship_rank
FROM points_per_race
GROUP BY driver_number, name_acronym, full_name, team_name
ORDER BY total_points DESC
