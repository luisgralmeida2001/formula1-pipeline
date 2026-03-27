-- models/gold/fct_team_pit_performance.sql
--
-- Performance de pit stops por equipe na temporada.
-- Responde: qual equipe tem a estratégia de pit stop mais eficiente?

{{ config(materialized='table') }}

SELECT
    team_name,
    COUNT(*)                            AS total_pit_stops,
    ROUND(MIN(avg_pit_seconds), 3)      AS fastest_avg_pit,
    ROUND(AVG(avg_pit_seconds), 3)      AS season_avg_pit,
    ROUND(MAX(avg_pit_seconds), 3)      AS slowest_avg_pit,
    ROUND(MIN(fastest_pit_seconds), 3)  AS absolute_fastest_pit,
    RANK() OVER (
        ORDER BY AVG(avg_pit_seconds) ASC
    )                                   AS pit_efficiency_rank
FROM {{ ref('fct_pit_stop_analysis') }}
WHERE team_name IS NOT NULL
GROUP BY team_name
ORDER BY season_avg_pit ASC
