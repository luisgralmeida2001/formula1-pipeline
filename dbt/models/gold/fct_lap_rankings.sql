-- models/gold/fct_lap_rankings.sql
--
-- Ranking de volta mais rápida por piloto em cada circuito.
-- Responde: quem é o mais rápido em cada pista?

{{ config(materialized='table') }}

SELECT
    r.circuit_short_name,
    r.country_name,
    r.name_acronym,
    r.full_name,
    r.team_name,
    r.fastest_lap_seconds,
    ROUND(r.fastest_lap_seconds / 60, 3)                    AS fastest_lap_minutes,
    RANK() OVER (
        PARTITION BY r.circuit_short_name
        ORDER BY r.fastest_lap_seconds ASC
    )                                                        AS lap_rank
FROM {{ ref('fct_race_results') }} r
WHERE r.fastest_lap_seconds IS NOT NULL
  AND r.fastest_lap_seconds > 0
