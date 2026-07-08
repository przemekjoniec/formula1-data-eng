-- Databricks notebook source

CREATE OR REPLACE VIEW formula1.gold.v_driver_standings
AS
WITH driver_session_summary AS(
    SELECT
        r.season,
        d.driver_id,
        d.driver_name,
        d.nationality,
        COUNT(*) as race_starts,
        SUM(r.points) AS total_points,
        COUNT_IF(r.is_win) AS number_of_wins,
        COUNT_IF(r.is_podium) AS number_of_podiums
    FROM formula1.gold.fact_session_results r
    JOIN formula1.gold.dim_drivers d
        ON r.driver_id = d.driver_id
    GROUP BY r.season,
        d.driver_id,
        d.driver_name,
        d.nationality)
SELECT season,
    driver_name,
    driver_id,
    RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
    nationality,
    race_starts,
    total_points,
    number_of_wins,
    number_of_podiums
FROM driver_session_summary

-- COMMAND ----------

SELECT * FROM formula1.gold.v_driver_standings WHERE season = 2025