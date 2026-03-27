# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold
# MAGIC
# MAGIC Lê as tabelas Silver e cria os modelos analíticos Gold.
# MAGIC
# MAGIC **Camada Gold:** dados agregados, prontos para responder perguntas de negócio.
# MAGIC
# MAGIC Modelos gerados:
# MAGIC - `fct_race_results` — resultado de cada piloto por corrida
# MAGIC - `fct_pit_stop_analysis` — análise de pit stops por equipe e circuito
# MAGIC - `dim_drivers` — dimensão de pilotos com info da temporada
# MAGIC - `agg_driver_performance` — performance acumulada por piloto na temporada

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

# Lê as tabelas Silver salvas pelo notebook 01
df_sessions  = spark.table("f1_silver.sessions")
df_drivers   = spark.table("f1_silver.drivers")
df_laps      = spark.table("f1_silver.laps")
df_pit       = spark.table("f1_silver.pit_stops")
df_positions = spark.table("f1_silver.positions")

# Cria o schema Gold se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS f1_gold")

# Filtra só sessões de corrida — exclui quali, treinos, etc.
df_races = df_sessions.filter(F.col("session_type") == "Race")

print(f"Corridas disponíveis: {df_races.count()}")
df_races.select("session_key", "circuit_short_name", "date_start").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md ## 1. Dimensão de Pilotos — `dim_drivers`

# COMMAND ----------

window_driver = Window.partitionBy("driver_number").orderBy(F.col("session_key").desc())

df_dim_drivers = (
    df_drivers
    .withColumn("rn", F.row_number().over(window_driver))
    .filter(F.col("rn") == 1)
    .drop("rn", "session_key", "meeting_key")
    .select(
        "driver_number", "name_acronym", "full_name",
        "team_name", "team_colour", "country_code", "headshot_url",
    )
    .orderBy("driver_number")
)

print(f"Pilotos únicos: {df_dim_drivers.count()}")
df_dim_drivers.show(truncate=False)

# COMMAND ----------

(
    df_dim_drivers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_gold.dim_drivers")
)
print("OK dim_drivers salvo")

# COMMAND ----------

# MAGIC %md ## 2. Resultado por Corrida — `fct_race_results`

# COMMAND ----------

window_final_pos = Window.partitionBy("session_key", "driver_number").orderBy(F.col("date").desc())

df_final_positions = (
    df_positions
    .join(df_races.select("session_key"), on="session_key", how="inner")
    .withColumn("rn", F.row_number().over(window_final_pos))
    .filter(F.col("rn") == 1)
    .select("session_key", "driver_number", F.col("position").alias("final_position"))
)

df_lap_stats = (
    df_laps
    .join(df_races.select("session_key"), on="session_key", how="inner")
    .groupBy("session_key", "driver_number")
    .agg(
        F.count("lap_number").alias("total_laps"),
        F.min("lap_duration").alias("fastest_lap_seconds"),
        F.avg("lap_duration").alias("avg_lap_seconds"),
        F.sum("lap_duration").alias("total_race_time_seconds"),
    )
)

df_pit_counts = (
    df_pit
    .join(df_races.select("session_key"), on="session_key", how="inner")
    .groupBy("session_key", "driver_number")
    .agg(
        F.count("lap_number").alias("total_pit_stops"),
        F.min("pit_duration").alias("fastest_pit_seconds"),
        F.avg("pit_duration").alias("avg_pit_seconds"),
    )
)

df_fct_race_results = (
    df_final_positions
    .join(df_lap_stats, on=["session_key", "driver_number"], how="left")
    .join(df_pit_counts, on=["session_key", "driver_number"], how="left")
    .join(
        df_races.select("session_key", "meeting_key", "circuit_short_name", "country_name", "date_start"),
        on="session_key", how="left"
    )
    .join(
        df_dim_drivers.select("driver_number", "name_acronym", "full_name", "team_name"),
        on="driver_number", how="left"
    )
    .select(
        "session_key", "meeting_key", "circuit_short_name", "country_name", "date_start",
        "driver_number", "name_acronym", "full_name", "team_name", "final_position", "total_laps",
        F.round("fastest_lap_seconds", 3).alias("fastest_lap_seconds"),
        F.round("avg_lap_seconds", 3).alias("avg_lap_seconds"),
        F.round("total_race_time_seconds", 3).alias("total_race_time_seconds"),
        F.coalesce(F.col("total_pit_stops"), F.lit(0)).alias("total_pit_stops"),
        F.round("fastest_pit_seconds", 3).alias("fastest_pit_seconds"),
        F.round("avg_pit_seconds", 3).alias("avg_pit_seconds"),
    )
    .orderBy("date_start", "final_position")
)

print(f"fct_race_results: {df_fct_race_results.count()} registros")
df_fct_race_results.show(10, truncate=False)

# COMMAND ----------

(
    df_fct_race_results.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_gold.fct_race_results")
)
print("OK fct_race_results salvo")

# COMMAND ----------

# MAGIC %md ## 3. Análise de Pit Stops por Equipe — `fct_pit_stop_analysis`

# COMMAND ----------

df_fct_pit_analysis = (
    df_pit
    .join(df_races.select("session_key", "circuit_short_name", "date_start"), on="session_key", how="inner")
    .join(df_dim_drivers.select("driver_number", "name_acronym", "team_name"), on="driver_number", how="left")
    .groupBy("team_name", "circuit_short_name")
    .agg(
        F.count("*").alias("total_pit_stops"),
        F.round(F.min("pit_duration"), 3).alias("fastest_pit_seconds"),
        F.round(F.avg("pit_duration"), 3).alias("avg_pit_seconds"),
        F.round(F.max("pit_duration"), 3).alias("slowest_pit_seconds"),
    )
    .orderBy("avg_pit_seconds")
)

print(f"fct_pit_stop_analysis: {df_fct_pit_analysis.count()} registros")
df_fct_pit_analysis.show(15, truncate=False)

# COMMAND ----------

(
    df_fct_pit_analysis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_gold.fct_pit_stop_analysis")
)
print("OK fct_pit_stop_analysis salvo")

# COMMAND ----------

# MAGIC %md ## 4. Performance Acumulada por Piloto — `agg_driver_performance`

# COMMAND ----------

df_agg_driver = (
    df_fct_race_results
    .groupBy("driver_number", "name_acronym", "full_name", "team_name")
    .agg(
        F.count("session_key").alias("races_completed"),
        F.sum(F.when(F.col("final_position") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("final_position") <= 3, 1).otherwise(0)).alias("podiums"),
        F.sum(F.when(F.col("final_position") <= 10, 1).otherwise(0)).alias("points_finishes"),
        F.round(F.avg("final_position"), 2).alias("avg_final_position"),
        F.round(F.avg("fastest_lap_seconds"), 3).alias("avg_fastest_lap"),
        F.round(F.avg("total_pit_stops"), 2).alias("avg_pit_stops_per_race"),
    )
    .orderBy(F.col("wins").desc(), F.col("avg_final_position"))
)

print(f"agg_driver_performance: {df_agg_driver.count()} registros")
df_agg_driver.show(truncate=False)

# COMMAND ----------

(
    df_agg_driver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_gold.agg_driver_performance")
)
print("OK agg_driver_performance salvo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold concluido!
# MAGIC
# MAGIC Tabelas em `f1_gold`:
# MAGIC - `dim_drivers`
# MAGIC - `fct_race_results`
# MAGIC - `fct_pit_stop_analysis`
# MAGIC - `agg_driver_performance`
# MAGIC
# MAGIC Para consultar no SQL Editor:
# MAGIC ```sql
# MAGIC SELECT * FROM f1_gold.agg_driver_performance ORDER BY wins DESC;
# MAGIC SELECT * FROM f1_gold.fct_race_results WHERE circuit_short_name = 'Monza';
# MAGIC ```
