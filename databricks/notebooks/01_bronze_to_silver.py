# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver
# MAGIC
# MAGIC Lê os dados brutos (JSON) do GCS, aplica limpeza e tipagem,
# MAGIC e salva as tabelas Silver no formato Delta no Databricks.
# MAGIC
# MAGIC **Camada Silver:** dados limpos, tipados e sem duplicatas.
# MAGIC Ainda fieis à fonte, mas prontos para transformações analíticas.

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

import sys
sys.path.append("/Workspace/brasileirao")

import config_local as cfg

credentials_dict = {
    "type": "authorized_user",
    "client_id": cfg.GCP_CLIENT_ID,
    "client_secret": cfg.GCP_CLIENT_SECRET,
    "refresh_token": cfg.GCP_REFRESH_TOKEN
}

# COMMAND ----------

# MAGIC %pip install google-cloud-storage

# COMMAND ----------

import os
import json
from google.cloud import storage

with open("/tmp/gcp_creds.json", "w") as f:
    json.dump(credentials_dict, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp_creds.json"

# Baixa todos os arquivos do GCS para o DBFS
client = storage.Client(project=cfg.GCP_PROJECT_ID)
bucket = client.bucket(cfg.GCP_BUCKET)

LOCAL_BASE = "/tmp/f1-bronze"
os.makedirs(LOCAL_BASE, exist_ok=True)

blobs = list(bucket.list_blobs())
print(f"Total de arquivos no GCS: {len(blobs)}")

for blob in blobs:
    dest_path = f"{LOCAL_BASE}/{blob.name}"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    blob.download_to_filename(dest_path)
    print(f"  ✅ {blob.name}")

print("Download concluído!")

# COMMAND ----------

BRONZE_BASE = "/tmp/f1-bronze"
SEASON = 2025
SILVER_BASE = "/tmp/silver/f1"

# COMMAND ----------

# Configurações — ajuste conforme seu projeto GCP
GCS_BUCKET = "f1-pipeline-bronze"       # nome do seu bucket
SEASON = 2025

# Caminhos de leitura no GCS
BRONZE_BASE = f"gs://{GCS_BUCKET}"

# Caminhos de escrita no Databricks (Delta tables)
SILVER_BASE = "/delta/silver/f1"

print(f"Lendo de: {BRONZE_BASE}")
print(f"Escrevendo em: {SILVER_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como conectar o GCS ao Databricks Community Edition
# MAGIC
# MAGIC O Community Edition não tem integração nativa com GCP.
# MAGIC A solução mais simples é usar as credenciais ADC diretamente:
# MAGIC
# MAGIC 1. No Databricks, vá em **Settings → Developer → Access Tokens** e gere um token
# MAGIC 2. Cole o conteúdo do seu `application_default_credentials.json` numa variável abaixo
# MAGIC
# MAGIC **Alternativa mais simples para estudo:**
# MAGIC usar o `dbutils.fs.cp` para copiar os arquivos do GCS para o DBFS (storage interno do Databricks)
# MAGIC e trabalhar a partir daí. É o que faremos aqui.

# COMMAND ----------

# MAGIC %md ## 1. Sessões

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# COMMAND ----------

# Verifica se os dados já estão no /tmp, se não, baixa novamente
import os

if not os.path.exists("/tmp/f1-bronze/sessions"):
    print("Dados não encontrados no /tmp, baixando do GCS...")
    
    blobs = list(bucket.list_blobs())
    print(f"Total de arquivos no GCS: {len(blobs)}")
    
    for blob in blobs:
        dest_path = f"/tmp/f1-bronze/{blob.name}"
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        blob.download_to_filename(dest_path)
    
    print("Download concluído!")
else:
    print("Dados já disponíveis no /tmp, pulando download.")

BRONZE_BASE = "/tmp/f1-bronze"
SEASON = 2025

# COMMAND ----------

import json
import os
import pandas as pd

# Lê os JSONs manualmente e cria o DataFrame
def load_json_files(base_path, entity, season):
    records = []
    entity_path = f"{base_path}/{entity}/year={season}"
    
    if not os.path.exists(entity_path):
        print(f"Caminho não encontrado: {entity_path}")
        return []
    
    for root, dirs, files in os.walk(entity_path):
        for file in files:
            if file.endswith(".json"):
                with open(f"{root}/{file}", "r") as f:
                    data = json.load(f)
                    records.extend(data)
    return records

sessions_data = load_json_files(BRONZE_BASE, "sessions", SEASON)
print(f"Sessions raw: {len(sessions_data)} registros")

df_sessions_raw = spark.createDataFrame(pd.DataFrame(sessions_data))
df_sessions_raw.printSchema()

# COMMAND ----------

# Limpeza e tipagem das sessões
df_sessions = (
    df_sessions_raw
    # Remove duplicatas pelo identificador único
    .dropDuplicates(["session_key"])
    # Seleciona e renomeia colunas relevantes
    .select(
        F.col("session_key").cast(IntegerType()),
        F.col("meeting_key").cast(IntegerType()),
        F.col("session_name").cast(StringType()),
        F.col("session_type").cast(StringType()),
        F.col("circuit_key").cast(IntegerType()),
        F.col("circuit_short_name").cast(StringType()),
        F.col("country_name").cast(StringType()),
        F.col("location").cast(StringType()),
        F.col("year").cast(IntegerType()),
        F.to_timestamp(F.col("date_start")).alias("date_start"),
        F.to_timestamp(F.col("date_end")).alias("date_end"),
    )
    # Filtra só sessões com session_key válido
    .filter(F.col("session_key").isNotNull())
)

print(f"Sessions silver: {df_sessions.count()} registros")
df_sessions.show(5, truncate=False)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS f1_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS f1_gold")

# COMMAND ----------

# Salva como Delta table
(
    df_sessions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_silver.sessions")
)
print("✅ sessions salvo")

# COMMAND ----------

# MAGIC %md ## 2. Pilotos (Drivers)

# COMMAND ----------

drivers_data = load_json_files(BRONZE_BASE, "drivers", SEASON)
print(f"Drivers raw: {len(drivers_data)} registros")
df_drivers_raw = spark.createDataFrame(pd.DataFrame(drivers_data))
df_drivers_raw.printSchema()

# COMMAND ----------

df_drivers = (
    df_drivers_raw
    .dropDuplicates(["session_key", "driver_number"])
    .select(
        F.col("session_key").cast(IntegerType()),
        F.col("meeting_key").cast(IntegerType()),
        F.col("driver_number").cast(IntegerType()),
        F.col("name_acronym").cast(StringType()),
        F.col("full_name").cast(StringType()),
        F.col("team_name").cast(StringType()),
        F.col("team_colour").cast(StringType()),
        F.col("country_code").cast(StringType()),
        F.col("headshot_url").cast(StringType()),
    )
    .filter(F.col("driver_number").isNotNull())
)

print(f"Drivers silver: {df_drivers.count()} registros")
df_drivers.show(5, truncate=False)

# COMMAND ----------

(
    df_drivers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_silver.drivers")
)
print("✅ drivers salvo")

# COMMAND ----------

# MAGIC %md ## 3. Voltas (Laps)

# COMMAND ----------

laps_data = load_json_files(BRONZE_BASE, "laps", SEASON)
print(f"Laps raw: {len(laps_data)} registros")
df_laps_raw = spark.createDataFrame(pd.DataFrame(laps_data))
df_laps_raw.printSchema()

# COMMAND ----------

df_laps = (
    df_laps_raw
    .dropDuplicates(["session_key", "driver_number", "lap_number"])
    .select(
        F.col("session_key").cast(IntegerType()),
        F.col("meeting_key").cast(IntegerType()),
        F.col("driver_number").cast(IntegerType()),
        F.col("lap_number").cast(IntegerType()),
        F.col("lap_duration").cast(DoubleType()),
        F.col("duration_sector_1").cast(DoubleType()),
        F.col("duration_sector_2").cast(DoubleType()),
        F.col("duration_sector_3").cast(DoubleType()),
        F.col("i1_speed").cast(DoubleType()),
        F.col("i2_speed").cast(DoubleType()),
        F.col("st_speed").cast(DoubleType()),
        F.col("is_pit_out_lap").cast("boolean"),
        F.to_timestamp(F.col("date_start")).alias("date_start"),
    )
    # Remove voltas sem duração (warmup laps, safety car, etc.)
    .filter(F.col("lap_duration").isNotNull())
    .filter(F.col("lap_duration") > 0)
)

print(f"Laps silver: {df_laps.count()} registros")
df_laps.show(5)

# COMMAND ----------

(
    df_laps.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_silver.laps")
)
print("✅ laps salvo")

# COMMAND ----------

# MAGIC %md ## 4. Pit Stops

# COMMAND ----------

pit_data = load_json_files(BRONZE_BASE, "pit_stops", SEASON)
print(f"PitStops raw: {len(pit_data)} registros")
df_pit_raw = spark.createDataFrame(pd.DataFrame(pit_data))
df_pit_raw.printSchema()

# COMMAND ----------

df_pit = (
    df_pit_raw
    .dropDuplicates(["session_key", "driver_number", "lap_number"])
    .select(
        F.col("session_key").cast(IntegerType()),
        F.col("meeting_key").cast(IntegerType()),
        F.col("driver_number").cast(IntegerType()),
        F.col("lap_number").cast(IntegerType()),
        F.col("pit_duration").cast(DoubleType()),
        F.to_timestamp(F.col("date")).alias("date"),
    )
    # Remove pit stops sem duração (dados incompletos da API)
    .filter(F.col("pit_duration").isNotNull())
    .filter(F.col("pit_duration") > 0)
    .filter(F.col("pit_duration") < 120)  # remove outliers absurdos (>2min)
)

print(f"Pit stops silver: {df_pit.count()} registros")
df_pit.show(5)

# COMMAND ----------

(
    df_pit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_silver.pit_stops")
)
print("✅ pit_stops salvo")

# COMMAND ----------

# MAGIC %md ## 5. Posições

# COMMAND ----------

positions_data = load_json_files(BRONZE_BASE, "positions", SEASON)
print(f"Positions raw: {len(positions_data)} registros")
df_pos_raw = spark.createDataFrame(pd.DataFrame(positions_data))
df_pos_raw.printSchema()

# COMMAND ----------

df_positions = (
    df_pos_raw
    .dropDuplicates(["session_key", "driver_number", "date"])
    .select(
        F.col("session_key").cast(IntegerType()),
        F.col("meeting_key").cast(IntegerType()),
        F.col("driver_number").cast(IntegerType()),
        F.col("position").cast(IntegerType()),
        F.to_timestamp(F.col("date")).alias("date"),
    )
    .filter(F.col("position").isNotNull())
)

print(f"Positions silver: {df_positions.count()} registros")
df_positions.show(5)

# COMMAND ----------

(
    df_positions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("f1_silver.positions")
)
print("✅ positions salvo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver concluído!
# MAGIC
# MAGIC Tabelas geradas em `/delta/silver/f1/`:
# MAGIC - `sessions` — sessões limpas e tipadas
# MAGIC - `drivers` — pilotos por sessão
# MAGIC - `laps` — voltas com durações e setores
# MAGIC - `pit_stops` — pit stops com duração filtrada
# MAGIC - `positions` — posições ao longo da corrida
# MAGIC
# MAGIC Próximo passo: rodar `02_silver_to_gold.py`
