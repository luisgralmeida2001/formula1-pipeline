# Documentação do Projeto

## Fases do projeto

### Fase 1 — Ingestão (`ingestion/`)
Script Python que consome as APIs OpenF1 e Ergast e persiste os dados brutos no GCS.
Containerizado com Docker para garantir portabilidade.

**Arquivos principais:**
- `ingestion/main.py` — ponto de entrada
- `ingestion/extractors/openf1.py` — cliente da OpenF1 API
- `ingestion/extractors/ergast.py` — cliente da Ergast API
- `ingestion/loaders/gcs.py` — upload para o GCS

---

### Fase 2 — Bronze (GCS)
Dados brutos armazenados como Parquet particionado por temporada/corrida.

**Estrutura no bucket:**
```
gs://f1-pipeline-bronze/
├── races/season=2025/round=01/data.parquet
├── laps/season=2025/round=01/data.parquet
├── pit_stops/season=2025/round=01/data.parquet
└── drivers/data.parquet
```

---

### Fase 3 — Silver + Gold (Databricks + PySpark)
Notebooks PySpark que leem o Bronze, limpam os dados (Silver) e criam agregações (Gold).

**Notebooks:**
- `databricks/notebooks/01_bronze_to_silver.py`
- `databricks/notebooks/02_silver_to_gold.py`

---

### Fase 4 — Modelagem (dbt)
Modelos dbt rodando sobre as tabelas Gold no BigQuery.

**Modelos principais:**
- `fct_race_results` — resultado de cada corrida por piloto
- `fct_pit_stops` — análise de pit stops por equipe
- `dim_drivers` — dimensão de pilotos
- `dim_circuits` — dimensão de circuitos

---

### Fase 5 — CI/CD (GitHub Actions)
Workflow que roda a cada push: lint, testes e validação dos modelos dbt.
