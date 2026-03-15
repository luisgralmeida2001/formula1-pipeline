# 🏎️ F1 Data Pipeline

Pipeline de dados end-to-end com dados da Fórmula 1, desenvolvido para estudo de Engenharia de Dados.

## Tecnologias

| Camada | Tecnologia |
|---|---|
| Ingestão | Python + Docker |
| Storage (raw) | Google Cloud Storage (GCS) |
| Processamento | Databricks + PySpark |
| Modelagem | dbt |
| Data Warehouse | BigQuery |
| CI/CD | GitHub Actions |
| Cloud | GCP |

## Arquitetura Medalião

```
API (OpenF1 + Ergast)
        │
        ▼
  [Bronze] GCS — dados brutos em JSON/Parquet
        │
        ▼
  [Silver] Databricks — limpeza, tipagem, deduplicação
        │
        ▼
  [Gold] Databricks + dbt — modelos analíticos
        │
        ▼
  [Data Warehouse] BigQuery — tabelas finais
```

## Perguntas que o projeto responde

- Quem lidera o campeonato de pilotos em 2025?
- Qual equipe tem a maior velocidade média nos pit stops?
- Qual circuito registra os maiores tempos de volta?
- Como a performance de cada piloto evoluiu ao longo da temporada?

## Fontes de dados

- **OpenF1 API** — `https://api.openf1.org` (gratuita, sem autenticação, temporada atual)
- **Ergast API** — `https://ergast.com/mrd` (gratuita, histórico completo desde 1950)

## Como rodar localmente

### Pré-requisitos
- Docker instalado
- Conta GCP com projeto criado
- Credenciais GCP configuradas (ver `gcp/README.md`)

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/f1-data-pipeline.git
cd f1-data-pipeline
```

### 2. Configure as variáveis de ambiente
```bash
cp .env.example .env
# Edite o .env com suas configurações
```

### 3. Execute a ingestão via Docker
```bash
docker build -t f1-ingestion ./ingestion
docker run --env-file .env f1-ingestion
```

## Estrutura do projeto

```
f1-data-pipeline/
├── ingestion/          # Scripts Python de extração das APIs
├── databricks/
│   ├── notebooks/      # Notebooks PySpark (Bronze → Silver → Gold)
│   └── jobs/           # Definições de jobs do Databricks Workflows
├── dbt/
│   ├── models/
│   │   ├── bronze/     # Fontes raw (sources)
│   │   ├── silver/     # Dados limpos
│   │   └── gold/       # Modelos analíticos finais
│   ├── tests/          # Testes customizados
│   └── macros/         # Macros reutilizáveis
├── gcp/                # Configs e scripts de infraestrutura GCP
├── .github/
│   └── workflows/      # Pipelines de CI/CD
├── docs/               # Documentação adicional
├── Dockerfile          # Container da ingestão
├── docker-compose.yml  # Orquestração local (dev)
├── .env.example        # Template de variáveis de ambiente
└── requirements.txt    # Dependências Python
```

## Status do projeto

- [ ] Fase 1 — Ingestão com Python + Docker
- [ ] Fase 2 — Armazenamento no GCS (Bronze)
- [ ] Fase 3 — Transformações PySpark no Databricks (Silver + Gold)
- [ ] Fase 4 — Modelagem com dbt
- [ ] Fase 5 — CI/CD com GitHub Actions
