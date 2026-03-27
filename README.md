# 🏎️ F1 Data Pipeline

Pipeline de dados end-to-end com dados da Fórmula 1, desenvolvido para estudo de Engenharia de Dados.

## Tecnologias

| Camada | Tecnologia |
|---|---|
| Ingestão | Python + Docker |
| Storage (raw) | Google Cloud Storage (GCS) |
| Processamento | Databricks + PySpark |
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
- Credenciais GCP (service account JSON)

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
docker build -t f1-ingestion .
docker run --env-file .env -v $(pwd)/gcp/credentials:/app/credentials f1-ingestion
```

## Estrutura do projeto

```
formula1-pipeline/
├── ingestion/              # Aplicação Python de extração das APIs
│   ├── extractors/         # Clientes das APIs (OpenF1)
│   ├── loaders/            # Upload para o GCS
│   └── tests/              # Testes unitários
├── databricks/
│   └── notebooks/          # Notebooks PySpark (Bronze → Silver → Gold)
├── docs/                   # Documentação adicional
├── .github/
│   └── workflows/          # Pipelines de CI/CD
├── Dockerfile              # Container da ingestão
├── docker-compose.yml      # Orquestração local (dev)
├── .env.example            # Template de variáveis de ambiente
└── requirements.txt        # Dependências Python
```

## Status do projeto

- [x] Fase 1 — Ingestão com Python + Docker
- [x] Fase 2 — Armazenamento no GCS (Bronze)
- [x] Fase 3 — Transformações PySpark no Databricks (Silver + Gold)
- [x] Fase 4 — CI/CD com GitHub Actions
