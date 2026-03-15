# ─────────────────────────────────────────
# Imagem base: Python 3.11 leve (slim)
# ─────────────────────────────────────────
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia primeiro só o requirements para aproveitar o cache do Docker
# (se o código mudar mas as dependências não, essa camada não é refeita)
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código de ingestão
COPY ingestion/ ./ingestion/

# Copia o .env.example como referência (o .env real é passado em runtime)
COPY .env.example .env.example

# Variável de ambiente padrão (pode ser sobrescrita no docker run)
ENV F1_SEASON=2025

# Comando que roda ao iniciar o container
CMD ["python", "-m", "ingestion.main"]
