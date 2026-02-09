# 🐳 Dockerfile - Grants Management ETL Pipeline
# Imagem Python com drivers ODBC para SQL Server

FROM python:3.11-slim

# Metadados
LABEL maintainer="Grants Management Team"
LABEL description="ETL Pipeline para Gestão de Convênios"

# Define diretório de trabalho
WORKDIR /app

# ===== INSTALA DEPENDÊNCIAS DE SISTEMA =====
# Necessário para pyodbc (driver ODBC para SQL Server)
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    unixodbc \
    odbcinst \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ===== INSTALA DRIVER ODBC 17 PARA LINUX =====
# Necessário para conectar ao SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

# ===== COPIA APLICAÇÃO =====
# Copia todo o código para o container
COPY . /app/

# ===== INSTALA DEPENDÊNCIAS PYTHON =====
RUN pip install --no-cache-dir -r requirements.txt

# ===== CRIA DIRETÓRIOS NECESSÁRIOS =====
RUN mkdir -p /app/data/raw /app/data/processed /app/logs

# ===== DEFINE VARIÁVEIS DE AMBIENTE =====
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app:${PATH}"

# ===== COMANDO DE INICIALIZAÇÃO =====
# O container executa o pipeline ETL
CMD ["python", "-m", "src.main"]
