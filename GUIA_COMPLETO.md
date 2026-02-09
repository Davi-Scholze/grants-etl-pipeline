# 📚 GUIA COMPLETO - GRANTS MANAGEMENT ETL PIPELINE

> **Documentação 100% Didática** - Entenda cada pasta, arquivo e processo do pipeline

---

## 📖 Índice

1. [Arquitetura Geral](#arquitetura-geral)
2. [Estrutura de Pastas](#estrutura-de-pastas)
3. [Fluxo do Pipeline (3 Etapas)](#fluxo-do-pipeline)
4. [Como Usar](#como-usar)
5. [Docker & Produção](#docker--produção)
6. [Troubleshooting](#troubleshooting)

---

## 🏗️ Arquitetura Geral

```
┌─────────────────────────────────────────────────────────┐
│                   VOCÊ (Usuário)                        │
│              Click: python -m src.main                  │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│     SINCRONIZAÇÃO AUTOMÁTICA (Downloads → Raw)          │
│  Copia XLSX/CSV de C:\Users\...\Downloads para data/raw │
│              e deleta os originais                      │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│   ETAPA 1: EXTRAÇÃO (Extract)                           │
│  Lê arquivos e cria CSVs consolidados                   │
│  - Lê: Despesas_SIT_*.xlsx (despesas)                   │
│  - Lê: *.csv (resumos financeiros)                      │
│  - Gera: despesas_geral.csv                             │
│  - Gera: resumo_termos.csv + resumo_rubricas.csv        │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│   ETAPA 2: TRANSFORMAÇÃO (Transform)                    │
│  Compara dados com banco usando fingerprint (hash MD5)  │
│  - Detecta: INSERT (novo) / UPDATE (mudou)              │
│  - Gera: despesas_upload.csv (para INSERT/UPDATE)       │
│  - Gera: update_termos.csv (divergências)               │
│  - Gera: update_rubricas.csv (divergências)             │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│   PAUSA INTERATIVA ⏸️                                    │
│  Mostra dados que serão atualizados                     │
│  Você digita SIM para continuar ou CANCELAR             │
└─────────────────┬───────────────────────────────────────┘
                  │
            ┌─────┴─────┐
            │           │
          [SIM]      [CANCELAR]
            │           │
            ▼           ▼
      ETAPA 3      FIM DO PIPELINE
            │      (sem atualizar banco)
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│   ETAPA 3: CARGA (Load)                                 │
│  Escreve dados no SQL Server                            │
│  - INSERT: registros novos                              │
│  - UPDATE: registros alterados                          │
│  - Atualiza: termos (rendimento financeiro)             │
│  - Atualiza: rubricas (estornos)                        │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
         ✅ BANCO ATUALIZADO!
```

---

## 📁 Estrutura de Pastas (Explicação Detalhada)

### Raiz do Projeto

```
Grants Management ETL Pipeline/
│
├── 🔧 .env                    # CONFIGURAÇÃO SENSÍVEL (não commitar!)
│                              # Contém: caminhos, string de conexão SQL Server
│
├── 📝 .env.template            # TEMPLATE do .env (seguro, para versionamento)
│                              # Use como referência para criar .env
│
├── 📦 requirements.txt         # DEPENDÊNCIAS PYTHON
│                              # pandas, pyodbc, python-dotenv, etc
│
├── 🐳 Dockerfile              # IMAGEM DOCKER (Linux com Python + ODBC)
│
├── 🐋 docker-compose.yaml     # ORQUESTRAÇÃO Docker 
│                              # SQL Server + ETL Pipeline in containers
│
│
├── 📁 data/                   # 🏭 DADOS (raw → processed)
│   ├── raw/                   # [ENTRADA] Arquivos recém-baixados
│   │   # Você coloca aqui (ou main.py copia automaticamente)
│   │   # Espera: Despesas_SIT_57884.xlsx, resumo.csv, etc
│   │
│   └── processed/             # [PROCESSAMENTO] Saída do pipeline
│       ├── despesas_geral.csv              ← Consolidação etapa 1
│       ├── despesas_upload.csv             ← Pendências etapa 2
│       ├── despesas_upload.processado.csv  ← Histórico etapa 3
│       ├── resumo_termos.csv               ← Termos extraídos
│       ├── resumo_rubricas.csv             ← Rubricas extraídas
│       ├── update_termos.csv               ← Divergências encontradas
│       └── update_rubricas.csv             ← Divergências encontradas
│
│
├── 📁 database/               # 💾 SQL SCRIPTS
│   ├── ddl/                   # Schema (criação de tabelas)
│   │   ├── estrutura_dbo_despesas.sql
│   │   ├── estrutura_dbo_favorecidos.sql
│   │   ├── estrutura_dbo_rubricas.sql
│   │   ├── estrutura_dbo_termos.sql
│   │   └── estrutura_dbo_vagas_termos.sql
│   │
│   ├── dml/                   # Logic (updates/deletes)
│   │   └── update_rules.sql
│   │
│   └── scripts_auxiliares/    # Ad-hoc scripts
│       └── *.sql              # Manutenção manual
│
│
├── 📁 docs/                   # 📚 DOCUMENTAÇÃO
│   ├── Estrutura de Pastas.txt
│   └── README.md              # (este arquivo)
│
│
├── 📁 logs/                   # 📋 LOGS DE EXECUÇÃO
│   # Criado automaticamente
│   # Conteúdo: MainPipeline_20260209_143022.log, etc
│
│
├── 📁 reports/                # 📊 DASHBOARDS (Power BI)
│   ├── Controle de RH Por Convênio.pbix
│   └── Grants_Dashboard.pbix
│
│
└── 📁 src/                    # 🧠 CÓDIGO-FONTE PYTHON
    │
    ├── 🏁 main.py             # ORQUESTRADOR PRINCIPAL
    │   # - Validação de .env
    │   # - Sincroniza Downloads
    │   # - Executa 3 etapas
    │   # - Pausa para confirmação
    │
    ├── 📥 extract/            # ETAPA 1: EXTRAÇÃO
    │   ├── expenses.py        # NOVO: Wrapper funcional
    │   │   # Classes: ExpensesExtractor
    │   │   # Métodos: run(), extrair_despesas_csv(), extrair_resumos()
    │   │
    │   ├── 1_extract_csv.py   # Script original (referência)
    │   └── 1_extract_resumo.py # Script original (referência)
    │
    ├── 🔄 transform/          # ETAPA 2: TRANSFORMAÇÃO
    │   ├── transformer.py     # NOVO: Wrapper funcional
    │   │   # Classes: ExpensesTransformer
    │   │   # Métodos: run(), transformar_despesas(), validar_e_preparar()
    │   │
    │   ├── 2_transform_compare.py  # Script original (referência)
    │   └── 2_validate.py           # Script original (referência)
    │
    ├── 📤 load/               # ETAPA 3: CARGA
    │   ├── loader.py          # NOVO: Wrapper funcional
    │   │   # Classes: ExpensesLoader
    │   │   # Métodos: run(), carregar_despesas(), atualizar_financeiro()
    │   │
    │   ├── 3_load_sql.py      # Script original (referência)
    │   └── 3_update_sql.py    # Script original (referência)
    │
    └── 🔧 utils/              # UTILITÁRIOS (importados por todos)
        ├── logger.py          # NOVO: Logging centralizado
        │   # Função: setup_logger()
        │   # Cria logs em arquivos com timestamp
        │
        ├── config.py          # NOVO: Configurações centralizadas
        │   # Classe: Config
        │   # Atributos: DIR_DOWNLOADS, DIR_STAGING, CONN_STR, etc
        │
        ├── database.py        # NOVO: Gerenciador de BD
        │   # Classe: DatabaseManager
        │   # Métodos: get_connection(), execute_query(), execute_insert_update()
        │
        ├── ingestor.py        # NOVO: Funções auxiliares
        │   # Funções: classificar_tipo_despesa(), limpar_cpf_cnpj()
        │   # Funções: copiar_downloads_para_raw() ← A FUNÇÃO NOVA!
        │
        └── README.md          # Documentação do módulo src/
```

---

## ▶️ Fluxo do Pipeline (3 Etapas Detalhadas)

### 🟢 ETAPA 1: EXTRAÇÃO (Extract)

**Objetivo:** Ler arquivos XLSX/CSV e consolidar em CSVs padronizados

**Entrada:**
- `data/raw/Despesas_SIT_57884.xlsx` (um para cada SIT)
- `data/raw/resumo*.csv` (archivos de resumo)

**Processo:**

```
1️⃣ Lê cada arquivo XLSX
   ├─ Converte para DataFrame
   ├─ Trata datas (dayfirst=True para brasileiro)
   ├─ Extrai rubrica do tipo de despesa
   ├─ Classifica tipo (PESSOAL, ENCARGOS, etc)
   ├─ Limpa CPF/CNPJ (remove formatação)
   └─ Cria coluna id_termo_rubrica (chave composta)

2️⃣ Consolida TODOS em um só DataFrame
   └─ Salva em: data/processed/despesas_geral.csv

3️⃣ Lê arquivos CSV de resumo
   ├─ Extrai SIT (número identificador)
   ├─ Extrai rendimento financeiro
   ├─ Extrai rubricas com estornos
   ├─ Cria chave composta (SIT-RUBRICA)
   └─ Salva em: data/processed/resumo_termos.csv
                data/processed/resumo_rubricas.csv
```

**Saída:**
```
✅ despesas_geral.csv (150+ linhas, 13 colunas)
   Colunas: id_codigo_sit | termo | rubrica | tipo_despesa | cpf_cnpj | 
            favorecido | tipo_doc_despesa | descricao_despesa | 
            tipo_doc_pagamento | data_pagamento | data_debito_convenio | valor | id_termo_rubrica

✅ resumo_termos.csv
   Colunas: nro_sit | rendimento_financeiro_total

✅ resumo_rubricas.csv
   Colunas: nro_sit | rubrica | valor_estornado | id_termo_rubrica
```

---

### 🟡 ETAPA 2: TRANSFORMAÇÃO (Transform)

**Objetivo:** Comparar dados com banco e identificar INSERT/UPDATE

**Processo:**

#### Fase A: Comparação de Despesas

```
1️⃣ Carrega despesas_geral.csv (do arquivo)

2️⃣ Para cada registro, gera FINGERPRINT (hash MD5)
   Fingerprint = MD5(termo|rubrica|tipo|cpf|favorecido|...|valor)
   → Serve para detectar QUALQUER mudança

3️⃣ Conecta ao SQL Server
   ├─ SELECT * FROM despesas WHERE id_codigo_sit NOT NULL
   └─ Para cada um, gera fingerprint igual

4️⃣ COMPARAÇÃO:
   ┌─────────────────────────────────────────┐
   │ ID não existe no banco? → INSERT        │
   │ ID existe E hash diferente? → UPDATE    │
   │ ID existe E hash igual? → IGNORAR       │
   └─────────────────────────────────────────┘

5️⃣ Salva resultado em: data/processed/despesas_upload.csv
   └─ Contém coluna 'acao': INSERT ou UPDATE
```

#### Fase B: Validação de Termos e Rubricas

```
1️⃣ CRIA MAPA SIT ↔ ID_TERMO
   SELECT id_termo, nro_sit FROM termos
   → Exemplo: {'57884': '6373', '63377': '6729', ...}

2️⃣ COMPARA TERMOS
   ├─ Lê resumo_termos.csv
   ├─ Lê SELECT nro_sit, rendimento_financeiro_total FROM termos
   ├─ Se valor CSV ≠ valor banco (diferença > R$ 0,01)
   └─ Salva divergência em: data/processed/update_termos.csv

3️⃣ COMPARA RUBRICAS
   ├─ Lê resumo_rubricas.csv
   ├─ TRADUZ SIT para ID_TERMO usando MAPA
   ├─ Cria chave composta: "{id_termo}-{rubrica}"
   ├─ Se valor CSV ≠ valor banco (diferença > R$ 0,01)
   └─ Salva divergência em: data/processed/update_rubricas.csv
```

**Saída:**
```
✅ despesas_upload.csv (com coluna 'acao')
   Exemplo:
   id_codigo_sit | termo | rubrica | ... | acao
   001           | 6373  | 3.3.90  | ... | INSERT
   002           | 6373  | 3.3.90  | ... | UPDATE

✅ update_termos.csv (SOMENTE se há divergências)
   nro_sit | rendimento_financeiro_total_csv
   57884   | 1250.50

✅ update_rubricas.csv (SOMENTE se há divergências)
   id_termo_rubrica | valor_estornado
   6373-3.3.90      | 500.25
```

---

### 🔵 PAUSA INTERATIVA ⏸️

**Objetivo:** Você CONFIRMAR antes de atualizar o banco

```
Pipeline mostra:
   📊 DESPESAS A ATUALIZAR:
      • INSERT (novos): 10 registros
      • UPDATE (alterados): 2 registros
      • Total: 12 registros
      
      Amostra (primeiros registros):
         [INSERT] ID:001 | Termo:6373 | Valor:R$1500.50
         [INSERT] ID:002 | Termo:6373 | Valor:R$2000.00
         [UPDATE] ID:003 | Termo:6373 | Valor:R$3500.75
         ... +9 registros
   
   💰 TERMOS A ATUALIZAR: 2 registros
      SIT:57884 | Rendimento:R$5000.50
      SIT:63377 | Rendimento:R$3200.75
   
   📋 RUBRICAS A ATUALIZAR: 1 registros
      6373-3.3.90 | Estorno:R$500.25

❓ Os dados acima estão CORRETOS? Digite 'SIM' para continuar ou qualquer outra coisa para CANCELAR:
```

**Comportamento:**
- Você digita `SIM` → Continua para Etapa 3
- Você digita qualquer outra coisa → **CANCELA** (banco não é alterado)

---

### 🟣 ETAPA 3: CARGA (Load)

**Objetivo:** Escrever dados no SQL Server

#### Fase A: Carga de Despesas

```
1️⃣ Lê despesas_upload.csv

2️⃣ Para cada registro:
   ├─ Se acao = 'INSERT':
   │  └─ INSERT INTO despesas (id_codigo_sit, termo, rubrica, ...)
   │             VALUES (?, ?, ?, ...)
   │
   └─ Se acao = 'UPDATE':
      └─ UPDATE despesas SET termo=?, rubrica=?, ...
                     WHERE id_codigo_sit = ?

3️⃣ Conta: X INSERT, Y UPDATE

4️⃣ Renomeia arquivo: despesas_upload.csv → despesas_upload.processado.csv
   (Histórico de execução)
```

#### Fase B: Atualização Financeira

```
1️⃣ Se existe update_termos.csv:
   └─ UPDATE termos SET rendimento_financeiro_total = ? 
                   WHERE nro_sit = ?

2️⃣ Se existe update_rubricas.csv:
   └─ UPDATE rubricas SET valor_estornado = ? 
                    WHERE id_termo_rubrica = ?

3️⃣ Deleta arquivos (cleanup)
   └─ Remove update_termos.csv e update_rubricas.csv
```

**Saída:**
```
🚀 INSERT: 10 | UPDATE: 2
✅ Banco atualizado com sucesso!
💾 Histórico: despesas_upload.processado.csv
```

---

## 🚀 Como Usar

### Instalação

#### 1️⃣ Pré-requisitos

```bash
# Python 3.8+ já instalado
python --version

# Clone/baixe o projeto
cd "Grants Management ETL Pipeline"
```

#### 2️⃣ Criar .env

```bash
# Copie o template
copy .env.template .env

# Edite com seus dados
# DIR_DOWNLOADS=...
# DIR_STAGING=...
# CONN_STR_SQLSERVER=...
```

#### 3️⃣ Instalar Dependências

```bash
pip install -r requirements.txt
```

### Execução

#### ▶️ Executar Pipeline Completo

```bash
python -m src.main
```

**O que acontece:**
```
1. Sincroniza Downloads → data/raw
2. Extrai arquivos
3. Transforma e compara
4. **Pausa para confirmação** ⏸️
5. Se SIM → Carrega no banco
6. Logs salvos em logs/
```

#### ▶️ Executar Etapas Individualmente (Debugging)

```bash
# Apenas Extração
python -m src.extract.expenses

# Apenas Transformação
python -m src.transform.transformer

# Apenas Carga
python -m src.load.loader
```

### 📋 Verificar Logs

```bash
# Ver mais recente
cat logs/MainPipeline_*.log | tail -50

# Ou no VS Code
# Abra: logs/MainPipeline_20260209_143022.log
```

---

## 🐳 Docker & Produção

### O que é Docker?

Docker permite **empacotar a aplicação + banco de dados** em containers isolados. Você não precisa instalar nada localmente!

### Arquivos Docker

#### `Dockerfile`
```dockerfile
# Imagem base: Python 3.11
FROM python:3.11-slim

# Instala ODBC driver (necessário para SQL Server)
RUN apt-get update && ...

# Copia a aplicação
COPY . /app/

# Instala dependências
RUN pip install -r requirements.txt

# Comando ao iniciar
CMD ["python", "-m", "src.main"]
```

**O que faz:** Cria imagem Docker com Python + ODBC + código

#### `docker-compose.yaml`
```yaml
services:
  sqlserver:        # Serviço 1: SQL Server para dados
    image: mssql/server:2022-latest
    ports:
      - "1433:1433"
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "..."

  etl_pipeline:     # Serviço 2: Aplicação Python
    build: .
    depends_on:
      - sqlserver   # Aguarda SQL ficar pronto
    environment:
      CONN_STR_SQLSERVER: "Server=sqlserver,..."
```

**O que faz:** Orquestra SQL Server + ETL juntos

### Usar Docker

#### ✅ Build (cria imagem)

```bash
docker-compose build
```

#### ✅ Run (inicia containers)

```bash
# Both SQL Server + ETL
docker-compose up

# Só SQL Server (para desenvolvimento)
docker-compose up sqlserver

# Só ETL (depois que SQL estiver pronto)
docker-compose up etl_pipeline
```

#### ✅ Logs

```bash
docker-compose logs -f etl_pipeline
docker-compose logs -f sqlserver
```

#### ✅ Parar

```bash
docker-compose down
```

#### ✅ Deletar tudo (⚠️ CUIDADO - deleta dados!)

```bash
docker-compose down -v
```

---

## 🔍 Troubleshooting

### ❌ "Variáveis de ambiente faltando"

**Solução:**
```bash
# Verificar .env
cat .env

# Deve ter:
DIR_DOWNLOADS=C:\Users\usuario\Documents\...
DIR_STAGING=C:\Users\usuario\Documents\...
CONN_STR_SQLSERVER=Driver={...};Server=...;
```

### ❌ "Arquivo não encontrado"

**Solução:**
```bash
# Colocar arquivos em data/raw/ e rodar
python -m src.main

# Ou colocar em Downloads e deixar main.py copiar
# Main copia automaticamente!
```

### ❌ "Erro de conexão SQL Server"

**Solução:**
```bash
# Verificar se SQL Server está rodando
# Se local: Services → SQL Server Agent
# Se Docker: docker-compose up sqlserver

# Testar conexão:
python -c "
import pyodbc
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=ETL_Convenios;Trusted_Connection=yes;')
print('✅ Conectado!')
"
```

### ❌ "ODBC Driver não encontrado"

**Solução (Windows):**
```bash
# Instalar: https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-sql-server
# Depois: pip install pyodbc
```

**Solução (Docker):**
```bash
# Já está incluído no Dockerfile
# Usar docker-compose
```

### ❌ "Unicode / Encoding error"

**Solução:**
```bash
# Etapa 1 trata automaticamente (Latin-1, UTF-8)
# Se ainda houver problema, verificar encoding dos CSVs
# Windows: Salvar como UTF-8 sem BOM
```

---

## 📊 Exemplo Prático Completo

### Cenário: Você tem 2 arquivos novos

```
C:\Users\usuario\Downloads\
├── Despesas_SIT_57884.xlsx     ← Novo!
└── resumo.csv                   ← Novo!
```

### Executa:

```bash
python -m src.main
```

### Saída Esperada:

```
🏁 PIPELINE ETL - GRANTS MANAGEMENT - INICIANDO

🔍 Validando configurações...
✅ Configurações OK

📥 Sincronizando arquivos de Downloads...
   📥 Movido: Despesas_SIT_57884.xlsx (original deletado)
   📥 Movido: resumo.csv (original deletado)
✅ 2 arquivo(s) movido(s) para data/raw

ETAPA 1/3: EXTRAÇÃO
➡️ Etapa 1a: Consolidação de Arquivos de Despesas
   ✅ SIT 57884 (arquivo 57884.xlsx) - 150 linhas extraídas
   💾 Total: 150 registros salvos em despesas_geral.csv
➡️ Etapa 1b: Extração de Resumos Financeiros
   ✅ SIT 57884 de 'resumo.csv'
   💾 Resumo termos: 1 registros salvos
   💾 Resumo rubricas: 5 registros salvos

ETAPA 2/3: TRANSFORMAÇÃO E VALIDAÇÃO
➡️ Etapa 2a: Comparação Inteligente (Despesas)
🔍 Consultando banco de dados...
📦 140 registros do banco carregados
   📊 INSERT: 10 | UPDATE: 0 | IGNORE: 140
   💾 10 registros salvos para upload
➡️ Etapa 2b: Validação de Termos e Rubricas
1️⃣  Analisando Termos...
   ✅ Termos sincronizados.
2️⃣  Analisando Rubricas...
   ✅ Rubricas sincronizadas.

=================== VALIDAÇÃO: Revise =================
📊 DESPESAS A ATUALIZAR:
   • INSERT (novos): 10 registros
   • UPDATE (alterados): 0 registros
   • Total: 10 registros
   
   Amostra (primeiros registros):
      [INSERT] ID:SIT-001 | Termo:6373 | Valor:R$1500.00
      [INSERT] ID:SIT-002 | Termo:6373 | Valor:R$2000.00
      [INSERT] ID:SIT-003 | Termo:6373 | Valor:R$1200.50
      ... +7 registros

❓ Os dados acima estão CORRETOS? Digite 'SIM' para continuar: SIM

✅ CONFIRMADO! Prosseguindo com a carga...

ETAPA 3/3: CARGA NO BANCO DE DADOS
➡️ Etapa 3a: Carga de Despesas (INSERT/UPDATE)
   🚀 INSERT: 10 | UPDATE: 0
   ✅ Arquivo renomeado para .processado.csv
➡️ Etapa 3b: Atualização de Termos e Rubricas
   ℹ️  Nada para atualizar (dados sincronizados)

======== ✨ PIPELINE CONCLUÍDO COM SUCESSO
⏱️  Tempo total: 12.45 segundos
======================================================
```

### Arquivos Criados:

```
data/processed/
├── despesas_geral.csv                  ← Etapa 1
├── despesas_upload.processado.csv      ← Etapa 3 (histórico)
├── resumo_termos.csv                   ← Etapa 1
├── resumo_rubricas.csv                 ← Etapa 1
└── (update_*.csv - não criados, dados sincronizados)

logs/
└── MainPipeline_20260209_143022.log    ← Detalhes completos
```

### Banco de Dados:

```sql
-- 10 novos registros inseridos
SELECT COUNT(*) FROM despesas
-- Resultado: +10 (era 140, agora 150)
```

---

## 🎯 Resumo em 1 Minuto

| O Quê | Como |
|-------|------|
| **Executar** | `python -m src.main` |
| **Confirmar dados** | Digite `SIM` quando pedido |
| **Ver logs** | Abra `logs/MainPipeline_*.log` |
| **Testar etapa 1** | `python -m src.extract.expenses` |
| **Testar etapa 2** | `python -m src.transform.transformer` |
| **Testar etapa 3** | `python -m src.load.loader` |
| **Com Docker** | `docker-compose up` |

---

**📧 Dúvidas?** Verificar logs ou executar com `--debug`

**🚀 Pronto para produção!**
