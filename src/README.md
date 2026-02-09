# 🧠 MÓDULO SRC - COMO USAR

## 📁 Estrutura

```
src/
├── main.py                 # Orquestrador principal do pipeline
├── extract/
│   ├── expenses.py        # Extrator (NOVO - wrapper funcional)
│   ├── 1_extract_csv.py   # Scripts originais (mantidos para referência)
│   └── 1_extract_resumo.py
├── transform/
│   ├── transformer.py     # Transformador (NOVO - wrapper funcional)
│   ├── 2_transform_compare.py
│   └── 2_validate.py
├── load/
│   ├── loader.py          # Carregador (NOVO - wrapper funcional)
│   ├── 3_load_sql.py
│   └── 3_update_sql.py
└── utils/
    ├── logger.py          # Sistema de logging centralizado (NOVO)
    ├── config.py          # Configurações e mapeamentos (NOVO)
    ├── database.py        # Gerenciador de BD (NOVO)
    └── ingestor.py        # Funções utilitárias (NOVO)
```

## 🚀 Como Executar

### Opção 1: Executar o Pipeline Completo
```bash
python -m src.main
```

Isso vai executar automaticamente as 3 etapas:
1. **Extração** - Lê CSVs/XLSXs e prepara dados
2. **Transformação** - Compara com banco e valida
3. **Carga** - Insere/atualiza registros no SQL Server

### Opção 2: Executar Etapas Individualmente (se necessário)
```bash
# Apenas extração
python -m src.extract.expenses

# Apenas transformação
python -m src.transform.transformer

# Apenas carga
python -m src.load.loader
```

## 📝 Pré-requisitos

1. **Python 3.8+** instalado
2. **Dependências** instaladas:
```bash
pip install -r requirements.txt
```

3. **Arquivo .env** configurado com:
   - `DIR_DOWNLOADS` - Pasta com arquivos de entrada
   - `DIR_STAGING` - Pasta de processamento
   - `CONN_STR_SQLSERVER` - String de conexão ao SQL Server

4. **Arquivos de entrada** em `data/raw/`:
   - `Despesas_SIT_*.xlsx` - Um para cada SIT
   - `*.csv` - Arquivos de resumo financeiro

## ✅ Validação

O pipeline valida automaticamente:
- ✔️ Variáveis de ambiente necessárias
- ✔️ Conexão com banco de dados
- ✔️ Existência de arquivos de entrada
- ✔️ Integridade dos dados (fingerprints)
- ✔️ Sincronização entre CSV e banco

## 📊 Saídas Geradas

Para cada execução:
- `data/processed/despesas_geral.csv` - Despesas consolidas
- `data/processed/despesas_upload.csv` - Registros para INSERT/UPDATE
- `data/processed/despesas_upload.processado.csv` - Versão pós-carga
- `data/processed/resumo_termos.csv` - Dados de termos
- `data/processed/resumo_rubricas.csv` - Dados de rubricas
- `data/processed/update_*.csv` - Divergências encontradas
- `logs/*.log` - Arquivos de controle e execução

## 🔍 Logs

Todos os logs são salvos em `logs/` com timestamp:
```
logs/
├── MainPipeline_20260209_143022.log
├── ExpensesExtractor_20260209_143022.log
├── ExpensesTransformer_20260209_143025.log
└── ExpensesLoader_20260209_143030.log
```

## 🛠️ Troubleshooting

| Erro | Solução |
|------|---------|
| `ERRO: Variáveis não definidas` | Verificar arquivo `.env` |
| `Arquivo não encontrado` | Colocar XLSXs em `data/raw/` |
| `Erro de conexão SQL` | Verificar CONN_STR e permissões |
| `No module named 'src'` | Executar com `python -m src.main` |

## 📚 Referência das Classes

### ExpensesExtractor
```python
from src.extract.expenses import ExpensesExtractor
extractor = ExpensesExtractor()
sucesso = extractor.run()  # Retorna True/False
```

### ExpensesTransformer
```python
from src.transform.transformer import ExpensesTransformer
transformer = ExpensesTransformer()
sucesso = transformer.run()
```

### ExpensesLoader
```python
from src.load.loader import ExpensesLoader
loader = ExpensesLoader()
sucesso = loader.run()
```

### Config
```python
from src.utils.config import Config
print(Config.SIT_TERMO_MAP)       # Mapeamento de SITs
print(Config.COLUNAS_DESPESAS)    # Colunas esperadas
```

### Logger
```python
from src.utils.logger import setup_logger
logger = setup_logger("MeuModulo")
logger.info("Mensagem")
logger.error("Erro", exc_info=True)
```
