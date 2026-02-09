# Grants Management ETL Pipeline

Este repositório contém o pipeline ETL (Extract → Transform → Load) para processamento de despesas de convênios.

## Visão Rápida
- Entrada: arquivos XLSX / CSV colocados em `Downloads/` ou `data/raw/`.
- Etapas:
  1. Extração (`src/extract/`) → consolida em `data/processed/despesas_geral.csv`
  2. Transformação (`src/transform/`) → gera `despesas_upload.csv` (coluna `acao` = INSERT/UPDATE)
  3. Validação manual → pausa e pede `SIM` antes de atualizar o banco
  4. Carga (`src/load/`) → insere/atualiza no SQL Server
- Logs em `logs/` com timestamp por etapa.

## Como rodar localmente (Windows, com `.venv`)
1. Ative a virtualenv do projeto (se existir):

```powershell
cd "C:\Users\usuario\Documents\Grants Management ETL Pipeline"
.\.venv\Scripts\activate
```

2. Execute o pipeline:

```powershell
python -m src.main
```

3. No ponto de pausa, digite `SIM` para confirmar a carga no banco.

## Como rodar com Docker (recomendado para produção)
1. Build e subir os serviços (SQL Server + ETL):

```bash
docker-compose build
docker-compose up -d
```

2. Ver logs do container ETL:

```bash
docker-compose logs -f etl_pipeline
```

3. Para parar:

```bash
docker-compose down
```

> O `docker-compose.yaml` já inclui serviço do SQL Server e volume para inicializar a database com `database/init.sql`.

## Arquivos importantes
- `src/main.py` — orquestrador (extração → transformação → validação → carga)
- `src/utils/config.py` — lê `.env` e provê constantes
- `data/raw/` — arquivos brutos
- `data/processed/` — CSVs consolidados e de upload
- `database/init.sql` — script de criação da database/tabelas
- `GUIA_COMPLETO.md` — documentação detalhada

## Segurança
- Não commite seu `.env` (já adicionado a `.gitignore`).
- Se você acidentalmente subir credenciais, troque as senhas imediatamente.

## Agendamento
- Windows: use o Task Scheduler apontando para `python -m src.main` na pasta do projeto.
- Linux: use `cron` apontando para `./venv/bin/python -m src.main`.

## Uso do GitHub Desktop
- Abra o GitHub Desktop, adicione o repositório local e use `Commit` / `Push` para sincronizar.

## Próximos passos sugeridos
- Adicionar `README.md` com exemplos (feito)
- Opcional: habilitar `GitHub Actions` para testes automatizados e linting
- Opcional: configurar Secrets no GitHub (para deploy) — não coloque segredos no repo

---
Se quiser, eu atualizo o `README.md` com comandos específicos do seu ambiente ou crio um workflow de CI/CD.
