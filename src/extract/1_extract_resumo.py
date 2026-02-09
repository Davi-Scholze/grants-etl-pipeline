import os
import logging
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

# ======================================================
# CONFIGURAÇÕES
# ======================================================
load_dotenv()

env_downloads = os.getenv("DIR_DOWNLOADS")
env_staging = os.getenv("DIR_STAGING")

if not env_downloads or not env_staging:
    raise ValueError("ERRO: DIR_DOWNLOADS ou DIR_STAGING não definidos no .env")

PASTA_DOWNLOADS = Path(env_downloads)
PASTA_STAGING = Path(env_staging)

SAIDA_TERMOS = PASTA_STAGING / "resumo_termos.csv"
SAIDA_RUBRICAS = PASTA_STAGING / "resumo_rubricas.csv"

logging.basicConfig(level=logging.INFO, format='[ETL-RESUMO] %(asctime)s - %(message)s')

# ======================================================
# FUNÇÕES DE LEITURA (Igual ao antigo script 2)
# ======================================================

def parse_brl(valor):
    """Converte 'R$ 1.200,50' ou '1.200,50' para float."""
    if not valor or str(valor).strip() in ("-", "", "R$ 0,00"):
        return 0.0
    val_str = str(valor).replace("R$", "").strip()
    return float(val_str.replace(".", "").replace(",", ".") or 0)

def ler_linhas_arquivo(caminho):
    """Tenta ler em Latin-1 (padrão bancos BR), fallback para UTF-8."""
    try:
        with open(caminho, "r", encoding="latin-1") as f:
            return f.readlines()
    except UnicodeDecodeError:
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                return f.readlines()
        except Exception as e:
            # Se der erro de leitura, apenas loga e retorna vazio (pula o arquivo)
            return []

def extrair_dados_arquivo(caminho):
    linhas = ler_linhas_arquivo(caminho)
    if not linhas:
        return None, 0.0, []

    nro_sit = None
    rendimento_total = 0.0
    rubricas = []

    # Flags de controle
    dentro_rendimentos = False
    dentro_despesas = False

    for linha in linhas:
        linha = linha.strip()
        partes = linha.split(";")

        # 1. Identificar o SIT (A chave para saber se o arquivo é válido)
        if linha.startswith("Nº SIT"):
            if len(partes) > 1:
                nro_sit = partes[1].strip()
            continue

        # Se ainda não achou SIT, não adianta processar o resto da linha
        if not nro_sit:
            continue

        # 2. Identificar Rendimentos
        if "Detalhes dos Rendimentos de Aplicações Financeiras" in linha:
            dentro_rendimentos = True
            continue
        
        if dentro_rendimentos and linha.startswith("T O T A L"):
            if len(partes) > 1:
                rendimento_total = parse_brl(partes[1])
            dentro_rendimentos = False

        # 3. Identificar Rubricas (Despesas/Estornos)
        if "Detalhe das Despesas" in linha:
            dentro_despesas = True
            continue

        if dentro_despesas:
            if not linha or "T O T A L" in linha or linha.startswith("Despesa;"):
                continue
            
            if len(partes) >= 5:
                cod_full = partes[0].strip()
                valor_est = parse_brl(partes[4]) # Coluna do valor estornado
                cod_rubrica = cod_full.split("-")[0].strip()

                if cod_rubrica:
                    rubricas.append({
                        "rubrica": cod_rubrica,
                        "valor_estornado": valor_est
                    })

    return nro_sit, rendimento_total, rubricas

# ======================================================
# EXECUÇÃO
# ======================================================

def executar_etl():
    logging.info(f"➡️  Varrendo pasta: {PASTA_DOWNLOADS}")

    lista_termos = []
    lista_rubricas = []

    # Pega TODOS os CSVs, independente do nome (arquivo.csv, arquivo (1).csv, etc)
    arquivos = list(PASTA_DOWNLOADS.glob("*.csv"))
    
    if not arquivos:
        logging.warning("⚠️  Nenhum arquivo .csv encontrado em Downloads.")
        return

    count_processados = 0
    count_ignorados = 0

    for caminho in arquivos:
        try:
            # Tenta extrair. Se não tiver "Nº SIT", retorna None e ignoramos o arquivo.
            sit, rendimento, rubs = extrair_dados_arquivo(caminho)

            if sit:
                # Se achou SIT, é um arquivo válido do sistema
                lista_termos.append({
                    "nro_sit": sit,
                    "rendimento_financeiro_total": rendimento
                })

                for r in rubs:
                    lista_rubricas.append({
                        "nro_sit": sit,
                        "rubrica": r["rubrica"],
                        "valor_estornado": r["valor_estornado"],
                        "id_termo_rubrica": f"{sit}-{r['rubrica']}"
                    })
                
                count_processados += 1
                logging.info(f"✔️  SIT {sit} encontrado em '{caminho.name}'")
            else:
                # Arquivo CSV aleatório (não é do sistema)
                count_ignorados += 1

        except Exception as e:
            logging.error(f"❌ Erro ao ler {caminho.name}: {e}")

    # --- Salvar Staging ---
    if lista_termos:
        PASTA_STAGING.mkdir(parents=True, exist_ok=True)
        
        df_termos = pd.DataFrame(lista_termos)
        df_rubricas = pd.DataFrame(lista_rubricas)

        # Remove duplicatas (caso tenha baixado o mesmo relatório 2x)
        df_termos.drop_duplicates(subset=["nro_sit"], keep="last", inplace=True)
        if not df_rubricas.empty:
            df_rubricas.drop_duplicates(subset=["id_termo_rubrica"], keep="last", inplace=True)

        df_termos.to_csv(SAIDA_TERMOS, index=False)
        df_rubricas.to_csv(SAIDA_RUBRICAS, index=False)

        logging.info("="*40)
        logging.info(f"💾 PROCESSAMENTO CONCLUÍDO")
        logging.info(f"   - Arquivos processados: {count_processados}")
        logging.info(f"   - Arquivos ignorados (sem SIT): {count_ignorados}")
        logging.info(f"   - Termos extraídos: {len(df_termos)}")
        logging.info(f"   - Saída: {PASTA_STAGING}")
        logging.info("="*40)
    else:
        logging.warning("⚠️ Nenhum arquivo válido com 'Nº SIT' foi encontrado.")

if __name__ == "__main__":
    executar_etl()