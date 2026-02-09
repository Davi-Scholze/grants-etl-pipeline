import os
import pandas as pd
import logging
from dotenv import load_dotenv

# ================= CONFIGURAÇÕES =================

load_dotenv()

DIR_DOWNLOADS = os.getenv("DIR_DOWNLOADS")
DIR_STAGING = os.getenv("DIR_STAGING")

if not DIR_DOWNLOADS or not DIR_STAGING:
    raise ValueError("ERRO: Variáveis DIR_DOWNLOADS ou DIR_STAGING não definidas no arquivo .env")

ORIGEM_PASTA = DIR_DOWNLOADS
ARQUIVO_SAIDA = os.path.join(DIR_STAGING, "despesas_geral.csv")

# ================= MAPEAMENTOS =================

SIT_TERMO_MAP = {
    "57884": "6373",
    "63377": "6729",
    "66270": "6822",
    "67303": "6893",
    "67669": "6932",
    "71199": "26478",
    "74699": "26672"
}

MAPEAMENTO_DESPESAS = {
    "PESSOAL CIVIL": "PESSOAL",
    "OBRIGAÇÕES PATRONAIS": "ENCARGOS",
    "MATERIAIS DE CONSUMO": "MATERIAIS DE CONSUMO",
    "SERVIÇOS DE TERCEIROS": "SERVIÇOS DE TERCEIROS"
}

# Adicionado id_codigo_sit no início
COLUNAS_ORDEM = [
    "id_codigo_sit", "termo", "rubrica", "tipo_despesa", "cpf_cnpj", "favorecido",
    "tipo_doc_despesa", "descricao_despesa", "tipo_doc_pagamento",
    "data_pagamento", "data_debito_convenio", "valor", "id_termo_rubrica"
]

# ================= LOG =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= FUNÇÕES AUXILIARES =================

def classificar_tipo(texto):
    texto = str(texto).upper()
    for chave, valor in MAPEAMENTO_DESPESAS.items():
        if chave in texto:
            return valor
    return "OUTROS"


def limpar_cpf(v):
    v = "".join(filter(str.isdigit, str(v)))
    if len(v) <= 11:
        return v.zfill(11)
    return v.zfill(14)


# ================= ETL =================

def executar_etl():
    logging.info("➡️ INÍCIO ETAPA 1: Consolidação dos arquivos de despesas")

    dados_consolidados = []

    for sit, termo in SIT_TERMO_MAP.items():
        caminho_arquivo = os.path.join(ORIGEM_PASTA, f"Despesas_SIT_{sit}.xlsx")

        if not os.path.exists(caminho_arquivo):
            logging.warning(f"⚠️ Arquivo não encontrado: {caminho_arquivo}")
            continue

        try:
            # Lê tudo como string para evitar problemas de tipagem inicial
            df = pd.read_excel(caminho_arquivo, dtype=str)

            if "Data do Pagamento" not in df.columns:
                logging.warning(f"⚠️ Coluna 'Data do Pagamento' ausente em {caminho_arquivo}")
                continue

            # ================= TRATAMENTO DE DATAS =================

            df["data_pagto_temp"] = pd.to_datetime(
                df["Data do Pagamento"], dayfirst=True, errors="coerce"
            )

            col_debito = None
            if "Data Débito Conta Convêvio" in df.columns:
                col_debito = "Data Débito Conta Convêvio"
            elif "Data Débito Conta Convênio" in df.columns:
                col_debito = "Data Débito Conta Convênio"

            if col_debito:
                df["data_debito_temp"] = pd.to_datetime(
                    df[col_debito], dayfirst=True, errors="coerce"
                )
            else:
                df["data_debito_temp"] = pd.NaT

            df = df.dropna(subset=["data_pagto_temp"])

            # ================= CONSTRUÇÃO DO DATAFRAME LIMPO =================

            novo = pd.DataFrame(index=df.index)

            # --- NOVA COLUNA: CÓDIGO SIT ---
            # Verifica se a coluna "Código" existe, senão preenche vazio ou trata erro
            if "Código" in df.columns:
                novo["id_codigo_sit"] = df["Código"].fillna("").str.strip()
            else:
                # Se for crítico não ter o código, mude para raise ValueError
                novo["id_codigo_sit"] = "" 

            novo["termo"] = termo
            
            novo["rubrica"] = (
                df["Tipo de Despesa"]
                .str.extract(r"^([\d\.]+)", expand=False)
                .fillna("")
                .str.strip()
            )

            novo["tipo_despesa"] = df["Tipo de Despesa"].apply(classificar_tipo)
            novo["cpf_cnpj"] = df["CPF/CNPJ"].apply(limpar_cpf)
            novo["favorecido"] = df["Favorecido"].fillna("").str.strip()

            novo["tipo_doc_despesa"] = (
                df["Tipo Documento Despesa"].fillna("").str.strip()
                if "Tipo Documento Despesa" in df.columns else ""
            )

            novo["descricao_despesa"] = df["Descrição da Despesa"].fillna("").str.strip()

            novo["tipo_doc_pagamento"] = (
                df["Tipo Documento Pagamento"].fillna("").str.strip()
                if "Tipo Documento Pagamento" in df.columns else ""
            )

            novo["data_pagamento"] = df["data_pagto_temp"].dt.strftime("%Y-%m-%d")
            novo["data_debito_convenio"] = (
                df["data_debito_temp"].dt.strftime("%Y-%m-%d").fillna("")
            )

            novo["valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

            novo["id_termo_rubrica"] = novo["termo"] + "-" + novo["rubrica"]

            # Força a ordem das colunas
            novo = novo[COLUNAS_ORDEM]

            dados_consolidados.append(novo)

            logging.info(f"✔️ {caminho_arquivo} | {len(novo)} linhas processadas (Termo: {termo})")

        except Exception as e:
            logging.error(f"❌ Erro ao processar {caminho_arquivo}: {e}")

    # ================= SAÍDA =================

    if dados_consolidados:
        df_final = pd.concat(dados_consolidados, ignore_index=True)

        os.makedirs(DIR_STAGING, exist_ok=True)

        df_final.to_csv(
            ARQUIVO_SAIDA,
            index=False,
            encoding="utf-8-sig",
            sep=","
        )

        logging.info(f"💾 Arquivo final gerado com {len(df_final)} linhas")
        logging.info(f"📄 Caminho: {ARQUIVO_SAIDA}")

    else:
        logging.warning("⚠️ Nenhum dado válido encontrado para consolidação")

if __name__ == "__main__":
    executar_etl()