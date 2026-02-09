"""
📥 ETAPA 3 - INSERÇÃO E ATUALIZAÇÃO NO SQL SERVER
"""

import pandas as pd
import pyodbc
import logging
import os
from dotenv import load_dotenv

# ================= CONFIGURAÇÕES =================

load_dotenv()

DIR_STAGING = os.getenv("DIR_STAGING")
CONN_STR = os.getenv("CONN_STR_SQLSERVER")

if not DIR_STAGING:
    raise ValueError("ERRO: DIR_STAGING não está definido no .env")

ARQUIVO_UPLOAD = os.path.join(DIR_STAGING, "despesas_upload.csv")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ================= ETAPA 3 =================

def carregar_banco():
    logging.info("➡️ INÍCIO ETAPA 3: Carga no SQL Server (Insert/Update)")

    if not os.path.exists(ARQUIVO_UPLOAD):
        logging.info("ℹ️ Arquivo de upload não encontrado (sem pendências).")
        return

    df = pd.read_csv(ARQUIVO_UPLOAD, dtype=str)
    if df.empty:
        logging.info("ℹ️ Arquivo vazio, nada para carregar.")
        return

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        cnt_insert = 0
        cnt_update = 0
        erros = 0
        
        # SQL Queries
        sql_insert = """
            INSERT INTO despesas (
                id_codigo_sit, termo, rubrica, tipo_despesa, cpf_cnpj, favorecido,
                tipo_doc_despesa, descricao_despesa, tipo_doc_pagamento,
                data_pagamento, data_debito_convenio, valor, id_termo_rubrica
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        sql_update = """
            UPDATE despesas SET
                termo = ?, rubrica = ?, tipo_despesa = ?, cpf_cnpj = ?, favorecido = ?,
                tipo_doc_despesa = ?, descricao_despesa = ?, tipo_doc_pagamento = ?,
                data_pagamento = ?, data_debito_convenio = ?, valor = ?, id_termo_rubrica = ?
            WHERE id_codigo_sit = ?
        """

        for _, row in df.iterrows():
            try:
                # Tratamento de nulos
                data_debito = row['data_debito_convenio']
                if pd.isna(data_debito) or str(data_debito).strip() == "":
                    data_debito = None
                
                # Prepara valor float
                val = float(row['valor'])
                
                acao = row.get('acao', 'INSERT') # Default para Insert se não houver coluna

                if acao == 'INSERT':
                    params = (
                        row['id_codigo_sit'],
                        row['termo'], row['rubrica'], row['tipo_despesa'], row['cpf_cnpj'], row['favorecido'],
                        row['tipo_doc_despesa'], row['descricao_despesa'], row['tipo_doc_pagamento'],
                        row['data_pagamento'], data_debito, val, row['id_termo_rubrica']
                    )
                    cursor.execute(sql_insert, params)
                    cnt_insert += 1

                elif acao == 'UPDATE':
                    # Note que no Update o id_codigo_sit vai pro final (WHERE)
                    params = (
                        row['termo'], row['rubrica'], row['tipo_despesa'], row['cpf_cnpj'], row['favorecido'],
                        row['tipo_doc_despesa'], row['descricao_despesa'], row['tipo_doc_pagamento'],
                        row['data_pagamento'], data_debito, val, row['id_termo_rubrica'],
                        row['id_codigo_sit'] # WHERE clause
                    )
                    cursor.execute(sql_update, params)
                    cnt_update += 1

            except Exception as e:
                erros += 1
                logging.error(f"Erro no registro ID {row.get('id_codigo_sit')}: {e}")

        conn.commit()
        conn.close()

        logging.info(f"🚀 SUCESSO! Inseridos: {cnt_insert} | Atualizados: {cnt_update}")
        if erros > 0:
            logging.warning(f"⚠️ Houve {erros} falhas de processamento.")

        # Renomeia para processado
        novo_nome = ARQUIVO_UPLOAD.replace(".csv", ".processado.csv")
        if os.path.exists(novo_nome):
            os.remove(novo_nome)
        os.rename(ARQUIVO_UPLOAD, novo_nome)

    except Exception as e:
        logging.error(f"❌ Erro crítico de conexão ou SQL: {e}")


if __name__ == "__main__":
    carregar_banco()