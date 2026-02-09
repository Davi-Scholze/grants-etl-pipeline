import pandas as pd
import pyodbc
import hashlib
import logging
import os
from dotenv import load_dotenv

# =============== CONFIGURAÇÕES ===============

load_dotenv()

DIR_STAGING = os.getenv("DIR_STAGING")
CONN_STR = os.getenv("CONN_STR_SQLSERVER")

if not DIR_STAGING:
    raise ValueError("ERRO: Variável DIR_STAGING não definida no .env")

ARQUIVO_ENTRADA = os.path.join(DIR_STAGING, "despesas_geral.csv")
ARQUIVO_SAIDA = os.path.join(DIR_STAGING, "despesas_upload.csv")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =============== FUNÇÕES ===============

def gerar_fingerprint(row):
    """
    Gera um hash único baseado no CONTEÚDO da linha (excluindo o ID).
    Isso serve para detectar se algo mudou.
    """
    # Concatena todos os campos relevantes para comparação
    # Atenção: Converta tudo para string e trate floats uniformemente
    raw = (
        f"{row['termo']}|{row['rubrica']}|{row['tipo_despesa']}|"
        f"{row['cpf_cnpj']}|{row['favorecido']}|{row['tipo_doc_despesa']}|"
        f"{row['descricao_despesa']}|{row['tipo_doc_pagamento']}|"
        f"{str(row['data_pagamento'])}|{str(row['data_debito_convenio'])}|"
        f"{float(row['valor']):.2f}|{row['id_termo_rubrica']}"
    )
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def processar_comparacao():
    logging.info("➡️ INÍCIO ETAPA 2: Comparação Inteligente (Insert/Update)")

    if not os.path.exists(ARQUIVO_ENTRADA):
        logging.error("❌ Arquivo geral não encontrado. Rode a etapa 1.")
        return

    # 1. Carrega CSV (Origem)
    df_csv = pd.read_csv(ARQUIVO_ENTRADA, dtype=str)
    if df_csv.empty:
        logging.warning("⚠️ CSV de entrada está vazio")
        return

    # Garante que valor é float para hash bater com a lógica antiga/nova
    df_csv["valor"] = pd.to_numeric(df_csv["valor"], errors="coerce").fillna(0.0)
    
    # Gera o fingerprint do CSV atual
    df_csv["fingerprint_csv"] = df_csv.apply(gerar_fingerprint, axis=1)

    # 2. Carrega Dados do Banco (Destino)
    # Precisamos puxar o id_codigo_sit e todas colunas para gerar o hash igual,
    # ou confiar que o banco traga os dados puros.
    
    dict_banco = {} # Formato: { 'id_codigo_sit': 'fingerprint_hash' }

    try:
        logging.info("🔍 Consultando banco de dados para mapear estado atual...")
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Puxa todas as colunas que usamos no hash
        query = """
            SELECT 
                id_codigo_sit, termo, rubrica, tipo_despesa, cpf_cnpj, favorecido,
                tipo_doc_despesa, descricao_despesa, tipo_doc_pagamento,
                data_pagamento, data_debito_convenio, valor, id_termo_rubrica
            FROM despesas
            WHERE id_codigo_sit IS NOT NULL
        """
        cursor.execute(query)

        col_names = [column[0] for column in cursor.description]
        
        for row in cursor.fetchall():
            # Transforma a row do banco em um dicionário para facilitar
            row_dict = dict(zip(col_names, row))
            
            # Tratamentos para bater com o CSV (ex: Datas e Nulls)
            # Precisamos garantir que o formato string seja idêntico ao do CSV
            # Se o banco retorna date object, converter para string YYYY-MM-DD
            
            data_pagto = str(row_dict['data_pagamento']) if row_dict['data_pagamento'] else ""
            data_debito = str(row_dict['data_debito_convenio']) if row_dict['data_debito_convenio'] else ""
            
            # Ajuste de None para string vazia nas strings
            row_limpa = {
                'termo': str(row_dict['termo']),
                'rubrica': str(row_dict['rubrica']),
                'tipo_despesa': str(row_dict['tipo_despesa']),
                'cpf_cnpj': str(row_dict['cpf_cnpj']),
                'favorecido': str(row_dict['favorecido']),
                'tipo_doc_despesa': str(row_dict['tipo_doc_despesa'] or "").strip(),
                'descricao_despesa': str(row_dict['descricao_despesa'] or "").strip(),
                'tipo_doc_pagamento': str(row_dict['tipo_doc_pagamento'] or "").strip(),
                'data_pagamento': data_pagto,
                'data_debito_convenio': data_debito,
                'valor': float(row_dict['valor'] or 0.0),
                'id_termo_rubrica': str(row_dict['id_termo_rubrica'])
            }

            # Gera Hash do Banco
            h_banco = gerar_fingerprint(row_limpa)
            
            # Chave do dicionário é o ID
            id_sit = str(row_dict['id_codigo_sit'])
            dict_banco[id_sit] = h_banco

        conn.close()
        logging.info(f"📦 Carregados {len(dict_banco)} registros do banco para memória.")

    except Exception as e:
        logging.error(f"❌ Erro ao conectar/ler banco: {e}")
        return

    # 3. Classificação (INSERT, UPDATE, IGNORE)
    lista_final = []

    inserts = 0
    updates = 0
    ignorados = 0

    for _, row in df_csv.iterrows():
        id_atual = str(row['id_codigo_sit'])
        hash_atual = row['fingerprint_csv']

        if id_atual not in dict_banco:
            # Não existe no banco -> INSERT
            row['acao'] = 'INSERT'
            lista_final.append(row)
            inserts += 1
        else:
            # Existe no banco, verificar se mudou
            hash_banco = dict_banco[id_atual]
            if hash_atual != hash_banco:
                # Hash diferente -> UPDATE
                row['acao'] = 'UPDATE'
                lista_final.append(row)
                updates += 1
            else:
                # Tudo igual -> IGNORE
                ignorados += 1

    # Remove a coluna temporária de fingerprint antes de salvar
    df_final = pd.DataFrame(lista_final)
    if 'fingerprint_csv' in df_final.columns:
        df_final.drop(columns=['fingerprint_csv'], inplace=True)

    # 4. Salvar Resultado
    if not df_final.empty:
        df_final.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")
        logging.info(f"✅ Gerado: {ARQUIVO_SAIDA}")
        logging.info(f"📊 Resumo: {inserts} Novos (INSERT) | {updates} Alterados (UPDATE) | {ignorados} Iguais (IGNORADOS)")
    else:
        logging.info("🎉 Nenhuma alteração necessária (Banco sincronizado).")
        if os.path.exists(ARQUIVO_SAIDA):
            os.remove(ARQUIVO_SAIDA)

if __name__ == "__main__":
    processar_comparacao()