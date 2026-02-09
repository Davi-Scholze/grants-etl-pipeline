import os
import logging
import pandas as pd
import pyodbc
import warnings
from pathlib import Path
from dotenv import load_dotenv

# ======================================================
# CONFIGURAÇÕES
# ======================================================
load_dotenv()

PASTA_STAGING = Path(os.getenv("DIR_STAGING", "."))
ENTRADA_TERMOS = PASTA_STAGING / "resumo_termos.csv"
ENTRADA_RUBRICAS = PASTA_STAGING / "resumo_rubricas.csv"

SAIDA_UPDATE_TERMOS = PASTA_STAGING / "update_termos.csv"
SAIDA_UPDATE_RUBRICAS = PASTA_STAGING / "update_rubricas.csv"

CONN_STR = os.getenv("CONN_STR_SQLSERVER")

logging.basicConfig(level=logging.INFO, format='%(message)s')
warnings.filterwarnings("ignore", category=UserWarning)

def limpar_string_numero(serie):
    """
    Remove .0 de números convertidos para string.
    Ex: "67303.0" -> "67303", 67303 -> "67303"
    """
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

def validar_updates():
    print("="*60)
    logging.info("🕵️  INICIANDO VALIDAÇÃO DE DADOS (CSV vs SQL SERVER)")
    print("="*60)

    if not ENTRADA_TERMOS.exists():
        logging.error("❌ Erro: Arquivos da etapa 1 não encontrados.")
        return

    try:
        conn = pyodbc.connect(CONN_STR)
    except Exception as e:
        logging.error(f"❌ Falha na conexão com o banco: {e}")
        return

    total_updates = 0
    mapa_sit_para_id_termo = {}

    # ======================================================
    # 0. PREPARAÇÃO: CRIAR MAPA SIT -> ID_TERMO
    # ======================================================
    # Aqui resolvemos o problema de tradução (Ex: 57884 vira 6373)
    try:
        # Pega IDs e SITs do banco
        query_mapa = "SELECT id_termo, nro_sit FROM termos"
        df_mapa = pd.read_sql(query_mapa, conn)
        
        # Limpeza agressiva para garantir que textos e números batam
        df_mapa["nro_sit"] = limpar_string_numero(df_mapa["nro_sit"])
        df_mapa["id_termo"] = limpar_string_numero(df_mapa["id_termo"])
        
        # Cria o dicionário { '57884': '6373', ... }
        mapa_sit_para_id_termo = df_mapa.set_index("nro_sit")["id_termo"].to_dict()
        
    except Exception as e:
        logging.error(f"❌ Erro ao criar mapa de IDs: {e}")
        return

    # ======================================================
    # 1. COMPARAR TERMOS
    # ======================================================
    logging.info("1️⃣  Analisando Termos (Rendimentos)...")
    
    df_termos_csv = pd.read_csv(ENTRADA_TERMOS, dtype=str)
    df_termos_csv["nro_sit"] = limpar_string_numero(df_termos_csv["nro_sit"]) # Limpa SIT do CSV
    df_termos_csv["rendimento_financeiro_total"] = pd.to_numeric(df_termos_csv["rendimento_financeiro_total"], errors="coerce").fillna(0.0)

    try:
        query_termos = "SELECT nro_sit, rendimento_financeiro_total FROM termos"
        df_termos_sql = pd.read_sql(query_termos, conn)
        df_termos_sql["nro_sit"] = limpar_string_numero(df_termos_sql["nro_sit"]) # Limpa SIT do SQL

        merged_termos = df_termos_csv.merge(
            df_termos_sql, 
            on="nro_sit", 
            how="inner", 
            suffixes=("_csv", "_sql")
        )

        diff_termos = merged_termos[
            (merged_termos["rendimento_financeiro_total_csv"] - merged_termos["rendimento_financeiro_total_sql"]).abs() > 0.01
        ]

        if not diff_termos.empty:
            diff_termos[["nro_sit", "rendimento_financeiro_total_csv"]].to_csv(SAIDA_UPDATE_TERMOS, index=False)
            logging.info(f"   ⚠️  DIVERGÊNCIA: {len(diff_termos)} termos para atualizar.")
            total_updates += len(diff_termos)
        else:
            if SAIDA_UPDATE_TERMOS.exists(): os.remove(SAIDA_UPDATE_TERMOS)
            logging.info("   ✅  Termos sincronizados.")

    except Exception as e:
        logging.error(f"   ❌ Erro em Termos: {e}")

    # ======================================================
    # 2. COMPARAR RUBRICAS (AQUI ESTÁ A CORREÇÃO DA CHAVE)
    # ======================================================
    logging.info("2️⃣  Analisando Rubricas (Estornos)...")

    if ENTRADA_RUBRICAS.exists():
        df_rubs_csv = pd.read_csv(ENTRADA_RUBRICAS, dtype=str)
        df_rubs_csv["valor_estornado"] = pd.to_numeric(df_rubs_csv["valor_estornado"], errors="coerce").fillna(0.0)
        
        # Limpa o SIT que veio do CSV
        df_rubs_csv["nro_sit"] = limpar_string_numero(df_rubs_csv["nro_sit"])

        # >>> O PULO DO GATO <<<
        # Traduz o SIT (csv) para ID_TERMO (banco) usando o mapa que criamos no início
        df_rubs_csv['id_termo_real'] = df_rubs_csv['nro_sit'].map(mapa_sit_para_id_termo)
        
        # Remove quem não achou par no banco (segurança)
        linhas_validas = df_rubs_csv.dropna(subset=['id_termo_real']).copy()

        if linhas_validas.empty:
            logging.warning("   ⚠️  Aviso: Nenhuma rubrica teve o SIT encontrado no Banco. Verifique se os SITs dos arquivos existem na tabela 'termos'.")
        else:
            # Recria a chave composta correta: "6373-3.3.90..."
            linhas_validas['id_termo_rubrica_corrigido'] = (
                linhas_validas['id_termo_real'] + "-" + linhas_validas['rubrica']
            )

            try:
                query_rubs = "SELECT id_termo_rubrica, valor_estornado FROM rubricas"
                df_rubs_sql = pd.read_sql(query_rubs, conn)
                
                # Merge usando a chave CORRIGIDA
                merged_rubs = linhas_validas.merge(
                    df_rubs_sql,
                    left_on="id_termo_rubrica_corrigido", 
                    right_on="id_termo_rubrica",          
                    how="inner",
                    suffixes=("_csv", "_sql")
                )

                diff_rubs = merged_rubs[
                    (merged_rubs["valor_estornado_csv"] - merged_rubs["valor_estornado_sql"]).abs() > 0.01
                ]

                if not diff_rubs.empty:
                    # Prepara saída renomeando a coluna corrigida para o nome padrão do banco
                    out = diff_rubs[["id_termo_rubrica_corrigido", "valor_estornado_csv"]].rename(
                        columns={"id_termo_rubrica_corrigido": "id_termo_rubrica", "valor_estornado_csv": "valor_estornado"}
                    )
                    out.to_csv(SAIDA_UPDATE_RUBRICAS, index=False)
                    logging.info(f"   ⚠️  DIVERGÊNCIA: {len(diff_rubs)} rubricas para atualizar.")
                    total_updates += len(diff_rubs)
                else:
                    if SAIDA_UPDATE_RUBRICAS.exists(): os.remove(SAIDA_UPDATE_RUBRICAS)
                    logging.info("   ✅  Rubricas sincronizadas.")

            except Exception as e:
                logging.error(f"   ❌ Erro em Rubricas: {e}")
    
    conn.close()

    # ======================================================
    # RESUMO
    # ======================================================
    print("-" * 60)
    if total_updates > 0:
        logging.info(f"📢  {total_updates} atualizações pendentes.")
        logging.info("👉  Rode '3_update_sql.py' para aplicar.")
    else:
        logging.info("🎉  Banco 100% atualizado.")
    print("=" * 60)

if __name__ == "__main__":
    validar_updates()