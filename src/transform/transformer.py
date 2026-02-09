"""
🔄 WRAPPER DA ETAPA DE TRANSFORMAÇÃO
Encapsula os scripts 2_transform_compare.py e 2_validate.py
"""

import os
import pandas as pd
import pyodbc
import hashlib
from pathlib import Path

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.utils.database import db_manager
from src.utils.ingestor import limpar_string_numero, parse_brl

logger = setup_logger("ExpensesTransformer")


class ExpensesTransformer:
    """Transformador centralizado com lógica de comparação e validação"""
    
    def __init__(self):
        self.dir_staging = Config.DIR_STAGING
        self.conn_str = Config.CONN_STR_SQLSERVER
        
        if not self.dir_staging:
            raise ValueError("❌ DIR_STAGING não definido")
        
        Path(self.dir_staging).mkdir(parents=True, exist_ok=True)
    
    def run(self) -> bool:
        """
        Executa transformações
        
        Returns:
            True se sucesso, False caso contrário
        """
        logger.info("=" * 60)
        logger.info("🔄 INICIANDO ETAPA 2: TRANSFORMAÇÃO E VALIDAÇÃO")
        logger.info("=" * 60)
        
        try:
            # Transforma despesas (comparação inteligente)
            sucesso_despesa = self.transformar_despesas()
            
            # Valida termos e rubricas
            sucesso_validacao = self.validar_e_preparar()
            
            if sucesso_despesa or sucesso_validacao:
                logger.info("✅ TRANSFORMAÇÃO CONCLUÍDA")
                return True
            else:
                logger.warning("⚠️  Nenhuma transformação foi realizada")
                return False
                
        except Exception as e:
            logger.error(f"💥 ERRO NA TRANSFORMAÇÃO: {e}", exc_info=True)
            return False
    
    def transformar_despesas(self) -> bool:
        """
        Compara despesas do CSV com banco de dados
        Classifica em INSERT, UPDATE, IGNORE
        """
        logger.info("➡️ Etapa 2a: Comparação Inteligente (Despesas)")
        
        arquivo_entrada = os.path.join(self.dir_staging, "despesas_geral.csv")
        arquivo_saida = os.path.join(self.dir_staging, "despesas_upload.csv")
        
        if not os.path.exists(arquivo_entrada):
            logger.error("❌ Arquivo de despesas não encontrado. Rode etapa 1.")
            return False
        
        # Carrega CSV
        df_csv = pd.read_csv(arquivo_entrada, dtype=str)
        if df_csv.empty:
            logger.warning("⚠️  CSV de entrada está vazio")
            return False
        
        # Prepara para hash
        df_csv["valor"] = pd.to_numeric(df_csv["valor"], errors="coerce").fillna(0.0)
        df_csv["fingerprint_csv"] = df_csv.apply(self._gerar_fingerprint, axis=1)
        
        # Carrega dados do banco
        dict_banco = {}
        try:
            logger.info("🔍 Consultando banco de dados...")
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
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
                row_dict = dict(zip(col_names, row))
                
                # Limpa dados para comparação
                data_pagto = str(row_dict['data_pagamento']) if row_dict['data_pagamento'] else ""
                data_debito = str(row_dict['data_debito_convenio']) if row_dict['data_debito_convenio'] else ""
                
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
                
                h_banco = self._gerar_fingerprint(row_limpa)
                id_sit = str(row_dict['id_codigo_sit'])
                dict_banco[id_sit] = h_banco
            
            conn.close()
            logger.info(f"📦 {len(dict_banco)} registros do banco carregados")
            
        except Exception as e:
            logger.error(f"❌ Erro ao ler banco: {e}")
            return False
        
        # Classifica registros
        lista_final = []
        inserts = 0
        updates = 0
        ignorados = 0
        
        for _, row in df_csv.iterrows():
            id_atual = str(row['id_codigo_sit'])
            hash_atual = row['fingerprint_csv']
            
            if id_atual not in dict_banco:
                row['acao'] = 'INSERT'
                lista_final.append(row)
                inserts += 1
            else:
                hash_banco = dict_banco[id_atual]
                if hash_atual != hash_banco:
                    row['acao'] = 'UPDATE'
                    lista_final.append(row)
                    updates += 1
                else:
                    ignorados += 1
        
        # Salva resultado
        if lista_final:
            df_final = pd.DataFrame(lista_final)
            df_final[Config.COLUNAS_DESPESAS + ['acao']].to_csv(arquivo_saida, index=False, encoding="utf-8")
            logger.info(f"   📊 INSERT: {inserts} | UPDATE: {updates} | IGNORE: {ignorados}")
            logger.info(f"   💾 {len(lista_final)} registros salvos para upload")
            return True
        else:
            logger.info("   ✅ Nada para atualizar (banco sincronizado)")
            return False
    
    def validar_e_preparar(self) -> bool:
        """
        Valida termos e rubricas contra banco
        Prepara arquivos de update
        """
        logger.info("➡️ Etapa 2b: Validação de Termos e Rubricas")
        
        pasta_staging = Path(self.dir_staging)
        entrada_termos = pasta_staging / "resumo_termos.csv"
        entrada_rubricas = pasta_staging / "resumo_rubricas.csv"
        saida_update_termos = pasta_staging / "update_termos.csv"
        saida_update_rubricas = pasta_staging / "update_rubricas.csv"
        
        if not entrada_termos.exists():
            logger.warning("   ⚠️  Arquivo de resumo_termos não encontrado")
            return False
        
        try:
            conn = pyodbc.connect(self.conn_str)
        except Exception as e:
            logger.error(f"❌ Erro de conexão com banco: {e}")
            return False
        
        mapa_sit_para_id = {}
        sucesso = False
        
        # Cria mapa SIT -> ID_TERMO
        try:
            query_mapa = "SELECT id_termo, nro_sit FROM termos"
            df_mapa = pd.read_sql(query_mapa, conn)
            df_mapa["nro_sit"] = df_mapa["nro_sit"].apply(limpar_string_numero)
            df_mapa["id_termo"] = df_mapa["id_termo"].apply(limpar_string_numero)
            mapa_sit_para_id = df_mapa.set_index("nro_sit")["id_termo"].to_dict()
        except Exception as e:
            logger.error(f"❌ Erro ao criar mapa: {e}")
            return False
        
        # ===== VALIDAR TERMOS =====
        logger.info("1️⃣  Analisando Termos...")
        
        df_termos_csv = pd.read_csv(entrada_termos, dtype=str)
        df_termos_csv["nro_sit"] = df_termos_csv["nro_sit"].apply(limpar_string_numero)
        df_termos_csv["rendimento_financeiro_total"] = pd.to_numeric(
            df_termos_csv["rendimento_financeiro_total"], errors="coerce"
        ).fillna(0.0)
        
        try:
            query_termos = "SELECT nro_sit, rendimento_financeiro_total FROM termos"
            df_termos_sql = pd.read_sql(query_termos, conn)
            df_termos_sql["nro_sit"] = df_termos_sql["nro_sit"].apply(limpar_string_numero)
            
            merged_termos = df_termos_csv.merge(
                df_termos_sql,
                on="nro_sit",
                how="inner",
                suffixes=("_csv", "_sql")
            )
            
            diff_termos = merged_termos[
                (merged_termos["rendimento_financeiro_total_csv"] - 
                 merged_termos["rendimento_financeiro_total_sql"]).abs() > 0.01
            ]
            
            if not diff_termos.empty:
                diff_termos[["nro_sit", "rendimento_financeiro_total_csv"]].to_csv(
                    saida_update_termos, index=False
                )
                logger.info(f"   ⚠️  {len(diff_termos)} termos divergentes encontrados")
                sucesso = True
            else:
                if saida_update_termos.exists():
                    os.remove(saida_update_termos)
                logger.info("   ✅ Termos sincronizados")
        
        except Exception as e:
            logger.error(f"   ❌ Erro em termos: {e}")
        
        # ===== VALIDAR RUBRICAS =====
        logger.info("2️⃣  Analisando Rubricas...")
        
        if entrada_rubricas.exists():
            df_rubs_csv = pd.read_csv(entrada_rubricas, dtype=str)
            df_rubs_csv["valor_estornado"] = pd.to_numeric(
                df_rubs_csv["valor_estornado"], errors="coerce"
            ).fillna(0.0)
            df_rubs_csv["nro_sit"] = df_rubs_csv["nro_sit"].apply(limpar_string_numero)
            
            # Traduz SIT para ID_TERMO
            df_rubs_csv['id_termo_real'] = df_rubs_csv['nro_sit'].map(mapa_sit_para_id)
            linhas_validas = df_rubs_csv.dropna(subset=['id_termo_real']).copy()
            
            if linhas_validas.empty:
                logger.warning("   ⚠️  Nenhuma rubrica encontrou par no banco")
            else:
                linhas_validas['id_termo_rubrica_corrigido'] = (
                    linhas_validas['id_termo_real'] + "-" + linhas_validas['rubrica']
                )
                
                try:
                    query_rubs = "SELECT id_termo_rubrica, valor_estornado FROM rubricas"
                    df_rubs_sql = pd.read_sql(query_rubs, conn)
                    
                    merged_rubs = linhas_validas.merge(
                        df_rubs_sql,
                        left_on="id_termo_rubrica_corrigido",
                        right_on="id_termo_rubrica",
                        how="inner",
                        suffixes=("_csv", "_sql")
                    )
                    
                    diff_rubs = merged_rubs[
                        (merged_rubs["valor_estornado_csv"] - 
                         merged_rubs["valor_estornado_sql"]).abs() > 0.01
                    ]
                    
                    if not diff_rubs.empty:
                        diff_rubs[["id_termo_rubrica", "valor_estornado_csv"]].rename(
                            columns={"valor_estornado_csv": "valor_estornado"}
                        ).to_csv(saida_update_rubricas, index=False)
                        logger.info(f"   ⚠️  {len(diff_rubs)} rubricas divergentes encontradas")
                        sucesso = True
                    else:
                        if saida_update_rubricas.exists():
                            os.remove(saida_update_rubricas)
                        logger.info("   ✅ Rubricas sincronizadas")
                
                except Exception as e:
                    logger.error(f"   ❌ Erro em rubricas: {e}")
        
        conn.close()
        return sucesso
    
    @staticmethod
    def _gerar_fingerprint(row) -> str:
        """Gera hash único baseado no conteúdo"""
        raw = (
            f"{row['termo']}|{row['rubrica']}|{row['tipo_despesa']}|"
            f"{row['cpf_cnpj']}|{row['favorecido']}|{row['tipo_doc_despesa']}|"
            f"{row['descricao_despesa']}|{row['tipo_doc_pagamento']}|"
            f"{str(row['data_pagamento'])}|{str(row['data_debito_convenio'])}|"
            f"{float(row['valor']):.2f}|{row['id_termo_rubrica']}"
        )
        return hashlib.md5(raw.encode('utf-8')).hexdigest()
