"""
📤 WRAPPER DA ETAPA DE CARGA
Encapsula os scripts 3_load_sql.py e 3_update_sql.py
"""

import os
import pandas as pd
import pyodbc
from pathlib import Path

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.utils.ingestor import limpar_string_numero

logger = setup_logger("ExpensesLoader")


class ExpensesLoader:
    """Carregador centralizado para banco de dados"""
    
    def __init__(self):
        self.dir_staging = Config.DIR_STAGING
        self.conn_str = Config.CONN_STR_SQLSERVER
        
        if not self.dir_staging:
            raise ValueError("❌ DIR_STAGING não definido")
        
        Path(self.dir_staging).mkdir(parents=True, exist_ok=True)
    
    def run(self) -> bool:
        """
        Executa toda a carga no banco de dados
        
        Returns:
            True se sucesso, False caso contrário
        """
        logger.info("=" * 60)
        logger.info("📤 INICIANDO ETAPA 3: CARGA NO BANCO DE DADOS")
        logger.info("=" * 60)
        
        try:
            # Carrega despesas (INSERT/UPDATE)
            sucesso_despesa = self.carregar_despesas()
            
            # Atualiza termos e rubricas
            sucesso_atualizacao = self.atualizar_financeiro()
            
            if sucesso_despesa or sucesso_atualizacao:
                logger.info("✅ CARGA CONCLUÍDA COM SUCESSO")
                return True
            else:
                logger.warning("⚠️  Nenhuma carga foi realizada")
                return False
                
        except Exception as e:
            logger.error(f"💥 ERRO NA CARGA: {e}", exc_info=True)
            return False
    
    def carregar_despesas(self) -> bool:
        """
        Carrega despesas (INSERT e UPDATE) no banco
        """
        logger.info("➡️ Etapa 3a: Carga de Despesas (INSERT/UPDATE)")
        
        arquivo_upload = os.path.join(self.dir_staging, "despesas_upload.csv")
        
        if not os.path.exists(arquivo_upload):
            logger.info("   ℹ️  Nenhum arquivo de upload (dados já sincronizados)")
            return False
        
        df = pd.read_csv(arquivo_upload, dtype=str)
        if df.empty:
            logger.info("   ℹ️  Arquivo vazio, nada para carregar")
            return False
        
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            sql_insert = """
                INSERT INTO despesas (
                    id_codigo_sit, termo, rubrica, tipo_despesa, cpf_cnpj, favorecido,
                    tipo_doc_despesa, descricao_despesa, tipo_doc_pagamento,
                    data_pagamento, data_debito_convenio, valor, id_termo_rubrica
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            sql_update = """
                UPDATE despesas SET
                    termo = ?, rubrica = ?, tipo_despesa = ?, cpf_cnpj = ?, 
                    favorecido = ?, tipo_doc_despesa = ?, descricao_despesa = ?, 
                    tipo_doc_pagamento = ?, data_pagamento = ?, data_debito_convenio = ?, 
                    valor = ?, id_termo_rubrica = ?
                WHERE id_codigo_sit = ?
            """
            
            cnt_insert = 0
            cnt_update = 0
            erros = 0
            
            for _, row in df.iterrows():
                try:
                    # Trata nulos
                    data_debito = row['data_debito_convenio']
                    if pd.isna(data_debito) or str(data_debito).strip() == "":
                        data_debito = None
                    
                    val = float(row['valor'])
                    acao = row.get('acao', 'INSERT')
                    
                    if acao == 'INSERT':
                        params = (
                            row['id_codigo_sit'], row['termo'], row['rubrica'],
                            row['tipo_despesa'], row['cpf_cnpj'], row['favorecido'],
                            row['tipo_doc_despesa'], row['descricao_despesa'],
                            row['tipo_doc_pagamento'], row['data_pagamento'],
                            data_debito, val, row['id_termo_rubrica']
                        )
                        cursor.execute(sql_insert, params)
                        cnt_insert += 1
                    
                    elif acao == 'UPDATE':
                        params = (
                            row['termo'], row['rubrica'], row['tipo_despesa'],
                            row['cpf_cnpj'], row['favorecido'], row['tipo_doc_despesa'],
                            row['descricao_despesa'], row['tipo_doc_pagamento'],
                            row['data_pagamento'], data_debito, val, row['id_termo_rubrica'],
                            row['id_codigo_sit']
                        )
                        cursor.execute(sql_update, params)
                        cnt_update += 1
                
                except Exception as e:
                    erros += 1
                    logger.error(f"   ❌ Erro no ID {row.get('id_codigo_sit')}: {e}")
            
            conn.commit()
            conn.close()
            
            logger.info(f"   🚀 INSERT: {cnt_insert} | UPDATE: {cnt_update}")
            if erros > 0:
                logger.warning(f"   ⚠️  {erros} erros durante processamento")
            
            # Renomeia para processado
            novo_nome = arquivo_upload.replace(".csv", ".processado.csv")
            if os.path.exists(novo_nome):
                os.remove(novo_nome)
            os.rename(arquivo_upload, novo_nome)
            logger.info(f"   💾 Arquivo renomeado para .processado.csv")
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Erro crítico de carga: {e}")
            return False
    
    def atualizar_financeiro(self) -> bool:
        """
        Atualiza termos e rubricas baseado em divergências encontradas
        """
        logger.info("➡️ Etapa 3b: Atualização de Termos e Rubricas")
        
        pasta_staging = Path(self.dir_staging)
        arq_termos = pasta_staging / "update_termos.csv"
        arq_rubricas = pasta_staging / "update_rubricas.csv"
        
        if not arq_termos.exists() and not arq_rubricas.exists():
            logger.info("   ℹ️  Nada para atualizar (dados sincronizados)")
            return False
        
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
        except Exception as e:
            logger.error(f"❌ Erro de conexão: {e}")
            return False
        
        sucesso = False
        
        # ===== ATUALIZA TERMOS =====
        if arq_termos.exists():
            logger.info("   1️⃣  Atualizando termos...")
            df = pd.read_csv(arq_termos)
            
            sql = "UPDATE termos SET rendimento_financeiro_total = ? WHERE nro_sit = ?"
            params = [
                (float(row["rendimento_financeiro_total_csv"]), 
                 limpar_string_numero(row["nro_sit"]))
                for _, row in df.iterrows()
            ]
            
            try:
                cursor.executemany(sql, params)
                conn.commit()
                logger.info(f"      ✅ {len(df)} termos atualizados")
                os.remove(arq_termos)
                sucesso = True
            except Exception as e:
                logger.error(f"      ❌ Erro: {e}")
        
        # ===== ATUALIZA RUBRICAS =====
        if arq_rubricas.exists():
            logger.info("   2️⃣  Atualizando rubricas...")
            df = pd.read_csv(arq_rubricas)
            
            sql = "UPDATE rubricas SET valor_estornado = ? WHERE id_termo_rubrica = ?"
            params = [
                (float(row["valor_estornado"]), str(row["id_termo_rubrica"]))
                for _, row in df.iterrows()
            ]
            
            try:
                cursor.executemany(sql, params)
                conn.commit()
                logger.info(f"      ✅ {len(df)} rubricas atualizadas")
                os.remove(arq_rubricas)
                sucesso = True
            except Exception as e:
                logger.error(f"      ❌ Erro: {e}")
        
        conn.close()
        return sucesso
