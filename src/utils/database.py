"""
💾 MÓDULO DE ACESSO À BASE DE DADOS
Fornece conexões e utilitários para SQL Server
"""

import pyodbc
import pandas as pd
from src.utils.config import Config
from src.utils.logger import setup_logger


logger = setup_logger("Database")


class DatabaseManager:
    """Gerenciador de conexões e operações SQL Server"""
    
    def __init__(self):
        self.conn_str = Config.CONN_STR_SQLSERVER
        
    def get_connection(self):
        """
        Obtém uma conexão ativa com SQL Server
        
        Returns:
            pyodbc.Connection
        """
        try:
            conn = pyodbc.connect(self.conn_str)
            logger.info("✅ Conectado ao SQL Server")
            return conn
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao SQL Server: {e}")
            raise
    
    def execute_query(self, query: str, params: list = None):
        """
        Executa uma query SELECT e retorna os resultados
        
        Args:
            query: Query SQL
            params: Parâmetros para evitar SQL injection
            
        Returns:
            pd.DataFrame
        """
        conn = self.get_connection()
        try:
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)
            logger.info(f"✅ Query executada: {len(df)} linhas retornadas")
            return df
        except Exception as e:
            logger.error(f"❌ Erro ao executar query: {e}")
            raise
        finally:
            conn.close()
    
    def execute_insert_update(self, query: str, params_list: list) -> int:
        """
        Executa INSERTs ou UPDATEs em lote
        
        Args:
            query: Query SQL (com placeholders ?)
            params_list: Lista de tuplas com parâmetros
            
        Returns:
            Número de registros afetados
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            affected = cursor.rowcount
            logger.info(f"✅ {affected} registros afetados")
            return affected
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao executar operação: {e}")
            raise
        finally:
            conn.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica se uma tabela existe no banco"""
        query = f"""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        """
        try:
            result = self.execute_query(query, [table_name])
            return not result.empty
        except:
            return False


# Instância global
db_manager = DatabaseManager()
