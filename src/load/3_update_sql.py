    import os
    import logging
    import pandas as pd
    import pyodbc
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv()

    PASTA_STAGING = Path(os.getenv("DIR_STAGING"))
    ARQ_TERMOS = PASTA_STAGING / "update_termos.csv"
    ARQ_RUBRICAS = PASTA_STAGING / "update_rubricas.csv"
    CONN_STR = os.getenv("CONN_STR_SQLSERVER")

    logging.basicConfig(level=logging.INFO, format='[ETL-UPDATE] %(asctime)s - %(message)s')

    def tratar_numero_sit(valor):
        """
        Converte '67669.0' ou 67669.0 para '67669' (string limpa).
        Isso evita o erro de conversão para INT no SQL Server.
        """
        try:
            return str(int(float(valor)))
        except:
            return str(valor)

    def atualizar_banco():
        logging.info("🚀 Iniciando atualização no SQL Server...")

        # Se não existem arquivos de update, não faz nada
        if not ARQ_TERMOS.exists() and not ARQ_RUBRICAS.exists():
            logging.info("🏁 Nada a atualizar.")
            return

        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
        except Exception as e:
            logging.error(f"❌ Erro de conexão: {e}")
            return

        # ======================================================
        # 1. Atualiza Termos
        # ======================================================
        if ARQ_TERMOS.exists():
            df = pd.read_csv(ARQ_TERMOS)
            logging.info(f"🔄 Atualizando {len(df)} Termos...")
            
            sql = "UPDATE termos SET rendimento_financeiro_total = ? WHERE nro_sit = ?"
            
            # CORREÇÃO 1: Usamos 'tratar_numero_sit' para remover o .0 do nro_sit
            params = [
                (float(row["rendimento_financeiro_total_csv"]), tratar_numero_sit(row["nro_sit"])) 
                for _, row in df.iterrows()
            ]
            
            try:
                cursor.executemany(sql, params)
                conn.commit()
                logging.info("✔️ Termos atualizados com sucesso.")
                os.remove(ARQ_TERMOS) # Limpa staging
            except Exception as e:
                logging.error(f"❌ Erro ao atualizar termos: {e}")

        # ======================================================
        # 2. Atualiza Rubricas
        # ======================================================
        if ARQ_RUBRICAS.exists():
            df = pd.read_csv(ARQ_RUBRICAS)
            logging.info(f"🔄 Atualizando {len(df)} Rubricas...")

            sql = "UPDATE rubricas SET valor_estornado = ? WHERE id_termo_rubrica = ?"
            
            # CORREÇÃO 2: Alterado de 'valor_estornado_csv' para 'valor_estornado'
            # O script 2 já renomeou essa coluna para o padrão do banco.
            try:
                params = [
                    (float(row["valor_estornado"]), str(row["id_termo_rubrica"])) 
                    for _, row in df.iterrows()
                ]

                cursor.executemany(sql, params)
                conn.commit()
                logging.info("✔️ Rubricas atualizadas com sucesso.")
                os.remove(ARQ_RUBRICAS) # Limpa staging
                
            except KeyError as e:
                logging.error(f"❌ Erro de Coluna: O script não encontrou a coluna {e}. Verifique o CSV gerado.")
            except Exception as e:
                logging.error(f"❌ Erro ao atualizar rubricas: {e}")

        conn.close()
        logging.info("🏁 Processo concluído.")

    if __name__ == "__main__":
        atualizar_banco()