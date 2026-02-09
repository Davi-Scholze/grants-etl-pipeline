"""
🎯 PIPELINE ETL MASTER - GRANTS MANAGEMENT
Orquestra todo o fluxo de extração, transformação e carga de dados
"""

import sys
import time
import os
import pandas as pd
from pathlib import Path
from src.extract.expenses import ExpensesExtractor
from src.transform.transformer import ExpensesTransformer
from src.load.loader import ExpensesLoader
from src.utils.logger import setup_logger
from src.utils.config import Config
from src.utils.ingestor import copiar_downloads_para_raw

logger = setup_logger("MainPipeline")


def validar_e_confirmar(dir_staging: str) -> bool:
    """
    Mostra os dados que serão carregados e pede confirmação do usuário
    
    Args:
        dir_staging: Caminho da pasta staging
        
    Returns:
        True se usuário confirmou, False se cancelou
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("🔍 VALIDAÇÃO: Revise os dados antes de atualizar o banco")
    logger.info("=" * 70)
    
    arquivo_upload = os.path.join(dir_staging, "despesas_upload.csv")
    arquivo_update_termos = os.path.join(dir_staging, "update_termos.csv")
    arquivo_update_rubs = os.path.join(dir_staging, "update_rubricas.csv")
    
    tem_dados = False
    
    # ===== MOSTRA DESPESAS PARA UPLOAD =====
    if os.path.exists(arquivo_upload):
        tem_dados = True
        df = pd.read_csv(arquivo_upload, dtype=str)
        
        inserts = len(df[df['acao'] == 'INSERT']) if 'acao' in df.columns else 0
        updates = len(df[df['acao'] == 'UPDATE']) if 'acao' in df.columns else 0
        
        logger.info("")
        logger.info("📊 DESPESAS A ATUALIZAR:")
        logger.info(f"   • INSERT (novos): {inserts} registros")
        logger.info(f"   • UPDATE (alterados): {updates} registros")
        logger.info(f"   • Total: {len(df)} registros")
        
        # Mostra primeiras 3 linhas
        if len(df) > 0:
            logger.info("")
            logger.info("   Amostra (primeiros registros):")
            for idx, row in df.head(3).iterrows():
                acao = row.get('acao', 'INSERT')
                logger.info(f"      [{acao}] ID:{row['id_codigo_sit']} | Termo:{row['termo']} | Valor:R${float(row['valor']):.2f}")
            
            if len(df) > 3:
                logger.info(f"      ... +{len(df)-3} registros")
    
    # ===== MOSTRA TERMOS PARA UPDATE =====
    if os.path.exists(arquivo_update_termos):
        tem_dados = True
        df = pd.read_csv(arquivo_update_termos)
        logger.info("")
        logger.info(f"💰 TERMOS A ATUALIZAR: {len(df)} registros")
        for idx, row in df.head(2).iterrows():
            logger.info(f"      SIT:{row['nro_sit']} | Rendimento:R${float(row['rendimento_financeiro_total_csv']):.2f}")
        if len(df) > 2:
            logger.info(f"      ... +{len(df)-2} registros")
    
    # ===== MOSTRA RUBRICAS PARA UPDATE =====
    if os.path.exists(arquivo_update_rubs):
        tem_dados = True
        df = pd.read_csv(arquivo_update_rubs)
        logger.info("")
        logger.info(f"📋 RUBRICAS A ATUALIZAR: {len(df)} registros")
        for idx, row in df.head(2).iterrows():
            logger.info(f"      {row['id_termo_rubrica']} | Estorno:R${float(row['valor_estornado']):.2f}")
        if len(df) > 2:
            logger.info(f"      ... +{len(df)-2} registros")
    
    if not tem_dados:
        logger.info("")
        logger.info("✅ Nenhum dado para atualizar (banco já está sincronizado)")
        return True
    
    # ===== PEDE CONFIRMAÇÃO =====
    logger.info("")
    logger.info("=" * 70)
    print("\n")
    resposta = input("❓ Os dados acima estão CORRETOS? Digite 'SIM' para continuar ou qualquer outra coisa para CANCELAR: ").strip().upper()
    print("\n")
    
    if resposta == "SIM":
        logger.info("✅ CONFIRMADO! Prosseguindo com a carga...")
        return True
    else:
        logger.warning("❌ CANCELADO! A carga foi interrompida por sua solicitação.")
        logger.info("💡 Revise os dados e execute novamente quando estiver pronto.")
        return False


def main():
    """Executa o pipeline ETL completo"""
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("🏁 PIPELINE ETL - GRANTS MANAGEMENT - INICIANDO")
    logger.info("=" * 70)
    
    try:
        # Valida configurações
        logger.info("🔍 Validando configurações...")
        Config.validate()
        logger.info("✅ Configurações OK")
        
        # ===== PRÉ-PROCESSAMENTO: Copia downloads para raw =====
        logger.info("")
        logger.info("📥 Sincronizando arquivos de Downloads...")
        qtd_copiados = copiar_downloads_para_raw(logger)
        if qtd_copiados > 0:
            logger.info(f"✅ {qtd_copiados} arquivo(s) movido(s) para data/raw")
        else:
            logger.info("ℹ️  Nenhum arquivo novo em Downloads")
        
        # ===== ETAPA 1: EXTRAÇÃO =====
        logger.info("")
        logger.info("=" * 70)
        logger.info("ETAPA 1/3: EXTRAÇÃO")
        logger.info("=" * 70)
        
        extractor = ExpensesExtractor()
        sucesso_extracao = extractor.run()
        
        if not sucesso_extracao:
            logger.warning("⚠️  Nenhum dado foi extraído. Verificar fonte de dados.")
            logger.info("🏁 PIPELINE FINALIZADO (sem dados para processar)")
            return
        
        # ===== ETAPA 2: TRANSFORMAÇÃO =====
        logger.info("")
        logger.info("=" * 70)
        logger.info("ETAPA 2/3: TRANSFORMAÇÃO E VALIDAÇÃO")
        logger.info("=" * 70)
        
        transformer = ExpensesTransformer()
        sucesso_transformacao = transformer.run()
        
        if not sucesso_transformacao:
            logger.warning("⚠️  Nenhuma transformação foi necessária (dados já sincronizados)")
            logger.info("✅ Banco já estava atualizado!")
            tempo_total = time.time() - start_time
            logger.info(f"⏱️  Tempo total: {tempo_total:.2f}s")
            return
        
        # ===== PAUSA PARA VALIDAÇÃO =====
        if not validar_e_confirmar(Config.DIR_STAGING):
            # Usuário cancelou
            tempo_total = time.time() - start_time
            logger.info(f"⏱️  Tempo até cancelamento: {tempo_total:.2f}s")
            return
        
        # ===== ETAPA 3: CARGA =====
        logger.info("")
        logger.info("=" * 70)
        logger.info("ETAPA 3/3: CARGA NO BANCO DE DADOS")
        logger.info("=" * 70)
        
        loader = ExpensesLoader()
        sucesso_carga = loader.run()
        
        if not sucesso_carga:
            logger.warning("⚠️  Nenhuma carga foi necessária")
        
        # ===== RESUMO FINAL =====
        tempo_total = time.time() - start_time
        logger.info("")
        logger.info("=" * 70)
        logger.info("✨ PIPELINE CONCLUÍDO COM SUCESSO")
        logger.info(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
        logger.info("=" * 70)
        
        return 0
    
    except ValueError as e:
        logger.critical(f"❌ ERRO DE CONFIGURAÇÃO: {e}")
        logger.info("💡 Verifique se o arquivo .env está correto")
        return 1
    
    except KeyboardInterrupt:
        logger.warning("⚠️  INTERRUPÇÃO DO USUÁRIO (Ctrl+C)")
        tempo_total = time.time() - start_time
        logger.info(f"⏱️  Tempo até interrupção: {tempo_total:.2f}s")
        return 1
    
    except Exception as e:
        logger.critical(f"💥 ERRO FATAL: {e}", exc_info=True)
        logger.info("📋 Verifique os logs para mais detalhes")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)