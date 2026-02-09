"""
📥 WRAPPER DA ETAPA DE EXTRAÇÃO
Encapsula os scripts 1_extract_csv.py e 1_extract_resumo.py
"""

import os
import pandas as pd
import logging
from pathlib import Path
from dotenv import load_dotenv

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.utils.ingestor import (
    classificar_tipo_despesa, limpar_cpf_cnpj, extrair_rubrica, 
    parse_brl, limpar_string_numero
)

logger = setup_logger("ExpensesExtractor")
load_dotenv()


class ExpensesExtractor:
    """Extrator centralizado para todas as fontes de dados"""
    
    def __init__(self):
        self.dir_downloads = Config.DIR_DOWNLOADS
        self.dir_staging = Config.DIR_STAGING
        self.sit_termo_map = Config.SIT_TERMO_MAP
        
        # Validar diretórios
        if not self.dir_downloads or not self.dir_staging:
            raise ValueError("❌ DIR_DOWNLOADS ou DIR_STAGING não definidos")
        
        # Criar staging se não existir
        Path(self.dir_staging).mkdir(parents=True, exist_ok=True)
    
    def run(self) -> bool:
        """
        Executa todas as extrações
        
        Returns:
            True se sucesso, False caso contrário
        """
        logger.info("=" * 60)
        logger.info("🏁 INICIANDO ETAPA 1: EXTRAÇÃO DE DADOS")
        logger.info("=" * 60)
        
        try:
            # Extrai CSVs de despesas
            sucesso_csv = self.extrair_despesas_csv()
            
            # Extrai resumos financeiros
            sucesso_resumo = self.extrair_resumos()
            
            if sucesso_csv or sucesso_resumo:
                logger.info("✅ EXTRAÇÃO CONCLUÍDA COM SUCESSO")
                return True
            else:
                logger.warning("⚠️  Nenhum dado foi extraído")
                return False
                
        except Exception as e:
            logger.error(f"💥 ERRO NA EXTRAÇÃO: {e}", exc_info=True)
            return False
    
    def extrair_despesas_csv(self) -> bool:
        """
        Extrai e processa despesas de arquivos XLSX
        Baseado em 1_extract_csv.py
        """
        logger.info("➡️ Etapa 1a: Consolidação de Arquivos de Despesas")
        
        dados_consolidados = []
        arquivos_processados = 0
        
        for sit, termo in self.sit_termo_map.items():
            caminho_arquivo = os.path.join(self.dir_downloads, f"Despesas_SIT_{sit}.xlsx")
            
            if not os.path.exists(caminho_arquivo):
                logger.warning(f"   ⚠️  Arquivo não encontrado: {caminho_arquivo}")
                continue
            
            try:
                # Lê tudo como string para evitar problemas de tipagem
                df = pd.read_excel(caminho_arquivo, dtype=str)
                
                if "Data do Pagamento" not in df.columns:
                    logger.warning(f"   ⚠️  Coluna 'Data do Pagamento' ausente em {caminho_arquivo}")
                    continue
                
                # Tratamento de datas
                df["data_pagto_temp"] = pd.to_datetime(
                    df["Data do Pagamento"], dayfirst=True, errors="coerce"
                )
                
                # Verifica coluna de débito (com variações de grafia)
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
                
                # Remove linhas sem data de pagamento
                df = df.dropna(subset=["data_pagto_temp"])
                
                # Constrói DataFrame limpo
                novo = pd.DataFrame(index=df.index)
                
                novo["id_codigo_sit"] = df.get("Código", "").fillna("").str.strip()
                novo["termo"] = termo
                novo["rubrica"] = df["Tipo de Despesa"].apply(extrair_rubrica)
                novo["tipo_despesa"] = df["Tipo de Despesa"].apply(classificar_tipo_despesa)
                novo["cpf_cnpj"] = df["CPF/CNPJ"].apply(limpar_cpf_cnpj)
                novo["favorecido"] = df["Favorecido"].fillna("").str.strip()
                novo["tipo_doc_despesa"] = df.get("Tipo Documento Despesa", "").fillna("").str.strip()
                novo["descricao_despesa"] = df["Descrição da Despesa"].fillna("").str.strip()
                novo["tipo_doc_pagamento"] = df.get("Tipo Documento Pagamento", "").fillna("").str.strip()
                novo["data_pagamento"] = df["data_pagto_temp"].dt.strftime("%Y-%m-%d")
                novo["data_debito_convenio"] = df["data_debito_temp"].dt.strftime("%Y-%m-%d")
                novo["valor"] = pd.to_numeric(df.get("Valor", 0), errors="coerce").fillna(0.0)
                novo["id_termo_rubrica"] = novo["termo"] + "-" + novo["rubrica"]
                
                # Remove linhas com rubrica ou termo vazio
                novo = novo[novo["rubrica"].str.len() > 0]
                novo = novo[novo["termo"].str.len() > 0]
                
                dados_consolidados.append(novo)
                arquivos_processados += 1
                logger.info(f"   ✅ SIT {sit} (arquivo {sit}.xlsx) - {len(novo)} linhas extraídas")
                
            except Exception as e:
                logger.error(f"   ❌ Erro ao processar {sit}.xlsx: {e}")
                continue
        
        # Salva consolida
        if dados_consolidados:
            df_final = pd.concat(dados_consolidados, ignore_index=True)
            
            # Reordena colunas
            colunas_ordem = Config.COLUNAS_DESPESAS
            df_final = df_final[colunas_ordem]
            
            saida = os.path.join(self.dir_staging, "despesas_geral.csv")
            df_final.to_csv(saida, index=False, encoding="utf-8")
            
            logger.info(f"   💾 Total: {len(df_final)} registros salvos em despesas_geral.csv")
            return True
        else:
            logger.warning("   ⚠️  Nenhum arquivo de despesas foi processado")
            return False
    
    def extrair_resumos(self) -> bool:
        """
        Extrai resumos financeiros (termos e rubricas)
        Baseado em 1_extract_resumo.py
        """
        logger.info("➡️ Etapa 1b: Extração de Resumos Financeiros")
        
        pasta_downloads = Path(self.dir_downloads)
        lista_termos = []
        lista_rubricas = []
        
        # Busca arquivos CSV
        arquivos = list(pasta_downloads.glob("*.csv"))
        
        if not arquivos:
            logger.warning("   ⚠️  Nenhum arquivo CSV encontrado em downloads")
            return False
        
        count_processados = 0
        
        for caminho in arquivos:
            try:
                linhas = self._ler_arquivo_csv(caminho)
                if not linhas:
                    continue
                
                sit, rendimento, rubricas = self._extrair_dados_csv(linhas)
                
                if sit:
                    lista_termos.append({
                        "nro_sit": sit,
                        "rendimento_financeiro_total": rendimento
                    })
                    
                    for rub in rubricas:
                        lista_rubricas.append({
                            "nro_sit": sit,
                            "rubrica": rub["rubrica"],
                            "valor_estornado": rub["valor_estornado"],
                            "id_termo_rubrica": f"{sit}-{rub['rubrica']}"
                        })
                    
                    count_processados += 1
                    logger.info(f"   ✅ SIT {sit} de '{caminho.name}'")
                    
            except Exception as e:
                logger.error(f"   ❌ Erro ao processar {caminho.name}: {e}")
                continue
        
        # Salva resumos
        sucesso = False
        
        if lista_termos:
            df_termos = pd.DataFrame(lista_termos)
            saida_termos = os.path.join(self.dir_staging, "resumo_termos.csv")
            df_termos.to_csv(saida_termos, index=False, encoding="utf-8")
            logger.info(f"   💾 Resumo termos: {len(df_termos)} registros salvos")
            sucesso = True
        
        if lista_rubricas:
            df_rubs = pd.DataFrame(lista_rubricas)
            saida_rubs = os.path.join(self.dir_staging, "resumo_rubricas.csv")
            df_rubs.to_csv(saida_rubs, index=False, encoding="utf-8")
            logger.info(f"   💾 Resumo rubricas: {len(df_rubs)} registros salvos")
            sucesso = True
        
        return sucesso
    
    @staticmethod
    def _ler_arquivo_csv(caminho: Path) -> list:
        """Lê arquivo CSV com tratamento de encoding"""
        try:
            with open(caminho, "r", encoding="latin-1") as f:
                return f.readlines()
        except UnicodeDecodeError:
            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    return f.readlines()
            except:
                return []
    
    @staticmethod
    def _extrair_dados_csv(linhas: list):
        """Extrai SIT, rendimento e rubricas de arquivo CSV"""
        nro_sit = None
        rendimento_total = 0.0
        rubricas = []
        
        dentro_rendimentos = False
        dentro_despesas = False
        
        for linha in linhas:
            linha = linha.strip()
            partes = linha.split(";")
            
            # Identifica SIT
            if linha.startswith("Nº SIT"):
                if len(partes) > 1:
                    nro_sit = partes[1].strip()
                continue
            
            if not nro_sit:
                continue
            
            # Identifica rendimentos
            if "Detalhes dos Rendimentos de Aplicações Financeiras" in linha:
                dentro_rendimentos = True
                continue
            
            if dentro_rendimentos and linha.startswith("T O T A L"):
                if len(partes) > 1:
                    rendimento_total = parse_brl(partes[1])
                dentro_rendimentos = False
            
            # Identifica despesas
            if "Detalhe das Despesas" in linha:
                dentro_despesas = True
                continue
            
            if dentro_despesas:
                if not linha or "T O T A L" in linha or linha.startswith("Despesa;"):
                    continue
                
                if len(partes) >= 5:
                    cod_full = partes[0].strip()
                    valor_est = parse_brl(partes[4])
                    cod_rubrica = cod_full.split("-")[0].strip()
                    
                    if cod_rubrica:
                        rubricas.append({
                            "rubrica": cod_rubrica,
                            "valor_estornado": valor_est
                        })
        
        return nro_sit, rendimento_total, rubricas
