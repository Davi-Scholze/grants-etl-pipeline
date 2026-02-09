"""
🔧 MÓDULO AUXILIAR DE INGESTÃO
Funções sanitizadoras e de processamento de dados
"""

import pandas as pd
import shutil
import os
from pathlib import Path
from src.utils.config import Config


def copiar_downloads_para_raw(logger=None, deletar_original=True) -> int:
    """
    Copia arquivos XLSX e CSV da pasta Downloads do Windows para a pasta raw
    Útil para automatizar o fluxo de entrada de dados
    
    Args:
        logger: Logger opcional para registrar operações
        deletar_original: Se True, deleta arquivo original após copiar (padrão: True)
        
    Returns:
        Número de arquivos copiados
    """
    # Pasta de downloads padrão do Windows
    downloads_win = Path.home() / "Downloads"
    pasta_raw = Path(Config.DIR_STAGING).parent / "raw"  # data/raw
    
    # Cria a pasta raw se não existir
    pasta_raw.mkdir(parents=True, exist_ok=True)
    
    # Extensões de interesse
    extensoes = [".xlsx", ".xls", ".csv"]
    
    arquivos_copiados = 0
    
    if not downloads_win.exists():
        if logger:
            logger.warning(f"⚠️  Pasta Downloads não encontrada: {downloads_win}")
        return 0
    
    try:
        for extensao in extensoes:
            # Busca arquivos com a extensão
            for arquivo in downloads_win.glob(f"*{extensao}"):
                try:
                    # Copia para pasta raw
                    destino = pasta_raw / arquivo.name
                    shutil.copy2(arquivo, destino)
                    
                    # Deleta original se parametrizado
                    if deletar_original:
                        try:
                            arquivo.unlink()  # Remove arquivo original
                            if logger:
                                logger.info(f"   📥 Movido: {arquivo.name} (original deletado)")
                        except Exception as e:
                            if logger:
                                logger.warning(f"   ⚠️  Erro ao deletar {arquivo.name}: {e}")
                    else:
                        if logger:
                            logger.info(f"   📥 Copiado: {arquivo.name}")
                    
                    arquivos_copiados += 1
                
                except Exception as e:
                    if logger:
                        logger.warning(f"   ⚠️  Erro ao processar {arquivo.name}: {e}")
    
    except Exception as e:
        if logger:
            logger.error(f"❌ Erro ao acessar Downloads: {e}")
    
    return arquivos_copiados


def classificar_tipo_despesa(texto: str) -> str:
    """
    Classifica despesa baseada em mapeamento predefinido
    
    Args:
        texto: Descrição da despesa
        
    Returns:
        Tipo classificado ou "OUTROS"
    """
    texto = str(texto).upper()
    for chave, valor in Config.MAPEAMENTO_DESPESAS.items():
        if chave in texto:
            return valor
    return "OUTROS"


def limpar_cpf_cnpj(valor) -> str:
    """
    Remove formatação de CPF/CNPJ, mantém apenas dígitos
    
    Args:
        valor: String com CPF ou CNPJ
        
    Returns:
        String com dígitos apenas
    """
    valor = "".join(filter(str.isdigit, str(valor)))
    if len(valor) <= 11:
        return valor.zfill(11)  # CPF
    return valor.zfill(14)  # CNPJ


def extrair_rubrica(tipo_despesa: str) -> str:
    """
    Extrai código da rubrica (6 dígitos) do tipo de despesa
    
    Args:
        tipo_despesa: Descrição que contém a rubrica
        
    Returns:
        Código da rubrica ou string vazia
    """
    import re
    match = re.search(r'^([\d\.]+)', str(tipo_despesa))
    return match.group(1).strip() if match else ""


def limpar_string_numero(value) -> str:
    """
    Remove .0 de números convertidos para string
    Exemplo: "67303.0" -> "67303"
    """
    try:
        return str(int(float(value)))
    except:
        return str(value).strip()


def parse_brl(valor) -> float:
    """
    Converte valores em formato BRL para float
    Exemplo: "R$ 1.200,50" -> 1200.50
    """
    if not valor or str(valor).strip() in ("-", "", "R$ 0,00"):
        return 0.0
    
    val_str = str(valor).replace("R$", "").strip()
    try:
        return float(val_str.replace(".", "").replace(",", ".") or 0)
    except:
        return 0.0


def validar_dataframe(df: pd.DataFrame, colunas_requeridas: list) -> bool:
    """
    Valida se um DataFrame possui todas as colunas requeridas
    
    Args:
        df: DataFrame a validar
        colunas_requeridas: Lista de colunas esperadas
        
    Returns:
        True se válido, False caso contrário
    """
    faltantes = set(colunas_requeridas) - set(df.columns)
    if faltantes:
        raise ValueError(f"❌ Colunas faltantes no DataFrame: {faltantes}")
    return True


def converter_data_para_sql(data_str: str) -> str:
    """
    Garante que data está em formato SQL (YYYY-MM-DD)
    
    Args:
        data_str: Data em string
        
    Returns:
        Data formatada para SQL ou None
    """
    if not data_str or str(data_str).strip() == "":
        return None
    
    try:
        data = pd.to_datetime(data_str, dayfirst=True)
        return data.strftime("%Y-%m-%d")
    except:
        return None
