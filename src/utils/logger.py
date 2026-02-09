"""
📋 MÓDULO DE LOGGING CENTRALIZADO
Fornece um logger consistente para todo o pipeline ETL
"""

import logging
import os
from pathlib import Path
from datetime import datetime


def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Configura um logger com formatação padrão e salva em arquivo.
    
    Args:
        name: Nome do logger (ex: "MainPipeline", "ExpensesExtractor")
        log_dir: Diretório para salvar logs
    
    Returns:
        Logger configurado
    """
    # Cria diretório de logs se não existir
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Timestamp para arquivo de log
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{name}_{timestamp}.log")
    
    # Formata mensagens com cores (no console)
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_formatter = logging.Formatter(log_format)
    
    # Logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Handler de arquivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)
    
    # Handler de console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)
    
    # Evita handlers duplicados
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
