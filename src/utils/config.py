"""
⚙️ MÓDULO DE CONFIGURAÇÃO CENTRALIZADA
Carrega variáveis de ambiente e oferece constantes do projeto
"""

import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()


class Config:
    """Classe para acessar configurações do projeto"""
    
    # 🔹 DIRETÓRIOS
    DIR_DOWNLOADS = os.getenv("DIR_DOWNLOADS")
    DIR_STAGING = os.getenv("DIR_STAGING")
    DIR_LOGS = os.getenv("DIR_LOGS", "logs")
    
    # 🔹 BANCO DE DADOS
    CONN_STR_SQLSERVER = os.getenv("CONN_STR_SQLSERVER")
    
    # 🔹 MAPEAMENTOS (Hardcoded para referência)
    SIT_TERMO_MAP = {
        "57884": "6373",
        "63377": "6729",
        "66270": "6822",
        "67303": "6893",
        "67669": "6932",
        "71199": "26478",
        "74699": "26672"
    }
    
    MAPEAMENTO_DESPESAS = {
        "PESSOAL CIVIL": "PESSOAL",
        "OBRIGAÇÕES PATRONAIS": "ENCARGOS",
        "MATERIAIS DE CONSUMO": "MATERIAIS DE CONSUMO",
        "SERVIÇOS DE TERCEIROS": "SERVIÇOS DE TERCEIROS"
    }
    
    COLUNAS_DESPESAS = [
        "id_codigo_sit", "termo", "rubrica", "tipo_despesa", "cpf_cnpj", "favorecido",
        "tipo_doc_despesa", "descricao_despesa", "tipo_doc_pagamento",
        "data_pagamento", "data_debito_convenio", "valor", "id_termo_rubrica"
    ]
    
    @staticmethod
    def validate():
        """Valida se as variáveis críticas estão definidas"""
        required = ["DIR_DOWNLOADS", "DIR_STAGING", "CONN_STR_SQLSERVER"]
        missing = [var for var in required if not getattr(Config, var)]
        
        if missing:
            raise ValueError(f"❌ Variáveis de ambiente faltando: {', '.join(missing)}")
        
        return True
