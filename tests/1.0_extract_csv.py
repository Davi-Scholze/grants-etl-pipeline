
# Block 01: The tools we need (imports) tha python go use

import os # Operating system interfaces
import pandas as pd # Data manipulation library
import logging # Set up logging
from dotenv import load_dotenv # Load environment variables from .env file


# Block 02: PATHS CONFIGURATIONS ----------------- Enviroment configuration for relative paths and sensitive data (Absolute paths should be avoided)

    # "load_dotenv()": Reads a file called ".env" that is in the same path as this script
    # "os.getenv(...)": Take the value that is inside the .env file
    # "if not ...": If it isn´t found, the script stops and shows an error message
    # "logging.basicConfig": Set up how the mensages will be shown in the console

# ===================== Configurations =========================

# Load the .env variables
load_dotenv()

# Take de path of the configurated folders
DIR_DOWNLOADS = os.getenv("DIR_DOWNLOADS")
DIR_STAGING = os.getenv("DIR_STAGING")

# Security lock: If the folders does not exist in the .env file, stop everything
if not DIR_DOWNLOADS or not DIR_STAGING:
    raise ValueError("ERRO: Variáveis DIR_DOWNLOADS ou DIR_STAGING não estão definidas no arquivo .env")

# Define os caminhos finais
ORIGEM_PASTA = DIR_DOWNLOADS
ARQUIVO_SAIDA = os.path.join(DIR_STAGING, "despesas_geral.csv") #"os.path.join": Junta as partes do caminho de forma correta



