# configs/settings.py

import os

# Notion API settings
NOTION_API_KEY = 'secret_I1pdTPPLPxWKs5HKLWbxztmwRwTev65MkItRTNTL7F3'  # Replace with your Notion API key
NOTION_API_URL = 'https://api.notion.com/v1'

# Rate Limiting Settings
NOTION_RATE_LIMIT_CALLS = int(os.getenv('NOTION_RATE_LIMIT_CALLS', '3'))  # Changed default to 3 calls
NOTION_RATE_LIMIT_PERIOD = int(os.getenv('NOTION_RATE_LIMIT_PERIOD', '1'))  # 1 second

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
NORMALIZED_DATA_DIR = os.path.join(DATA_DIR, 'normalized')
REPLACED_DATA_DIR = os.path.join(DATA_DIR, 'replaced')
TRANSFORMED_DATA_DIR = os.path.join(DATA_DIR, 'transformed')
REPLACEMENTS_DIR = os.path.join(DATA_DIR, 'replacements')
EXCEL_FILES_DIR = os.path.join(DATA_DIR, 'excel_files')

# Adjusted relative path to replacement_lists.xlsx
REPLACEMENT_LISTS_EXCEL_PATH = os.path.join(EXCEL_FILES_DIR, 'replacement_lists.xlsx')

LOGS_DIR = os.path.join(BASE_DIR, 'logs')
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
DATABASE_PATH = os.path.join(DATABASE_DIR, 'crm_database.db')
BACKUPS_DIR = os.path.join(DATABASE_DIR, 'backups')

# Selected databases
SELECTED_DATABASES = {
    #'3dbe9a740286479f90513cb93cdeb614': 'all_deals',
    '80e6f169a48c48459c0ff2b9f722947c': 'hunter_leads',
    '50def1fba524454f8546fd0692f2ea94': 'db_experience',
    '135789c14919805a94a8d38f1b575744' : 'hunter_leads_GAFFFF25',
    '10e10a1d3d154aa8a59d6f6753d3da40' : 'all_deals_GAFFFF25',
    '147789c1491980c09221ecd5920b750a' : 'clients deals markets'
}

# Time zone settings
TIME_ZONE = 'America/Sao_Paulo'

# Teste de leitura do arquivo replacement_lists.xlsx
import pandas as pd

try:
    df = pd.read_excel(REPLACEMENT_LISTS_EXCEL_PATH, sheet_name='company_replacement_list')
    print("Arquivo lido com sucesso.")
except FileNotFoundError:
    print("Arquivo não encontrado.")
