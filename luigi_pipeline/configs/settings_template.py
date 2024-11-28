# configs/settings.py

import os

# Notion API settings
NOTION_API_KEY = '---'  # Replace with your Notion API key
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
    '--': 'all_deals',
    '--': 'hunter_leads',
    '--': 'db_experience',
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
