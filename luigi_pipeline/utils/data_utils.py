# utils/data_utils.py

import logging
import json

def transform_data_to_relational_tables(data):
    """
    Transforms data into relational table records.
    
    Args:
        data (dict): The JSON data to transform.
    
    Returns:
        tuple: Three lists containing company, contact, and deal records respectively.
    """
    company_records = []
    contact_records = []
    deal_records = []

    for entry in data:
        # Company record
        company_record = {
            'company_name_new': entry.get('company_name_new', ''),
            'company_name_old': entry.get('company_name_old', ''),
            'company_cnpj': entry.get('company_cnpj', ''),
            'company_city': entry.get('company_city', ''),
            'company_state': entry.get('company_state', ''),
            'company_country': entry.get('company_country', ''),
            'company_region': entry.get('company_region', ''),
            'company_address': entry.get('company_address', ''),
            'company_telephone': entry.get('company_telephone', ''),
            'company_website': entry.get('company_website', ''),
            'company_main_sector': entry.get('company_main_sector', ''),
            'company_sub_sector': entry.get('company_sub_sector', '')
        }
        company_records.append(company_record)

        # Contact record
        contact_record = {
            'contact_name_new': entry.get('contact_name_new', ''),
            'contact_name_old': entry.get('contact_name_old', ''),
            'contact_phone': entry.get('contact_phone', ''),
            'contact_email': entry.get('contact_email', ''),
            'contact_position': entry.get('contact_position', ''),
            'contact_area': entry.get('contact_area', ''),
            'company_name_new': entry.get('company_name_new', '')
        }
        contact_records.append(contact_record)

        # Deal record
        deal_record = {
            'page_id': entry.get('page_id', ''),
            'source_db': entry.get('source_db', ''),
            'contact_name_new': entry.get('contact_name_new', ''),
            'company_name_new': entry.get('company_name_new', ''),
            'creation_date': entry.get('creation_date', ''),
            'last_edit_time': entry.get('last_edit_time', ''),
            'status': entry.get('status', ''),
            'campaign': json.dumps(entry.get('campaign', [])),
            'package': json.dumps(entry.get('package', [])),
            'products_ad': json.dumps(entry.get('products_ad', [])),
            'products_ex': json.dumps(entry.get('products_ex', [])),
            'services': json.dumps(entry.get('services', [])),
            'contract_value': entry.get('contract_value', ''),
            'duration_months': entry.get('duration_months', ''),
            'renewal_date': entry.get('renewal_date', '')
        }
        deal_records.append(deal_record)

    return company_records, contact_records, deal_records
