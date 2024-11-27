# configs/column_mappings.py

COLUMN_MAPPINGS = {
    "all_deals": {
        "nome_empresa": "company_name_new",
        "CLIENT COMPANY": "company_name_old",
        "CPF/CNPJ": "company_cnpj",
        "MAIN SECTOR": "company_main_sector",
        "SECTOR": "company_sub_sector",
        "REGION": "company_state",
        "LOCATION": "company_country",
        "WEBSITE": "company_website",
        "CONTACT NAME_CHAMPION": "contact_name_new",
        "TELEPHONE_CHAMPION": "contact_phone",
        "EMAIL_CHAMPION": "contact_email",
        "POSITION": "contact_position",
        "f.PACKAGE": "package",
        "UPSELL_PACKAGE": "upsell_package",
        "UPSELL_AGRIPRODUCT": "upsell_agriproducts",
        "AGRI_PRODUCTS": "products_ad",
        "CAMPAIGN OUTBOUND": "campaign",
        "UPSELL_MARKETSSERVICES": "upsell_services",
        "MARKETS_SERVICES": "services",
        "MTHS": "duration_months",
        "RENEWAL": "renewal_date",
        "R$ TOTAL": "contract_value",
        "STATUS_MARKETS": "status",
        "CARGO": "contact_position",
        # Removed duplicate key with trailing space
    },
    "hunter_leads": {
        "CLIENT": "company_name_old",
        "nome_empresa": "company_name_new",
        "CNPJ-CPF": "company_cnpj",
        "SECTOR": "company_sub_sector",
        "REGION": "company_state",
        "LOCATION": "company_country",
        "CIDADE": "company_city",
        "WEBSITE": "company_website",
        "CONTACT NAME": "contact_name_new",
        "TELEPHONE": "contact_phone",
        "EMAIL": "contact_email",
        "POSITION": "contact_position",
        "PACKAGE": "package",
        "CAMPAIGN OUTBOUND": "campaign",
        "AGRI_PRODUCTS": "products_ad",
        "STATUS_MARKETS": "status",
        "CARGO": "contact_position",
        # Removed duplicate key with trailing space
    },
    "db_experience": {
        "CLIENT": "company_name_old",
        "nome_empresa": "company_name_new",
        "SECTOR": "company_sub_sector",
        "CONTACT NAME": "contact_name_new",
        "TELEPHONE": "contact_phone",
        "EMAIL": "contact_email",
        "POSITION": "contact_position",
        "AGRI_PRODUCTS": "products_ex",
        "EVENT_NAME": "events_experience"
    }
}
