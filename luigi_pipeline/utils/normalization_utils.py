# utils/normalization_utils.py

import re
import structlog

# List of unwanted phrases to remove
UNWANTED_PHRASES = [
    "Téléphone : Non disponible",
    "Téléphone :\tNon disponible",
    "Email : Non disponible",
    "Email :\tNon disponible",
    # Add other phrases as needed
]

# Partial replacements dictionary
partial_replacements_dict = {
    r'\s+S\.A\.?$': '',
    r'\s+SA$': '',
    r'\s+Ltda\.?$': '',
    r'\s+Ltd\.?$': '',
    # Add other patterns as needed
}

def normalize_entry(entry, logger):
    """
    Normalizes a data entry by performing the following actions:
    - Trims the value.
    - Removes line breaks and tabulations from all text fields.
    - Removes specific unwanted phrases from all text fields.
    - Applies partial string replacements.
    - Applies specific normalizations based on the field.

    Args:
        entry (dict): Dictionary representing a data entry.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        dict: Normalized data entry.
    """
    for key, value in entry.items():
        if isinstance(value, str):
            original_value = value
            value = value.strip()
            # Remove line breaks and tabulations
            value = remove_line_breaks(value, logger)

            # Remove unwanted phrases
            value = remove_unwanted_phrases(value, logger)

            # Apply partial string replacements
            value = replace_substrings(value, partial_replacements_dict, logger)

            # Apply specific normalizations if necessary
            value = normalize_specific_fields(key, value, logger)

            entry[key] = value
            if original_value != value:
                logger.debug(event="field_normalized", message=f"Field '{key}' normalized from '{original_value}' to '{value}'.")
    return entry

def remove_unwanted_phrases(text, logger):
    """
    Removes all unwanted phrases from a string.

    Args:
        text (str): Text to be cleaned.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        str: Text without unwanted phrases.
    """
    for phrase in UNWANTED_PHRASES:
        if phrase in text:
            text = text.replace(phrase, '')
            logger.debug(event="remove_phrase", message=f"Removed phrase '{phrase}' from text.")
    return text.strip()

def replace_substrings(text, replacements_dict, logger):
    """
    Replaces substrings in the text based on the provided replacements dictionary.

    Args:
        text (str): The text to perform replacements on.
        replacements_dict (dict): A dictionary where keys are regex patterns to search for,
                                  and values are the replacement strings.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        str: The text with replacements applied.
    """
    original_text = text
    for pattern, replacement in replacements_dict.items():
        new_text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        if new_text != text:
            logger.debug(event="substring_replaced", message=f"Replaced pattern '{pattern}' with '{replacement}' in text '{original_text}'.")
        text = new_text
    if original_text != text:
        logger.debug(event="text_modified", message=f"Text modified from '{original_text}' to '{text}'.")
    return text.strip()

def normalize_specific_fields(key, value, logger):
    """
    Applies specific normalizations based on the field.

    Args:
        key (str): Field name.
        value (str): Field value.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        str: Normalized field value.
    """
    if key in ['company_name_new', 'contact_name_new']:
        return normalize_name(value, logger)
    # Add other specific normalizations if necessary
    return value

def normalize_name(name, logger):
    """
    Normalizes a name by removing unwanted characters, line breaks, suffixes, and standardizing to uppercase.

    Args:
        name (str): Name to be normalized.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        str: Normalized name.
    """
    try:
        if isinstance(name, str):
            original_name = name
            # Remove line breaks and tabulations
            name = re.sub(r'[\n\t]+', ' ', name)
            name = name.strip()
            # Remove unwanted characters at the beginning
            name = re.sub(r'^[*•#]+', '', name)
            # Remove special characters
            unwanted_chars = r'[*()|•#\/.,;[\]?!@]'
            name = re.sub(unwanted_chars, ' ', name)
            # Remove content within parentheses
            name = re.sub(r'\(.*?\)', '', name)
            # Remove unwanted suffixes at the end
            unwanted_suffixes = [
                r'\s+Ltda\.?$', r'\s+S\.A\.?$', r'\s+SA$', r'\s+S\.A$', r'\s+Ltd\.?$'
                # Add other patterns as needed
            ]
            for suffix in unwanted_suffixes:
                new_name = re.sub(suffix, '', name, flags=re.IGNORECASE)
                if new_name != name:
                    logger.debug(event="suffix_removed", message=f"Removed suffix '{suffix}' from name '{name}'.")
                name = new_name
            # Remove multiple spaces
            name = re.sub(r'\s+', ' ', name)
            name = name.strip().upper()
            logger.debug(event="name_normalized", message=f"Name normalized from '{original_name}' to '{name}'.")
            return name
    except Exception as e:
        logger.error(event="name_normalization_error", message=f'Error normalizing name "{name}": {str(e)}', exception=str(e))
    return name

def remove_line_breaks(text, logger):
    """
    Removes line breaks and tabulations from a string.

    Args:
        text (str): Text to be cleaned.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        str: Cleaned text without line breaks and tabulations.
    """
    if not isinstance(text, str):
        if text is None:
            logger.warning(event="remove_line_breaks", message="Received None value for text. Returning empty string.")
        else:
            logger.warning(event="remove_line_breaks", message=f"Received non-string value for text: {text}. Returning empty string.")
        return ''
    cleaned_text = text.replace('\n', ' ').replace('\t', ' ').strip()
    if cleaned_text != text:
        logger.debug(event="remove_line_breaks", message="Removed line breaks and tabulations.")
    return cleaned_text

def apply_replacements_to_entry(entry, company_replacements, contact_replacements, region_replacements, logger):
    """
    Applies specific substitutions to data entries based on provided replacement dictionaries.

    Args:
        entry (dict): Data entry dictionary.
        company_replacements (dict): Mapping of old to new company names.
        contact_replacements (dict): Mapping of old to new contact details.
        region_replacements (dict): Mapping of old to new region details.
        logger (structlog.BoundLogger): Logger for logging events.

    Returns:
        dict: Data entry with replacements applied.
    """
    # Replace company names
    old_company_name = entry.get('company_name_old', '')
    new_company_name = company_replacements.get(old_company_name, old_company_name)
    entry['company_name_new'] = new_company_name
    if new_company_name != old_company_name:
        logger.debug(event="replace_company", message=f"Replaced company '{old_company_name}' with '{new_company_name}'.")

    # Replace contact names
    old_contact_name = entry.get('contact_name_old', '')
    key = (old_company_name, old_contact_name)
    contact_replacement = contact_replacements.get(key, {})
    if contact_replacement:
        entry['contact_name_new'] = contact_replacement.get('new_contact_name', old_contact_name)
        contact_phone = contact_replacement.get('contact_phone', entry.get('contact_phone', ''))
        contact_phone = remove_line_breaks(contact_phone, logger)
        if contact_phone.strip() in UNWANTED_PHRASES:
            contact_phone = ''
            logger.debug(event="replace_contact_phone", message="Contact phone is in unwanted phrases. Setting to empty string.")
        entry['contact_phone'] = contact_phone

        contact_email = contact_replacement.get('contact_email', entry.get('contact_email', ''))
        contact_email = remove_line_breaks(contact_email, logger)
        if contact_email.strip() in UNWANTED_PHRASES:
            contact_email = ''
            logger.debug(event="replace_contact_email", message="Contact email is in unwanted phrases. Setting to empty string.")
        entry['contact_email'] = contact_email
        logger.debug(event="replace_contact", message=f"Replaced contact '{old_contact_name}' with '{entry['contact_name_new']}'.")

    # Replace region information
    region_value = entry.get('company_state', '')
    region_replacement = region_replacements.get(region_value, {})
    if region_replacement:
        entry.update(region_replacement)
        logger.debug(event="replace_region", message=f"Replaced region '{region_value}' with '{region_replacement}'.")

    return entry
