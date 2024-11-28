# Luigi ETL Pipeline Dataflow Document

---

## Introduction

This document provides a comprehensive overview of the dataflow in the Luigi ETL pipeline, incorporating the code and planning as per the specified requirements. The pipeline is designed to extract data from selected Notion databases, transform and normalize the data according to defined rules, apply replacements using provided lists, and load the data into a SQLite relational database. The pipeline is modular, with each Luigi task responsible for a specific step in the ETL process. This document details each task, the data it processes, the files and directories involved, and how data moves through the pipeline.

---

## Pipeline Overview

The Luigi ETL pipeline consists of the following modular tasks:

1. **ExtractRawData**
2. **ExtractSelectedProperties**
3. **NormalizeData**
4. **ApplyReplacements**
5. **TransformToRelationalTables**
6. **LoadToDatabase**
7. **BackupDatabase**
8. **LoggingSetup** (Utility)

Each task processes data and produces outputs that serve as inputs to subsequent tasks. The pipeline ensures data integrity, handles incremental updates based on `last_edit_time`, and maintains logs and backups.

---

## Dataflow Diagram

Below is a high-level representation of the dataflow through the pipeline tasks:

```
+-------------------+
|   ExtractRawData  |
|    (Task 1)       |
+---------+---------+
          |
          v
+---------------------------+
| ExtractSelectedProperties |
|         (Task 2)          |
+-----------+---------------+
            |
            v
+-------------------+
|   NormalizeData   |
|     (Task 3)      |
+---------+---------+
          |
          v
+--------------------+
|  ApplyReplacements |
|      (Task 4)      |
+---------+----------+
          |
          v
+-------------------------------+
| TransformToRelationalTables   |
|          (Task 5)             |
+------------+------------------+
             |
             v
+-------------------+
|  LoadToDatabase   |
|     (Task 6)      |
+---------+---------+
          |
          v
+-------------------+
|  BackupDatabase   |
|     (Task 7)      |
+-------------------+
```

---

## Detailed Task Descriptions

### Task 1: ExtractRawData

- **Objective**: Fetch raw JSON data from selected Notion databases.
- **Inputs**:
  - Notion API credentials (`NOTION_API_KEY` from `configs/settings.py`).
  - List of selected Notion databases (`SELECTED_DATABASES` from `configs/settings.py`).
- **Outputs**:
  - Raw JSON data files stored in `data/raw/` directory.
    - Files: `db_all_deals_data.json`, `db_hunter_leads_data.json`, `db_db_experience_data.json`.

#### Process Description

1. **Initialization**:
   - Instantiate the `NotionClient` class from `utils/notion_client.py`.
   - Ensure the `data/raw/` directory exists.

2. **Data Fetching**:
   - Iterate over each database ID and name in `SELECTED_DATABASES`.
   - Use the Notion API to query each database.
     - Handle pagination and rate limiting.
     - Fetch all pages (`results`) from each database.

3. **Data Storage**:
   - Save the fetched data to JSON files in `data/raw/`.
   - Log the completion of data extraction for each database.

### Task 2: ExtractSelectedProperties

- **Objective**: Extract selected properties from the raw data using column mappings.
- **Inputs**:
  - Raw JSON data files from Task 1 (`data/raw/`).
  - Column mappings (`COLUMN_MAPPINGS` from `configs/column_mappings.py`).
- **Outputs**:
  - Processed JSON files with selected properties in `data/processed/`.
    - Files: `processed_all_deals_data.json`, `processed_hunter_leads_data.json`, `processed_db_experience_data.json`.

#### Process Description

1. **Initialization**:
   - Ensure the `data/processed/` directory exists.

2. **Data Processing**:
   - For each raw data file:
     - Load the JSON data.
     - Identify the database name and corresponding column mappings.
     - Iterate over each entry (`page`) in the data.
       - Extract `page_id`, `last_edit_time`, and `source_db`.
       - For each property in the column mappings:
         - Extract the property based on its type (e.g., `title`, `rich_text`, `select`, `multi_select`).
         - Store the extracted value with the mapped key.
     - Collect all processed entries.

3. **Data Storage**:
   - Save the processed data to JSON files in `data/processed/`.
   - Log the completion of property extraction for each database.

### Task 3: NormalizeData

- **Objective**: Normalize selected properties using defined normalization rules.
- **Inputs**:
  - Processed JSON files from Task 2 (`data/processed/`).
  - Normalization rules (`utils/normalization_utils.py`).
- **Outputs**:
  - Normalized data files in `data/normalized/`.
    - Files: `normalized_all_deals_data.json`, `normalized_hunter_leads_data.json`, `normalized_db_experience_data.json`.

#### Process Description

1. **Initialization**:
   - Ensure the `data/normalized/` directory exists.

2. **Data Normalization**:
   - For each processed data file:
     - Load the JSON data.
     - Iterate over each entry.
       - Normalize company names using `normalize_name` function.
       - Normalize contact names similarly.
       - Apply other normalization rules as needed.
     - Collect all normalized entries.

3. **Data Storage**:
   - Save the normalized data to JSON files in `data/normalized/`.
   - Log the completion of data normalization for each database.

### Task 4: ApplyReplacements

- **Objective**: Apply replacements to selected properties using replacement lists.
- **Inputs**:
  - Normalized data files from Task 3 (`data/normalized/`).
  - Replacement lists extracted from `replacement_lists.xlsx` (stored in `data/replacements/`).
    - `company_replacement_list.csv`
    - `contact_replacement_list.csv`
    - `region_replacement_list.csv`
- **Outputs**:
  - Data files with replacements applied in `data/replaced/`.
    - Files: `replaced_all_deals_data.json`, `replaced_hunter_leads_data.json`, `replaced_db_experience_data.json`.

#### Process Description

1. **Initialization**:
   - Ensure the `data/replaced/` and `data/replacements/` directories exist.

2. **Load Replacement Lists**:
   - Read `replacement_lists.xlsx` and extract the sheets into CSV files.
   - Convert the CSV files into dictionaries for quick lookup.

3. **Apply Replacements**:
   - For each normalized data file:
     - Load the JSON data.
     - Iterate over each entry.
       - Replace company names using `company_replacements_dict`.
       - Replace contact names using `contact_replacements_dict`.
       - Replace region information using `region_replacements_dict`.
     - Collect all entries with replacements applied.

4. **Data Storage**:
   - Save the replaced data to JSON files in `data/replaced/`.
   - Log the completion of applying replacements for each database.

### Task 5: TransformToRelationalTables

- **Objective**: Transform the data into relational tables matching the SQL desired schema.
- **Inputs**:
  - Data files from Task 4 (`data/replaced/`).
- **Outputs**:
  - CSV files for `company_table`, `contact_table`, and `deals_table` in `data/transformed/`.

#### Process Description

1. **Initialization**:
   - Ensure the `data/transformed/` directory exists.

2. **Data Transformation**:
   - Initialize empty lists for company, contact, and deal records.
   - For each replaced data file:
     - Load the JSON data.
     - Iterate over each entry.
       - Extract fields relevant to the company and create company records.
       - Extract fields relevant to contacts and create contact records.
       - Extract fields relevant to deals and create deal records.
       - Handle multi-select fields by converting them to JSON strings.
     - Accumulate all records.

3. **Data Deduplication**:
   - Convert the lists of records into DataFrames.
   - Remove duplicates based on relevant keys (e.g., `company_name_new`, `contact_name_new`).

4. **Data Storage**:
   - Save the DataFrames as CSV files in `data/transformed/`:
     - `company_table.csv`
     - `contact_table.csv`
     - `deals_table.csv`
   - Log the completion of data transformation.

### Task 6: LoadToDatabase

- **Objective**: Load the transformed data into the SQLite database, performing incremental updates based on `last_edit_time`.
- **Inputs**:
  - CSV files from Task 5 (`data/transformed/`).
  - Existing SQLite database (if any) (`database/crm_database.db`).
- **Outputs**:
  - Updated SQLite database in `database/crm_database.db`.

#### Process Description

1. **Initialization**:
   - Ensure the `database/` directory exists.
   - Connect to the SQLite database (create it if it doesn't exist).

2. **Load Existing Data**:
   - If the tables exist, load the existing data into DataFrames.

3. **Data Merging**:
   - Read the CSV files into DataFrames.
   - For each table:
     - Merge the new data with existing data.
     - Compare `last_edit_time` to determine which records need updating.
     - Keep the latest version of each record based on `last_edit_time`.

4. **Data Loading**:
   - Write the updated DataFrames to the database, replacing the existing tables.
   - Close the database connection.
   - Log the completion of data loading.

### Task 7: BackupDatabase

- **Objective**: Perform daily backups of the SQLite database and maintain backups for the last month.
- **Inputs**:
  - The current SQLite database (`database/crm_database.db`).
- **Outputs**:
  - Backup files stored in `database/backups/`.

#### Process Description

1. **Initialization**:
   - Ensure the `database/backups/` directory exists.

2. **Backup Creation**:
   - Copy the current database file to a new file with a timestamped name (e.g., `crm_database_YYYYMMDD.db`).

3. **Backup Maintenance**:
   - List all backup files.
   - Delete backups older than one month.

4. **Logging**:
   - Log the completion of the backup process.

### Task 8: LoggingSetup (Utility)

- **Objective**: Set up logging configuration to maintain separate logs for normal execution and errors.
- **Inputs**:
  - None (configuration from `configs/settings.py`).
- **Outputs**:
  - Log files in `luigi_pipeline/logs/`:
    - `pipeline.log` for general logs.
    - `errors.log` for error logs.

#### Process Description

1. **Initialization**:
   - Ensure the `luigi_pipeline/logs/` directory exists.

2. **Logging Configuration**:
   - Configure the root logger to handle different logging levels.
   - Set up file handlers for `INFO` and `ERROR` levels.
   - Set up a console handler for `INFO` level.
   - Define the logging format.

3. **Usage**:
   - The `setup_logging` function is called at the beginning of the pipeline execution to initialize logging.

---

## Files and Directories

### Directories

- **luigi_pipeline/**: Contains all Luigi pipeline code.
  - **tasks/**: Luigi tasks for each step.
  - **utils/**: Utility modules.
  - **configs/**: Configuration files.
  - **logs/**: Log files.

- **data/**: Stores data at various stages.
  - **raw/**: Raw data from Notion.
  - **processed/**: Data after property extraction.
  - **normalized/**: Data after normalization.
  - **replaced/**: Data after applying replacements.
  - **transformed/**: Data ready for database loading.
  - **replacements/**: Replacement lists in CSV format.
  - **excel_files/**: Original replacement lists Excel file.

- **database/**: Contains the SQLite database and backups.
  - **backups/**: Backup files.

### Key Files

- **luigi_pipeline/pipeline.py**: Main Luigi pipeline script orchestrating tasks.
- **luigi_pipeline/tasks/\*.py**: Individual Luigi tasks.
- **luigi_pipeline/utils/\*.py**: Utility modules for Notion interaction, data manipulation, and normalization.
- **luigi_pipeline/configs/settings.py**: Centralized settings, including API keys and paths.
- **luigi_pipeline/configs/column_mappings.py**: Column mappings for each database.
- **data/excel_files/replacement_lists.xlsx**: Replacement lists provided by the sales team.
- **requirements.txt**: Python dependencies.
- **README.md**: Project documentation.

---

## Dataflow Summary

1. **Data Extraction**:
   - **Input**: None.
   - **Process**: Fetch raw data from Notion.
   - **Output**: Raw JSON files in `data/raw/`.

2. **Property Extraction**:
   - **Input**: Raw JSON files.
   - **Process**: Extract selected properties using column mappings.
   - **Output**: Processed JSON files in `data/processed/`.

3. **Normalization**:
   - **Input**: Processed JSON files.
   - **Process**: Normalize data according to defined rules.
   - **Output**: Normalized JSON files in `data/normalized/`.

4. **Apply Replacements**:
   - **Input**: Normalized JSON files, replacement lists.
   - **Process**: Replace company names, contact names, and region information.
   - **Output**: Replaced JSON files in `data/replaced/`.

5. **Transformation**:
   - **Input**: Replaced JSON files.
   - **Process**: Transform data into relational tables.
   - **Output**: CSV files in `data/transformed/`.

6. **Loading to Database**:
   - **Input**: CSV files, existing database.
   - **Process**: Load data into the database with incremental updates.
   - **Output**: Updated SQLite database in `database/crm_database.db`.

7. **Backup**:
   - **Input**: Updated database.
   - **Process**: Create a backup and maintain backups for the last month.
   - **Output**: Backup files in `database/backups/`.

---

## Additional Considerations

### Incremental Updates

- **Logic**:
  - The pipeline checks `last_edit_time` for each record.
  - Only records with updated `last_edit_time` are processed and updated in the database.

### Error Handling and Logging

- **Error Handling**:
  - Try-except blocks are used to catch exceptions.
  - Errors are logged to `errors.log`.

- **Logging**:
  - Normal execution logs are written to `pipeline.log`.
  - Error logs are written to `errors.log`.

### Multi-select Properties

- **Handling**:
  - Multi-select fields are converted to JSON strings before storage.
  - Fields include `campaign`, `package`, `products_ad`, `products_ex`, `services`.

### Time Zone Handling

- **Time Zone**:
  - Dates and times are handled in the Brazilian time zone (`America/Sao_Paulo`).

### Deployment Environment

- **Compatibility**:
  - The code is compatible with both Windows and Linux environments.
  - File paths and regex patterns are handled appropriately.

### Rate Limiting

- **Notion API**:
  - The `NotionClient` class includes logic to handle rate limits by retrying after the specified `Retry-After` duration.

---

## Conclusion

This document provides a detailed overview of the dataflow in the Luigi ETL pipeline, covering each task's objectives, inputs, outputs, and processes. By following the modular design and utilizing the specified directories and files, the pipeline ensures efficient data processing, maintains data integrity, and facilitates easy maintenance and scalability.

---