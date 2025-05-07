# providers-v3

This project aims to consolidate, clean, and restructure provider data from various sources into a well-organized Airtable base.

## Project Structure

-   **/00_source_data/**: Contains the original, raw data files from different systems.
    -   `airtable_monolithic/`: Initial complete CSV dump.
    -   `identifiers/`: Files specifically containing provider unique identifiers (NPI, etc.).
    -   `pulse_data/`: Data extracted from the Pulse system (includes LOMG grid info).
    -   `website_data/`: Data extracted from website sources.
    -   `salesforce_data/`: Data from Salesforce (placeholder for now).
-   **/01_processed_data/**: Contains cleaned, transformed data and generated picklists.
    -   `main_provider_table/`: Holds the evolving main provider dataset, starting with identifiers and progressively enriched.
    -   `source_specific_picklists/`: Raw picklists generated directly from columns in source files (e.g., all unique specialties found in Pulse data).
    -   `harmonized_picklists/`: Master picklists created after consolidating and standardizing values from source-specific picklists (e.g., a single master list of all unique, standardized specialties).
-   **/02_scripts/**: Contains all Python scripts used for data processing, picklist generation, etc.
    -   `archive/`: Older or superseded scripts.
-   **/03_documentation/**: Contains detailed planning documents, data dictionaries, and other project documentation.
    -   `data_restructuring_plan.md`: The detailed plan for data processing, Airtable schema, and harmonization strategy.
-   **/.cursor/**: Contains project-specific rules and settings for AI assistant collaboration.
    -   `rules.mdc`: Rules for AI interaction during this project.

## General Workflow

1.  **Source Data Collection**: Place raw CSV extracts into the appropriate subdirectories under `/00_source_data/`.
2.  **Identifier Processing**: Run scripts (e.g., `02_scripts/process_identifiers.py`) to ensure all providers have unique IDs. Output goes to `/01_processed_data/main_provider_table/`.
3.  **Picklist Generation (Source-Specific)**: Run scripts (e.g., `02_scripts/generate_source_specialty_picklists.py`) to extract unique values from relevant columns in source files. Outputs go to `/01_processed_data/source_specific_picklists/`.
4.  **Data Harmonization**: Manually or with script assistance, compare source-specific picklists to create master, harmonized picklists. Store these in `/01_processed_data/harmonized_picklists/`.
5.  **Data Enrichment & Table Building**: Iteratively update the main provider table and develop other related tables using the harmonized data and picklists.
6.  **Airtable Integration**: Plan and execute the import of processed data into a structured Airtable base.

(Refer to `/03_documentation/data_restructuring_plan.md` for more detailed steps and schema design.) 