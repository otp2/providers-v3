# Data Sources and Target Schema

This document details the data sources used in this project and outlines the proposed target schema for the consolidated Airtable base.

## Data Sources

1.  **Legacy Airtable Export (`00_source_data/airtable_monolithic/Providers-All Providers.csv`)**
    *   **Description:** A comprehensive CSV dump from a previous Airtable base. Contains a wide variety of fields, many potentially outdated or inconsistently populated.
    *   **Role:** Primary source for the initial provider list and NPIs. Used to establish the base mapping file (`05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv`). It is also used as a source for web addresses (`Profile Link - Legacy Site`) and credentials (`Credentials`) during the enrichment of the `new_provider_truth_file.csv`.
    *   **Key Fields Used So Far:** `First Name`, `Last Name`, `National Provider Identifier (NPI)`, `Credentials`, `Profile Link - Legacy Site`.
    *   **Cleaning Applied:** Basic parsing for initial NPI/Name extraction. Names and NPIs are normalized during script processing.

2.  **Pulse System Exports (`00_source_data/pulse_data/`)**
    *   **Files:** `pulse_bhi.csv`, `pulse_counseling.csv`, `pulse_mm.csv`.
    *   **Description:** Recent exports from the Pulse system, separated by service line (Behavioral Health Integration, Counseling, Medication Management).
    *   **Role:** Provides potentially more up-to-date information on provider names, credentials, availability, location, specialties, etc. Used for reconciliation against the legacy Airtable data and for direct data enrichment (e.g., phone numbers from `pulse_bhi.csv`, credentials from all three for `new_provider_truth_file.csv`).
    *   **Key Fields Used So Far:** `Provider Name` (BHI, MM), `Therapist Name` (Counseling), `Patient Facing Number` (BHI), `Credentials` (all Pulse files).
    *   **Cleaning Applied:** Significant cleaning was applied via scripts (`clean_csv.py`, `clean_pulse_files.py`, direct file edit) to trim whitespace, remove blank rows, standardize name columns, attempt to parse multi-value fields (like Credentials, Ages, Specialties), and fix specific character encoding/syntax highlighting issues (zero-width spaces). Name parsing and credential parsing are applied during consolidation and enrichment scripts.

3.  **Northshore Provider List (`05_airtable_and_mapping/03_northshore/northshore_names.csv`)**
    *   **Description:** A list containing provider names specific to Northshore.
    *   **Role:** Used as an additional source for provider name reconciliation. Names from this list are incorporated into `new_provider_truth_file.csv`.
    *   **Key Fields Used So Far:** `First Name`, `Last Name` (derived from original single name column).
    *   **Cleaning Applied:** Original file was split into First/Last Name columns, headers added, whitespace cleaned, and specific name corrections applied based on matching results. Names are normalized during script processing.

4.  **Salesforce Picklists (`02_salesforce_picklist/`)**
    *   **Files:** `salesforce_ages.csv`, `salesforce_credentials.csv`, `salesforce_genders.csv`, `salesforce_treatment_modialities.csv`.
    *   **Description:** Canonical lists representing the standard, approved values for key categorical fields as defined in Salesforce.
    *   **Role:** Serve as the **source of truth** for standardizing corresponding data points. `salesforce_credentials.csv` is directly used to populate the `Salesforce Credential` column in `new_provider_truth_file.csv`.
    *   **Key Fields:** Single column containing the standard picklist values (e.g., `salesforce_credentials` in `salesforce_credentials.csv`).
    *   **Cleaning Applied:** Basic formatting checks (trailing commas, blank lines) were applied. Values are normalized (uppercase, stripped) during script lookups.

5.  **Processed Intermediate Files (Examples in `05_airtable_and_mapping/`)**
    *   **Files:** `provider_ids_for_mapping.csv`, `pulse_consolidated_names.csv`, `unmatched_providers.csv`.
    *   **Description:** These are intermediate files created during the reconciliation process.
    *   **Role:** They were used as inputs for creating the `new_provider_truth_file.csv`.
        *   `provider_ids_for_mapping.csv`: Legacy NPI/Name mapping, used for NPI lookups.
        *   `pulse_consolidated_names.csv`: Cleaned and combined names from all Pulse sources.
        *   `unmatched_providers.csv`: Providers from legacy data not found in Pulse or Northshore.

## Master Provider Data File

*   **File:** `01_processed_data/new_provider_truth_file.csv`
*   **Description:** This is the primary, consolidated master list of providers, intended to be the single source of truth going forward. It is created and enriched by various scripts.
*   **Current Schema (as of credential enrichment):**
    *   `uiud`: Text (Currently blank, intended for unique internal ID)
    *   `First Name`: Text
    *   `Last Name`: Text
    *   `NPI Number`: Text (Normalized NPI)
    *   `Internal Label`: Text (Indicates original main source category: "Pulse-BHI", "Pulse-Counseling", "Pulse-MM", "Northshore", "Legacy/Unmatched")
    *   `Phone Number`: Text (Formatted as (XXX) XXX-XXXX, populated for BHI providers)
    *   `Web Address`: Text (URL, populated from legacy Airtable)
    *   `Salesforce Credential`: Text (Standardized credential from `salesforce_credentials.csv`, populated by matching against source credentials)

## Proposed Target Airtable Schema (Conceptual)

This is a preliminary schema based on the project goals and data encountered so far. It will likely evolve as more fields are mapped. The `new_provider_truth_file.csv` serves as a direct precursor to this Airtable structure.

**Main Provider Table (`Providers`)**

*   `uiud (PK)`: Unique Identifier (Primary Key)
*   `NPI`: National Provider Identifier (Unique, Indexed)
*   `First Name`: Text
*   `Last Name`: Text
*   `Full Name (Formula)`: Formula concatenating First and Last Name
*   `Internal Label`: Text (Source category)
*   `Phone Number`: Phone
*   `Web Address`: URL
*   `Salesforce Credential (Text or Link)`: Text (if single) or Link to `Credentials` table (if allowing multiple canonical credentials per provider in the future). *Currently, the script populates with the first matched Salesforce credential.*
*   `Gender Identity (Link to Gender Table)`: Link to `Gender Identity` table (single select)
*   `Ages Seen (Link to Ages Table)`: Link to `Ages Seen` table (allows multiple)
*   `Treatment Modalities (Link to Modalities Table)`: Link to `Treatment Modalities` table (allows multiple)
*   `Specialties (Link to Specialties Table)`: Link to `Specialties` table (allows multiple) - *Note: Requires creating a Salesforce picklist or similar standard list for specialties.*
*   `Locations (Link to Locations Table)`: Link to `Locations` table (allows multiple)
*   `Availability Status`: Single Select (e.g., "Accepting Referrals", "Closed", "Waitlist")
*   `Source System(s) - Detailed`: Multi-Select or Text (e.g., "Pulse-MM", "Airtable-Legacy", "Northshore") - To track data provenance if needed beyond `Internal Label`.
*   `Last Updated (Pulse)`: Date/Time
*   `Other Considerations/Notes`: Long Text
*   ...(Other fields as identified and mapped from source data)

**Supporting Tables (Linked Records)**

These tables will be populated directly from the `02_salesforce_picklist` files (or created for concepts like Specialties/Locations).

*   **`Credentials` Table**
    *   `Credential Name (PK)`: Text (e.g., "LCSW", "MD", "PMHNP-BC") - Sourced from `salesforce_credentials.csv`.
    *   `Providers (Link to Providers Table)`: Link back to providers with this credential.
*   **`Gender Identity` Table**
    *   `Gender Name (PK)`: Text (e.g., "Male", "Female", "Non-binary")
    *   `Providers (Link to Providers Table)`: Link back.
*   **`Ages Seen` Table**
    *   `Age Group Name (PK)`: Text (e.g., "Children (5-9)", "Seniors 65+")
    *   `Providers (Link to Providers Table)`: Link back.
*   **`Treatment Modalities` Table**
    *   `Modality Name (PK)`: Text (e.g., "CBT", "DBT", "ACT")
    *   `Providers (Link to Providers Table)`: Link back.
*   **`Specialties` Table** (Requires defining a standard list)
    *   `Specialty Name (PK)`: Text (e.g., "Anxiety", "Depression", "Substance Use")
    *   `Providers (Link to Providers Table)`: Link back.
*   **`Locations` Table** (Requires defining a standard list or parsing from source)
    *   `Location Name (PK)`: Text (e.g., "Naperville", "Virtual Only", "Hinsdale")
    *   `Providers (Link to Providers Table)`: Link back.

This linked record structure enforces standardization and makes filtering/grouping much more powerful in Airtable. 