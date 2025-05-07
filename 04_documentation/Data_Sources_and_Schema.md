# Data Sources and Target Schema

This document details the data sources used in this project and outlines the proposed target schema for the consolidated Airtable base.

## Data Sources

1.  **Legacy Airtable Export (`00_source_data/airtable_monolithic/Providers-All Providers.csv`)**
    *   **Description:** A comprehensive CSV dump from a previous Airtable base. Contains a wide variety of fields, many potentially outdated or inconsistently populated.
    *   **Role:** Primary source for the initial provider list and NPIs. Used to establish the base mapping file (`05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv`). Contains many fields beyond basic identification that will need mapping later (e.g., locations, specialties, contact info).
    *   **Key Fields Used So Far:** `First Name`, `Last Name`, `National Provider Identifier (NPI)`.
    *   **Cleaning Applied:** Basic parsing for initial NPI/Name extraction.

2.  **Pulse System Exports (`00_source_data/pulse_data/`)**
    *   **Files:** `pulse_bhi.csv`, `pulse_counseling.csv`, `pulse_mm.csv`.
    *   **Description:** Recent exports from the Pulse system, separated by service line (Behavioral Health Integration, Counseling, Medication Management).
    *   **Role:** Provides potentially more up-to-date information on provider names, credentials, availability, location, specialties, etc. Used for reconciliation against the legacy Airtable data.
    *   **Key Fields Used So Far:** `Provider Name` (BHI, MM), `Therapist Name` (Counseling).
    *   **Cleaning Applied:** Significant cleaning was applied via scripts (`clean_csv.py`, `clean_pulse_files.py`, direct file edit) to trim whitespace, remove blank rows, standardize name columns, attempt to parse multi-value fields (like Credentials, Ages, Specialties), and fix specific character encoding/syntax highlighting issues (zero-width spaces).

3.  **Northshore Provider List (`05_airtable_and_mapping/03_northshore/northshore_names.csv`)**
    *   **Description:** A list containing provider names specific to Northshore.
    *   **Role:** Used as an additional source for provider name reconciliation against the primary mapping list.
    *   **Key Fields Used So Far:** `First Name`, `Last Name` (derived from original single name column).
    *   **Cleaning Applied:** Original file was split into First/Last Name columns, headers added, whitespace cleaned, and specific name corrections applied based on matching results.

4.  **Salesforce Picklists (`02_salesforce_picklist/`)**
    *   **Files:** `salesforce_ages.csv`, `salesforce_credentials.csv`, `salesforce_genders.csv`, `salesforce_treatment_modialities.csv`.
    *   **Description:** Canonical lists representing the standard, approved values for key categorical fields as defined in Salesforce.
    *   **Role:** Serve as the **source of truth** for standardizing corresponding data points from the source files. The goal is to map values found in source data (e.g., various representations of "LCSW" or age ranges) to the specific values present in these files.
    *   **Key Fields:** Single column containing the standard picklist values.
    *   **Cleaning Applied:** Basic formatting checks (trailing commas, blank lines) were applied.

## Proposed Target Airtable Schema (Conceptual)

This is a preliminary schema based on the project goals and data encountered so far. It will likely evolve as more fields are mapped.

**Main Provider Table (`Providers`)**

*   `Provider ID (PK)`: Unique Identifier (Potentially derived from NPI or generated)
*   `NPI`: National Provider Identifier (Unique, Indexed)
*   `First Name`: Text
*   `Last Name`: Text
*   `Full Name (Formula)`: Formula concatenating First and Last Name
*   `Credentials (Link to Credentials Table)`: Link to `Credentials` table (allows multiple)
*   `Gender Identity (Link to Gender Table)`: Link to `Gender Identity` table (single select)
*   `Ages Seen (Link to Ages Table)`: Link to `Ages Seen` table (allows multiple)
*   `Treatment Modalities (Link to Modalities Table)`: Link to `Treatment Modalities` table (allows multiple)
*   `Specialties (Link to Specialties Table)`: Link to `Specialties` table (allows multiple) - *Note: Requires creating a Salesforce picklist or similar standard list for specialties.*
*   `Locations (Link to Locations Table)`: Link to `Locations` table (allows multiple)
*   `Availability Status`: Single Select (e.g., "Accepting Referrals", "Closed", "Waitlist")
*   `Source System(s)`: Multi-Select or Text (e.g., "Pulse-MM", "Airtable-Legacy", "Northshore") - To track data provenance.
*   `Last Updated (Pulse)`: Date/Time
*   `Link to Website`: URL
*   `Other Considerations/Notes`: Long Text
*   ...(Other fields as identified and mapped from source data - e.g., contact info, insurance restrictions, specific program flags like OARS/STEM, etc.)

**Supporting Tables (Linked Records)**

These tables will be populated directly from the `02_salesforce_picklist` files (or created for concepts like Specialties/Locations).

*   **`Credentials` Table**
    *   `Credential Name (PK)`: Text (e.g., "LCSW", "MD", "PMHNP-BC")
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