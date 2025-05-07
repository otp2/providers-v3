# Provider Data Consolidation & Standardization (providers-v3)

This project aims to consolidate, clean, standardize, and map provider data from various historical and current sources into a reliable dataset suitable for a new Airtable base. The goal is to create a single source of truth aligned with Salesforce standards.

## Project Goal

To create a unified, clean, and accurate provider dataset by:
1.  Ingesting data from legacy Airtable exports, current Pulse system exports, and Northshore provider lists.
2.  Reconciling provider identities across these sources using NPI and name matching.
3.  Standardizing key data points (Credentials, Ages Seen, Gender Identity, Treatment Modalities) against canonical Salesforce picklists.
4.  Preparing the consolidated data for eventual import into a new, structured Airtable base.

For a detailed explanation of project goals, see [Project Goals and Scope](./04_documentation/Project_Goals_and_Scope.md).

## Current Status

*   **Data Ingestion:** Legacy Airtable, Pulse (BHI, Counseling, MM), and Northshore data loaded.
*   **Salesforce Picklists:** Standard picklists for Credentials, Ages, Genders, and Modalities loaded.
*   **Name Reconciliation:**
    *   Northshore names cleaned and reconciled against the primary mapping list (derived from legacy Airtable).
    *   Pulse names extracted, cleaned, consolidated, and reconciled against the primary mapping list.
    *   All Pulse and Northshore providers are now accounted for in the primary mapping list (`05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv`).
*   **Unmatched Provider Identification:** Providers listed in the primary mapping file but *not* found in Pulse or Northshore sources have been identified and saved separately.
*   **Initial Data Cleaning:** Basic formatting cleanup applied to Salesforce picklists and source files (whitespace, blank rows, specific character issues).

## Project Structure

```
├── 00_source_data/
│   ├── airtable_monolithic/      # Original Airtable CSV dump
│   │   └── Providers-All Providers.csv
│   ├── pulse_data/               # Raw Pulse system exports (cleaned in place)
│   │   ├── pulse_bhi/
│   │   ├── pulse_counseling/
│   │   └── pulse_mm/
│   ├── northshore/               # Original Northshore data (content moved/processed)
│   └── (other legacy sources...)
├── 01_processed_data/          # (Currently unused, may be used later)
├── 02_salesforce_picklist/     # Canonical picklists from Salesforce
│   ├── salesforce_ages.csv
│   ├── salesforce_credentials.csv
│   ├── salesforce_genders.csv
│   └── salesforce_treatment_modialities.csv
├── 03_scripts/                 # Contains helper scripts (often temporary, now deleted)
│   └── archive/                  # Older versions of scripts
├── 04_documentation/           # Project documentation
│   ├── Project_Goals_and_Scope.md
│   ├── Data_Sources_and_Schema.md
│   ├── Processing_Log_and_Methodology.md
│   ├── Salesforce_Picklist_Mapping.md
│   └── Next_Steps_and_Considerations.md
│   └── data_restructuring_plan.md # Original planning doc (may be outdated)
├── 05_airtable_and_mapping/    # Files related to mapping and reconciliation
│   ├── 01_name_npi_airtable/     # Primary mapping list derived from Airtable
│   │   └── provider_ids_for_mapping.csv
│   ├── 02_pulse/                 # Processed data from Pulse
│   │   └── pulse_consolidated_names.csv
│   ├── 03_northshore/            # Processed data from Northshore
│   │   └── northshore_names.csv
│   └── 04_not_in_pulse_or_northshore/ # Providers from mapping not found elsewhere
│       └── unmatched_providers.csv
├── .gitignore
└── README.md
```

## Key Documentation

*   [Project Goals and Scope](./04_documentation/Project_Goals_and_Scope.md)
*   [Data Sources and Schema](./04_documentation/Data_Sources_and_Schema.md)
*   [Processing Log and Methodology](./04_documentation/Processing_Log_and_Methodology.md)
*   [Salesforce Picklist Mapping](./04_documentation/Salesforce_Picklist_Mapping.md)
*   [Next Steps and Considerations](./04_documentation/Next_Steps_and_Considerations.md)

## Next Steps Overview

1.  **Salesforce Picklist Mapping:** Map source data fields (Credentials, Ages, etc.) to the standard Salesforce lists.
2.  **NPI Lookup & Validation:** Verify and find missing NPIs for providers.
3.  **Field Mapping:** Map remaining relevant fields from source data to the target schema.
4.  **Data Validation & Cleanup:** Perform final consistency checks and data cleaning.
5.  **Airtable Preparation:** Format data for import into the new Airtable base structure.

See [Next Steps and Considerations](./04_documentation/Next_Steps_and_Considerations.md) for details. 