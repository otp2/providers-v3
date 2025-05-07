# Project Goals and Scope

## High-Level Goal

The primary objective of this project is to create a single, clean, reliable, and standardized dataset of provider information, ultimately residing in a new, well-structured Airtable base. This dataset will serve as the single source of truth for provider demographics, credentials, specialties, locations, and other key attributes, aligned with current business standards (Salesforce picklists).

## Rationale (The "Why")

Provider data is currently fragmented across several systems and files:

*   A **legacy Airtable export** (`Providers-All Providers.csv`) contains a wide range of historical data, but is potentially outdated, inconsistent, and lacks standardization.
*   Recent exports from the **Pulse system** (`pulse_bhi.csv`, `pulse_counseling.csv`, `pulse_mm.csv`) contain more current details but exist as separate files with their own formatting inconsistencies.
*   A **Northshore provider list** (`northshore_names.csv`) provided another source for reconciliation.
*   Other potential historical sources may exist.

This fragmentation makes it difficult to get a complete and accurate picture of the provider network. Data inconsistencies (e.g., different name formats, varying credential representations, non-standard specialty lists) hinder reporting, analysis, and operational processes. Aligning data with **Salesforce picklist standards** is crucial for future integration and consistency across business systems.

This project aims to resolve these issues by creating a unified and standardized dataset.

## Scope

### In Scope

The project includes the following activities:

*   **Data Ingestion:** Loading data from the specified source files:
    *   `00_source_data/airtable_monolithic/Providers-All Providers.csv`
    *   `00_source_data/pulse_data/` (BHI, Counseling, MM files)
    *   `00_source_data/northshore/northshore_names.csv` (and potentially others if identified)
    *   `02_salesforce_picklist/` files (as reference standards)
*   **Provider Identity Reconciliation:**
    *   Identifying unique providers across all sources.
    *   Utilizing National Provider Identifiers (NPIs) as the primary key where available.
    *   Employing fuzzy name matching techniques (normalized first/last names) for providers lacking NPIs or where NPIs might be inconsistent.
*   **Data Cleaning & Standardization:**
    *   Parsing and cleaning provider names (splitting first/last, handling suffixes/titles, removing extra characters).
    *   Cleaning and standardizing key categorical fields (Credentials, Ages Seen, Gender Identity, Treatment Modalities/Specialties) against the canonical Salesforce picklists found in `02_salesforce_picklist/`.
    *   Applying general data cleaning (trimming whitespace, handling blank rows/values, resolving character encoding issues).
*   **Data Consolidation & Mapping:**
    *   Defining a target data schema suitable for a new Airtable base (see `Data_Sources_and_Schema.md`).
    *   Mapping relevant fields from source files to the target schema.
    *   Creating intermediate files to track mapping and reconciliation progress (e.g., consolidated name lists, lists of unmatched providers).
*   **Gap & Inconsistency Analysis:** Identifying providers present in some sources but not others, and highlighting data points that conflict or require further investigation.
*   **Data Preparation:** Preparing the final, consolidated dataset in a format (likely CSV) suitable for import into a new Airtable base.
*   **Documentation:** Creating and maintaining documentation covering goals, sources, schema, processing steps, and methodology.

### Out of Scope (Initial Phase)

The following activities are considered out of scope for the initial phase of this project:

*   **Building the New Airtable Base:** This project focuses on preparing the *data*; the actual creation and configuration of the destination Airtable base is a subsequent step.
*   **Real-time Data Integration:** Establishing automated pipelines for ongoing data synchronization between source systems and the target Airtable base.
*   **Extensive NPI Lookup for *All* Missing NPIs:** While some NPI validation is in scope, performing exhaustive external lookups for every provider lacking an NPI in the source data is deferred unless deemed critical during the process.
*   **Resolution of *All* Data Conflicts:** While conflicts will be identified, the resolution of complex discrepancies requiring business decisions may be deferred. 