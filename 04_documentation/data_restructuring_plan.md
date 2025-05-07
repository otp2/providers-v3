# Provider Data Restructuring and Airtable Integration Plan

## 1. Project Goal
   - To restructure and consolidate provider data from various source systems (Pulse/LOMG, Website, Salesforce, etc.).
   - To harmonize shared concepts (e.g., specialties, locations) that may be represented differently across sources into a single, canonical representation.
   - To enrich the data with additional information as needed.
   - To create a well-structured Airtable base with main tables and linked record fields for efficient data management, consistency, and analysis.

## 2. Current Data Sources
   - `airtable/Providers-All Providers.csv` (Initial monolithic file)
   - Categorized CSVs (to be uploaded):
     - `categorized_provider_data/all_fields_main/` (Expected to be the most comprehensive source for core provider data)
     - `categorized_provider_data/website_only/`
     - `categorized_provider_data/pulse_table/` (Contains LOMG grid data)
     - `categorized_provider_data/salesforce_table/`

## 3. Proposed Airtable Structure
   - **Main Tables (Examples - to be refined):**
     - `Providers`: Central table for core provider information. Will link to master supporting tables for attributes like specialties, languages, locations, etc.
     - `Locations`: Master list of all physical clinic sites, hospitals, or office locations. Data from various sources will be consolidated here. Providers will link to this table.
     - *(Potentially other main tables based on distinct core concepts from categorized data, e.g., a table for Salesforce-specific entities if they are not directly provider attributes)*

   - **Master Supporting Tables (Accessed via Linked Fields in Main Tables):**
     - `Master Specialties`: A single, authoritative list of all medical services or specialties. Variations from different source files (Pulse, Website, Salesforce) will be mapped to these canonical entries.
     - `Master Languages`: A standardized list of languages.
     - `Master Credentials`: A standardized list of professional credentials.
     - `Master Conditions Treated`: A single, authoritative list of medical conditions.
     - `Master Treatment Modalities`: Standardized list of therapeutic approaches.
     - `Master Insurance Plans`: A consolidated list of insurance plans.
     - *(Other master supporting tables will be identified for shared concepts. These tables are crucial for data consistency and will be populated after a harmonization process.)*
   
   - **Key Principle:** Shared concepts existing across multiple source files (like specialties, locations, etc.) will have a dedicated "Master" supporting table in Airtable. Data from source files will be mapped and linked to these master tables to ensure a single source of truth within Airtable for these attributes.

## 4. Key Steps & Tasks
   - **Data Profiling & Source Analysis:** For each source CSV, identify relevant columns, their data types, and the range/format of their values, especially for fields intended for master supporting tables (e.g., all columns containing 'specialty' information).
   - **Data Cleaning and Preparation:** Standardize formats within each source file as much as possible.
   - **Schema Design for Airtable:**
     - Define fields and data types for all main tables.
     - Define fields and data types for all master supporting tables.
   - **Data Harmonization & Mapping (Critical Step):**
     - For each shared concept (e.g., Specialties, Locations):
       - Extract all unique values from the relevant columns across *all* source files.
       - Develop a canonical list of values for the corresponding master Airtable table (e.g., the definitive list for `Master Specialties`).
       - Create a clear mapping (e.g., in a spreadsheet or a dedicated mapping table) from each source-specific value to its canonical equivalent in the master Airtable table.
   - **Picklist Generation:**
     - Generate raw picklists from source columns to aid in the harmonization process.
     - The finalized canonical lists from the harmonization step will form the content of the master supporting tables.
   - **Data Migration Strategy:**
     - Phase 1: Populate master supporting tables with the harmonized, canonical values.
     - Phase 2: Import data into main tables, using the mapping rules to link to the correct records in the master supporting tables.
   - **Airtable Configuration:** Set up linked record fields, views, rollups, lookups, and formulas.

## 5. Tools & Technologies
   - Python (for data processing, transformation, harmonization assistance, and picklist generation)
   - Pandas library
   - Airtable
   - Spreadsheets (potentially for managing mapping rules during harmonization)

## 6. Potential Challenges & Considerations
   - Complexity of mapping diverse source values to a canonical model, especially with many variations or nuanced differences.
   - Ensuring data integrity during the mapping and linking process.
   - Scalability of the mapping process if there are thousands of unique source values.
   - Defining the "source of truth" if different files provide conflicting core information for the same provider.
   - Keeping the mapping rules and Airtable data synchronized if source data changes frequently.