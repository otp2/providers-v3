# Processing Log and Methodology

This document tracks the key data processing and cleaning steps undertaken in this project.

## Initial Setup & Exploration

*   **Git Repository Initialization:** Local Git repository initialized and connected to `https://github.com/otp2/providers-v3.git`.
*   **Initial Environment Setup:** Python environment checked, `pandas` library confirmed/installed.
*   **Project Structure Review:** Directory contents listed and reviewed to understand initial file layout (`list_dir` command).
*   **File Content Sampling:** Read initial lines of `README.md`, `04_documentation/data_restructuring_plan.md`, and key scripts (`process_identifiers.py`, `generate_source_specialty_picklists.py`) to understand original intent and data structure (`read_file` command).

## Salesforce Picklist Cleaning

*   **Files:** `02_salesforce_picklist/salesforce_*.csv`
*   **Methodology:** Visual inspection followed by scripted edits (`edit_file` tool, later deleted).
*   **Actions:**
    *   Removed trailing commas from header lines (`salesforce_genders.csv`, `salesforce_credentials.csv`).
    *   Attempted to remove trailing blank lines (`salesforce_credentials.csv` - faced difficulty with automated edits, may require manual check).

## Credential Reconciliation (Mapping vs. Salesforce)

*   **Goal:** Ensure credentials listed in the main mapping file (`01_providers_with_uids.csv` at the time) aligned with the `salesforce_credentials.csv` picklist.
*   **Methodology:**
    1.  Created a temporary Python script (`credential_checker.py`) using `pandas`.
    2.  Read master credentials from `salesforce_credentials.csv` into a set.
    3.  Read provider data from `01_providers_with_uids.csv`.
    4.  Extracted unique credentials from the provider data.
    5.  Compared the two sets:
        *   Identified credentials in provider data but **not** in the master Salesforce list.
        *   Identified credentials in the master Salesforce list that were **not** used in the provider data.
    6.  Printed a report summarizing the findings.
*   **Outcome:** Report generated highlighting discrepancies (e.g., "LCPC" vs "LCSW", "PMHNP" vs "PMHNP-BC"). Script deleted after use.

## Name/NPI Mapping List Creation & Initial Population

*   **Goal:** Create a central file mapping NPIs to standardized First/Last names, initially based on the legacy Airtable data.
*   **Methodology:**
    1.  Created directory `05_airtable_and_mapping/`.
    2.  Created temporary Python script (`extract_provider_data.py`) using `pandas`.
    3.  Read `00_source_data/airtable_monolithic/Providers-All Providers.csv`.
    4.  Selected columns: `First Name`, `Last Name`, `National Provider Identifier (NPI)`.
    5.  Saved the selected columns to `05_airtable_and_mapping/provider_ids_for_mapping.csv`.
    6.  Moved this file into a subdirectory: `05_airtable_and_mapping/01_name_npi_airtable/`.
*   **Outcome:** `provider_ids_for_mapping.csv` created as the primary mapping reference. Script deleted.

## Northshore Data Processing & Reconciliation

*   **Goal:** Integrate Northshore provider names and reconcile them against the mapping list.
*   **Methodology:**
    1.  Created processing subdirectories (`02_pulse`, `03_northshore`) within `05_airtable_and_mapping/`.
    2.  Manually placed `northshore_names.csv` into `05_airtable_and_mapping/03_northshore/`.
    3.  **Cleaning:** Reformatted `northshore_names.csv` using `edit_file` (direct content replacement after reading the file) to:
        *   Split single name column into `First Name`, `Last Name`.
        *   Add headers.
        *   Trim whitespace.
    4.  **Matching:** Created and ran a temporary Python script (`name_matcher.py`) to:
        *   Normalize names (lowercase, strip whitespace, remove hyphens/spaces) in both `northshore_names.csv` and `provider_ids_for_mapping.csv` to create a `search_key`.
        *   Compare `search_key`s.
        *   Report matched names, unmatched Northshore names, and unmatched mapping names.
    5.  **Reconciliation:** Based on the matching report, created and ran a temporary script (`update_northshore_names.py`) to apply specific corrections (e.g., Jon -> Jonathan, remove entries, fix hyphenation) to `northshore_names.csv`.
    6.  **Final Check:** Re-ran `name_matcher.py` to confirm corrections improved matches.
    7.  Added missing provider (`Priya Kapoor`) to `provider_ids_for_mapping.csv` via script (`add_priya_kapoor.py`).
    8.  **Mapping File Formatting:** Reformatted `provider_ids_for_mapping.csv` via script (`reformat_provider_ids.py`) to hyphenate multi-word last names and move NPI to the first column.
*   **Outcome:** `northshore_names.csv` cleaned; names reconciled against mapping list; `provider_ids_for_mapping.csv` updated and reformatted. All temporary scripts deleted.

## Pulse Data Processing & Reconciliation

*   **Goal:** Integrate Pulse provider names (BHI, Counseling, MM) and reconcile them against the mapping list.
*   **Methodology:**
    1.  Created subdirectories within `00_source_data/pulse_data/` (`pulse_bhi`, `pulse_counseling`, `pulse_mm`).
    2.  Manually uploaded corresponding CSVs.
    3.  **Initial Cleaning:** Ran a generic cleaning script (`clean_csv.py`) on all three Pulse files to trim whitespace, remove blank rows.
    4.  **Targeted Cleaning:** Ran a more advanced script (`clean_pulse_files.py`) to standardize name columns, handle multi-value separators (`;#`), attempt credential parsing, and normalize case/whitespace.
    5.  **Troubleshooting `pulse_counseling.csv`:** Addressed syntax highlighting issues caused by hidden zero-width space characters (`\u200b`) via direct file content replacement (`edit_file` tool).
    6.  **Consolidation:** Created and ran script (`consolidate_pulse_names.py`) to:
        *   Read the relevant name column from each cleaned Pulse file (`Provider Name` or `Therapist Name`).
        *   Apply cleaning logic (remove quotes, parentheses, attempt suffix/credential removal).
        *   Split into `First Name`, `Last Name` (hyphenating multi-part last names).
        *   Add a `Pulse Label` column (`BHI`, `Counseling`, `MM`).
        *   Save the consolidated list to `05_airtable_and_mapping/02_pulse/pulse_consolidated_names.csv`.
        *   Included detailed record counting for verification.
    7.  **Matching:** Created and ran script (`pulse_mapping_checker.py`) to compare `pulse_consolidated_names.csv` against `provider_ids_for_mapping.csv` using normalized name `search_key`s, reporting matches and mismatches.
    8.  **Reconciliation:** Based on mismatches, applied corrections using scripts:
        *   `update_mapping_file.py`: Updated names in the mapping file based on NPI and added missing providers.
        *   `update_pulse_consolidated.py`: Corrected specific names in the consolidated Pulse list.
        *   `fix_final_pulse_mismatches.py`: Corrected two final specific mismatches in the mapping file.
    9.  **Final Check:** Re-ran `pulse_mapping_checker.py` to confirm zero unmatched names from the Pulse list.
*   **Outcome:** Pulse files cleaned; names extracted, cleaned, consolidated; all Pulse names successfully reconciled with the main mapping list (`provider_ids_for_mapping.csv`). All temporary scripts deleted.

## Unmatched Provider Identification

*   **Goal:** Isolate providers from the main mapping list who were not found in *either* Pulse or Northshore data.
*   **Methodology:** Created and ran script (`save_unmatched_providers.py`) to:
    *   Read mapping, Pulse, and Northshore name files.
    *   Normalize names and create sets of search keys.
    *   Identify mapping keys not present in Pulse *or* Northshore sets.
    *   Retrieve corresponding NPI, First Name, Last Name from the mapping file.
    *   Save results to `05_airtable_and_mapping/04_not_in_pulse_or_northshore/unmatched_providers.csv`.
*   **Outcome:** List of 60 potentially inactive or differently named providers saved for review. Script deleted.

## Name Standardization Example (Alexandra Lee-Elstein)

*   **Goal:** Standardize a specific provider's last name.
*   **Methodology:**
    1.  Used `grep_search` to find the NPI for "Alexandra Lee-Elstein" in the mapping file.
    2.  Created and ran script (`update_elstein_name.py`) to change the Last Name associated with NPI `1043520661` from "Lee-Elstein" to "Elstein" in `provider_ids_for_mapping.csv`.
*   **Outcome:** Name updated in the mapping file. Script deleted.

## Creation of New Master Provider List (`new_provider_truth_file.csv`)

*   **Goal:** Create a new, consolidated master provider list to serve as the primary source of truth, moving away from `provider_ids_for_mapping.csv`. This new file will incorporate names from Pulse, Northshore, and unmatched legacy providers, along with NPIs and a new UIUD column.
*   **Methodology:**
    1.  Created Python script `03_scripts/create_new_truth_file.py`.
    2.  The script loads data from:
        *   `05_airtable_and_mapping/02_pulse/pulse_consolidated_names.csv` (Pulse providers)
        *   `05_airtable_and_mapping/03_northshore/northshore_names.csv` (Northshore providers)
        *   `05_airtable_and_mapping/04_not_in_pulse_or_northshore/unmatched_providers.csv` (Legacy providers not in Pulse/Northshore)
        *   `05_airtable_and_mapping/01_name_npi_airtable/provider_ids_for_mapping.csv` (for NPI lookup).
    3.  It standardizes names (First, Last) and NPIs.
    4.  For Pulse providers, it retains the `Pulse Label`. For Northshore and unmatched, it assigns "Northshore" or "Legacy/Unmatched" as the `Internal Label`.
    5.  It combines these sources, prioritizing NPIs for matching where available.
    6.  The script generates a new CSV with columns: `uiud` (blank), `First Name`, `Last Name`, `NPI Number`, `Internal Label`.
*   **Execution & Outcome:**
    *   Script `03_scripts/create_new_truth_file.py` was executed.
    *   A new master provider list `01_processed_data/new_provider_truth_file.csv` was successfully generated, containing 251 unique providers.

## Enriching Master List: Phone Numbers and Web Addresses

*   **Goal:** Add phone numbers (for BHI providers) and web addresses (from legacy Airtable) to the `new_provider_truth_file.csv`.
*   **Methodology:**
    1.  Created Python script `03_scripts/enrich_truth_file.py`.
    2.  The script loads:
        *   `01_processed_data/new_provider_truth_file.csv` (the current master list).
        *   `00_source_data/pulse_data/pulse_bhi/pulse_bhi.csv` (for BHI phone numbers, using `Provider Name` and `Patient Facing Number`).
        *   `00_source_data/airtable_monolithic/Providers-All Providers.csv` (for web addresses, using `First Name`, `Last Name`, `National Provider Identifier (NPI)`, and `Profile Link - Legacy Site`).
    3.  It adds two new columns to the master list: `Phone Number` and `Web Address`.
    4.  Phone numbers are populated only for providers with an `Internal Label` containing "BHI". It matches BHI providers by name (after normalization) to find their phone number in `pulse_bhi.csv`. Phone numbers are formatted to `(XXX) XXX-XXXX`.
    5.  Web addresses are populated by matching providers from the master list (first by NPI, then by normalized name) to the legacy Airtable data.
*   **Execution & Outcome:**
    *   Script `03_scripts/enrich_truth_file.py` was executed successfully.
    *   The `new_provider_truth_file.csv` was updated in place with phone numbers for 21 BHI providers and web addresses for 176 providers.

## Enriching Master List: Salesforce Credentials

*   **Goal:** Add a standardized `Salesforce Credential` column to `new_provider_truth_file.csv` by cross-referencing credentials from various source files against the official `02_salesforce_picklist/salesforce_credentials.csv`.
*   **Methodology:**
    1.  Created Python script `03_scripts/add_salesforce_credentials.py`.
    2.  The script loads:
        *   `01_processed_data/new_provider_truth_file.csv`.
        *   `02_salesforce_picklist/salesforce_credentials.csv` (loaded into a set for lookup, preserving original casing for output).
        *   Credential sources: `pulse_bhi.csv`, `pulse_counseling.csv`, `pulse_mm.csv` (using `Provider Name` and `Credentials`), and `Providers-All Providers.csv` (using `First Name`, `Last Name`, `NPI`, and `Credentials`).
    3.  It builds lookup dictionaries from the source files to map providers (by NPI and normalized name key) to their raw credential strings.
    4.  For each provider in the truth file, it:
        *   Collects all their raw credential strings from the source files.
        *   Parses each raw string, splitting by delimiters (`,`, `;`, `/`, `&`, `#`, ` and `) and normalizing (uppercase, strip periods).
        *   Checks each parsed credential candidate against the Salesforce credential set.
        *   If a match is found, the *first* corresponding original Salesforce credential (from `salesforce_credentials.csv`) is written to the new `Salesforce Credential` column.
    5.  Initial run resulted in 0 matches due to an error in the `parse_credentials` function that incorrectly added spaces to single credentials (e.g., "LCSW" became "L CS W").
    6.  The `parse_credentials` function was debugged and corrected by removing the faulty regex line.
*   **Execution & Outcome:**
    *   Script `03_scripts/add_salesforce_credentials.py` (with corrected parsing) was executed successfully.
    *   The `new_provider_truth_file.csv` was updated in place. The `Salesforce Credential` column was populated for 215 providers. 