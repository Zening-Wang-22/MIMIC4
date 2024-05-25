# MIMIC-IV Data Processing for Sepsis Research

## Overview
This repository is dedicated to processing the MIMIC-IV dataset for upcoming sepsis research. 

## Data Preparation
The `mimic4.R` script within this repository is used to generate the desired datasets from the MIMIC-IV database. It operates by creating numerous subtables based on specific criteria, which are then merged to form a comprehensive dataset. 

### Integration with BigQuery
The `mimic4.R` file is configured to connect with Google BigQuery. We utilize SQL within R to query and manipulate data directly from BigQuery.

### Example Dataset
Included in this repository is an example dataset named `icu_chemistry_cbc_cleaned_top10.csv`. This file represents a preliminary merged dataset comprised of only three subtables generated from the `mimic4.R` script. We will update the complete dataset later. 
