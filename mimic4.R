library(data.table)
library(tidyr)
library(tidyverse)
library(readxl)
library(gdata)
library(lubridate)
library(comorbidity)
library(bigrquery)
library(tidyr)
library(DBI)
library(dbplyr)
library(dplyr)

'%!in%' <- function(x,y)!('%in%'(x,y))

#### Connect to BigQuery#####

projectid = "marine-guard-420203" # replace with your own project id
bigrquery::bq_auth() # login with google account associated with physionet account

#################### 1 ICU
sql1 <- "
SELECT sepsis.*, icu.hadm_id, icu.intime, icu.outtime, icu.los FROM `physionet-data.mimiciv_derived.sepsis3` sepsis
LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu
ON sepsis.stay_id = icu.stay_id"

bq_data1 <- bq_project_query(projectid, query = sql1)
icu_sepsis3 = bq_table_download(bq_data1)

#################### 2 chemistry
sql2 <- "
SELECT * FROM `physionet-data.mimiciv_derived.chemistry`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data2 <- bq_project_query(projectid, query = sql2)
chemistry_sepsis3 = bq_table_download(bq_data2)


###################### 3 CBC
sql3 <- "
SELECT * FROM `physionet-data.mimiciv_derived.complete_blood_count`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data3 <- bq_project_query(projectid, query = sql3)
cbc_sepsis3 = bq_table_download(bq_data3)

###################### 4 antibiotics
sql4 <- "
SELECT * FROM `physionet-data.mimiciv_derived.antibiotic`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data4 <- bq_project_query(projectid, query = sql4)
antibiotic_sepsis3 = bq_table_download(bq_data4)

###################### 5 norepinephrine (no subject_id, filter by stay_id)
sql5 <- "
SELECT * FROM `physionet-data.mimiciv_derived.norepinephrine_equivalent_dose`
WHERE stay_id IN (
  SELECT stay_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)"

bq_data5 <- bq_project_query(projectid, query = sql5)
vasopressin_sepsis3 = bq_table_download(bq_data5)


###################### 6 vitalsign
sql6 <- "
SELECT * FROM `physionet-data.mimiciv_derived.vitalsign`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)"

bq_data6 <- bq_project_query(projectid, query = sql6)
vitalsign_sepsis3 = bq_table_download(bq_data6)


###################### 7 race + admission type
sql7 <- "
SELECT a.subject_id, a.hadm_id, a.admission_type, a.race
FROM `physionet-data.mimiciv_hosp.admissions` AS a
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data7 <- bq_project_query(projectid, query = sql7)
race_adtype_sepsis3 = bq_table_download(bq_data7)


###################### 8 age
sql8 <- "
SELECT a.subject_id, a.hadm_id, a.age
FROM `physionet-data.mimiciv_derived.age` AS a
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data8 <- bq_project_query(projectid, query = sql8)
age_sepsis3 = bq_table_download(bq_data8)

###################### 9 gender
sql9 <- "
SELECT p.subject_id, p.gender
FROM `physionet-data.mimiciv_hosp.patients` AS p
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)"

bq_data9 <- bq_project_query(projectid, query = sql9)
gender_sepsis3 = bq_table_download(bq_data9)

###################### Join Data Set (will revise later)
# icu_chemistry_cbc_cleaned <- icu_sepsis3 |> 
#   left_join(chemistry_sepsis3, by = "hadm_id") |> 
#   left_join(cbc_sepsis3, by = c("hadm_id", "charttime")) |> 
#   select(-subject_id.x, -subject_id.y, -specimen_id.y) |> 
#   rename(specimen_id = specimen_id.x) |> 
#   select(subject_id, stay_id, hadm_id, specimen_id, everything())

########################### Save 10 lines
# icu_chemistry_cbc_cleaned_top10 <- head(icu_chemistry_cbc_cleaned, 10)
# write.csv(icu_chemistry_cbc_cleaned_top10, "icu_chemistry_cbc_cleaned_top10.csv", row.names = FALSE)



