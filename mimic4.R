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

projectid = "marine-guard-420203"#replace with your own project id
bigrquery::bq_auth()#login with google account associated with physionet account

#################### 1
sql1 <- "
SELECT sepsis.*,icu.hadm_id,icu.intime,icu.outtime,icu.los FROM `physionet-data.mimiciv_derived.sepsis3` sepsis
LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu
ON sepsis.stay_id = icu.stay_id"

bq_data1 <- bq_project_query(projectid, query = sql1)

icu_sepsis3 = bq_table_download(bq_data1)

#################### 2
sql2 <- "
SELECT * FROM `physionet-data.mimiciv_derived.chemistry`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data2 <- bq_project_query(projectid, query = sql2)

chemistry_sepsis3 = bq_table_download(bq_data2)


###################### 3
sql3 <- "
SELECT * FROM `physionet-data.mimiciv_derived.complete_blood_count`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data3 <- bq_project_query(projectid, query = sql3)

cbc_sepsis3 = bq_table_download(bq_data3)


###################### Join Data Set
icu_chemistry_cbc_cleaned <- icu_sepsis3 |> 
  left_join(chemistry_sepsis3, by = "hadm_id") |> 
  left_join(cbc_sepsis3, by = c("hadm_id", "charttime")) |> 
  select(-subject_id.x, -subject_id.y, -specimen_id.y) |> 
  rename(specimen_id = specimen_id.x) |> 
  select(subject_id, stay_id, hadm_id, specimen_id, everything())

########################### Save 10 lines
# icu_chemistry_cbc_cleaned_top10 <- head(icu_chemistry_cbc_cleaned, 10)
# write.csv(icu_chemistry_cbc_cleaned_top10, "icu_chemistry_cbc_cleaned_top10.csv", row.names = FALSE)



