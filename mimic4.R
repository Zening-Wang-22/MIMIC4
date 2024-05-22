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
#bq_auth(use_oob = TRUE)

projectid = "marine-guard-420203"#replace with your own project id
bigrquery::bq_auth()#login with google account associated with physionet account

sql <- "
SELECT *
FROM `physionet-data.mimiciv_icu.icustays`
"
bq_data <- bq_project_query(projectid, query = sql)

icustays = bq_table_download(bq_data)

sql2 <- "
SELECT * FROM `physionet-data.mimiciv_derived.sepsis3`"

bq_data2 <- bq_project_query(projectid, query = sql2)

sepsis3 = bq_table_download(bq_data2)


sql3 <- "
SELECT * FROM `physionet-data.mimiciv_derived.chemistry`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data3 <- bq_project_query(projectid, query = sql3)

chemistry_sepsis3 = bq_table_download(bq_data3)


###################### 4 
sql4 <- "
SELECT * FROM `physionet-data.mimiciv_derived.complete_blood_count`
WHERE subject_id IN (
  SELECT subject_id FROM `physionet-data.mimiciv_derived.sepsis3` 
)
AND hadm_id IS NOT NULL"

bq_data4 <- bq_project_query(projectid, query = sql4)

blood_sepsis3 = bq_table_download(bq_data4)
cbc_sepsis3 = blood_sepsis3



#################### 5
sql5 <- "
SELECT sepsis.*,icu.hadm_id,icu.intime,icu.outtime,icu.los FROM `physionet-data.mimiciv_derived.sepsis3` sepsis
LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu
ON sepsis.stay_id = icu.stay_id"

bq_data5 <- bq_project_query(projectid, query = sql5)

icu_sepsis3 = bq_table_download(bq_data5)



############################
library(dplyr)
icu_chemistry <- left_join(icu_sepsis3, chemistry_sepsis3, by = "hadm_id")
icu_chemistry_cbc <- left_join(icu_chemistry, cbc_sepsis3, by = c("hadm_id", "charttime"))
icu_chemistry_cbc_cleaned <- icu_chemistry_cbc |> 
  select(-c(subject_id.x, subject_id.y, specimen_id.y)) |>
  rename(specimen_id = specimen_id.x) |>
  select(subject_id, stay_id, hadm_id, specimen_id, everything())


########################### Save 10 lines
icu_chemistry_cbc_cleaned_top10 <- head(icu_chemistry_cbc_cleaned, 10)
write.csv(icu_chemistry_cbc_cleaned_top10, "icu_chemistry_cbc_cleaned_top10.csv", row.names = FALSE)



