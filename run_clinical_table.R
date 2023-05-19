#require('tidyverse')
require('dplyr')
require('tidyr')
require('gt')

setwd('~/git/ggo_analyses_capsule-7449557/')
source('code/clinical_table_script.R')

####
#require(tidyverse)
#webshot::install_phantomjs()
working_dir = '~/Sema4/etl/s4_curated_dataset_lci'
table.name = 'baseline_ex_location_patient_info'
df = read.csv(paste0(working_dir, '/', table.name, '.csv'))
predictors = c('gender_name', 'race_ethnicity', 'age_group', 'smoking_cat')
group.variable = 'cat'
table.file.name = paste0(working_dir, '/', table.name, '.pdf')
select = dplyr::select

clinical.subgroup.table = create_clinical_table(df, predictors, group.variable,
                                                'rows',
                                                table.name, table.file.name)

# debug
# group.variable = group
#
# df %>%
#   group_by(.data[[predictors[i]]], .data[[group.variable]]) %>%
#   count() %>%
#   spread(group.variable, n, fill=0)
#
# df %>%
#   group_by(.data[[group.variable]]) %>%
#   count() %>%
#   rename(subgroup.total=n)
