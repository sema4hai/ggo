# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# # Analyses for draft version 2

# %%
#!source ~/.ssh/s4_curated_dataset_lci.sh
#!pip install pymysql venn seaborn pandasql plotly psutil
import os
from pathlib import Path
import pandas as pd, numpy as np
import seaborn as sns
import venn
from sqlalchemy import create_engine, text
from pandasql import sqldf
import toml
import matplotlib.pyplot as plt
#plt.style.use('classic')
get_ipython().run_line_magic('matplotlib', 'inline')


# %%
#database credentials and connection information 
config = toml.load(Path('~/.ssh/s4_curated_dataset_lci.toml').expanduser())
working_dir=Path('~/Sema4/etl/s4_curated_dataset_lci').expanduser()
environ = config
db_user = environ.get('DATABASE_USERNAME')
db_password = environ.get('DATABASE_PASSWORD')
#db_host = 'jnj-lci.cluster-ckwbormwta82.us-east-1.rds.amazonaws.com'
db_host = 'jnj-lci-8.ckwbormwta82.us-east-1.rds.amazonaws.com'
db_name = 's4_curated_dataset_lci'
db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}'


# %%
#connect to database
engine = create_engine(db_url)
connection = engine.connect().execution_options(autocommit=True)

# %% [markdown]
# ## I. Base study population
# Request:
# * Patients with an ICD code for lung cancer (LCa) diagnosis (~13,000 patients) in the Mt. Sinai database are the base population for this study. 
# * This broad cohort is divided into 3 cohorts as shown below.
# ![cohort_def.png](attachment:cohort_def.png)
# 
# Approach: see IV.1 below.
# 
# ## II. Full study period
# Request: All data available 2003-December 2020.
# 
# Approach:
# * The database contains some data out of the study range.
# * For the GGO analyses, all patient with the first CT chest scan before 2003 or after 2020 are excluded.
# 
# ## III. GGO cohort specification
# Requests: 
# * Patient to be 18 years of age or older as on date of first chest CT or LDCT scan.  
# * Patients with reported GGOs on at least one radiology report/progress notes/other clinical document. 
# 
# Approach:  
# * For the analyses, patients younger than 18 yo at the first ‘CT chest’ radiology reports are excluded. 
# * Only patients with recognized GGO term (ggo_level_1 like ‘GGO_term’ in notes_ggo table) are included in the analyses.
# 
# ## IV. Requested outputs as part of Phase 1
# %% [markdown]
# ## IV.1 For overall cohort of ~13,000 patients with an ICD Dx code for lung cancer
# Approach:
# * ICD codes for lung cancer used: 
#     * ICD-10: C34 
#     * ICD-9: 162 
# 
# ### 1.a Definition for sub cohorts
# Approach: 
# * Define the three mutually exclusive cohorts as below: 
#     * Path-confirmed: patients have cancer_type=1 in the 'cancer_diagnoses' table 
#     * Likely LCa: patients who were not path-confirmed to have LCa and on >= 3 visits with an LCa ICD code  
#     * Unlikely LCa: patients who were not path-confirmed to have LCa and on < 3 visits with an LCa ICD
# * Then exclude the patients with LCa diagnosis (first path-confirmed report if available, otherwise first ICD report) reported earlier than 2003 or later than 2020. 
# 
# Results: 
# * The cohort info can be found in the view: v_s4_lca_cohort 
# 
# ### 1.b Sample count of sub cohorts (cohorts are mutually exclusive)
# Summary:

# %%
confirmed = """SELECT distinct d_person_id 
    FROM cancer_diagnoses where cancer_type_id = 1
    """
other = f"""SELECT d_person_id, count(distinct(event_date)) as n_days_with_lca_diagnosis
    FROM person_icd_codes 
    where (icd_code like 'C34%' or icd_code like '162%')
        and d_person_id not in ({confirmed})
    GROUP BY d_person_id
    """
select = f"""SELECT d_person_id, 'path-confirmed' as cohort 
    FROM ({confirmed}) as confirmed
    UNION ALL
    SELECT d_person_id, 'likely' as cohort 
    FROM ({other}) as other
    where n_days_with_lca_diagnosis >= 3 
    UNION ALL
    SELECT d_person_id, 'unlikely' as cohort
    FROM ({other}) as other
    where n_days_with_lca_diagnosis < 3
    """
tmp = connection.execute(text(f"""create or replace view v_s4_lca_cohort as
    {select}"""))


# %%
pd.read_sql("select count(*) total_patients from v_s4_lca_cohort",
                 connection, index_col='total_patients')


# %%
# checking
pd.read_sql("select cohort, count(*) patients from v_s4_lca_cohort group by cohort",
            connection, index_col='cohort')

# %% [markdown]
# ### 1.c Summary statistics of CT scans for each of the cohorts (distribution of CT scans, median, average scans per patient during study period)
# Approach:
# * Using 'CT chest' records in the radiologies table.
# 
# Summary:

# %%
tmp=connection.execute(text(r"""
    create or replace view _ct_chest as
    select * from radiologies
    where imaging_study like '%CT%' and location like '%Chest%'
    """))


# %%
cohort_order=['path-confirmed', 'likely', 'unlikely']
describe_order = ['mean', 'min', '25%', '50%', '75%', 'max']


# %%
data = pd.read_sql("""
    select d_person_id, cohort
    , count(distinct radiology_date) ct_scans
    from v_s4_lca_cohort
    join _ct_chest using(d_person_id)
    group by d_person_id, cohort
    """, connection)
data.groupby('cohort')['ct_scans'].describe()    .loc[cohort_order, describe_order] 

# %% [markdown]
# Finding:
# * Patients in the path-confirmed and LCa likely cohorts tend to have more CT chests on average than those in the unlikely cohort (5 scans vs 4).
#   
# ### 1.d Summary statistics for observation time (pre- and post LCa Dx) for each LCa patient cohort in the database
# Approach: 
# * LCa Dx date is defined as the first LCa Dx date in cancer_diagnoses if available, otherwise the first LCa ICD report date. 
# * Observation time is defined by days from first visit date to Dx date (pre-Dx) or the last visit date to Dx date (post-Dx). (The last visit date has been updated with the refreshed database.) 
# 
# Summary: 

# %%
sql = r"""
with first_icd as (
    select d_person_id
    , min(event_date) first_icd_date
    , min(year_event_date) first_icd_year
    from person_icd_codes
    where (icd_code like 'C34%' or icd_code like '162%')
    group by d_person_id
), first_confirmed as (
    select d_person_id
    , min(date_of_diagnosis) first_confirmed_date
    , min(year_of_diagnosis) first_confirmed_year
    from cancer_diagnoses
    join cancer_types using (cancer_type_id)
    where cancer_type_name='LCA'
    group by d_person_id
)
select d_person_id
, coalesce(first_confirmed_date, first_icd_date) dx_date
, coalesce(first_confirmed_year, first_icd_year) dx_year
from first_icd
left join first_confirmed using (d_person_id)
"""
#pd.read_sql(sql, connection)


# %%
tmp = connection.execute(text(f"""create or replace view _lca_dx as
    {sql}
    """))
#pd.read_sql('select count(*) from _lca_dx', connection)


# %%
tmp = pd.read_sql("""
select * from _lca_dx
where dx_year>2020 or dx_year<2003
order by dx_year
""", connection)


# %%
tmp = pd.read_sql("""
select greatest(2,4,null), least(2, 4, null)
""", connection)


# %%
sql = """
with range_visit as (
    select d_person_id, min(visit_date) first_visit, max(visit_date) last_visit
    from visits
    group by d_person_id
), range_dx as (
    select d_person_id, min(event_date) first_dx, max(event_date) last_dx
    from person_icd_codes
    group by d_person_id
), range_proc as (
    select d_person_id, min(procedure_date) first_proc, max(procedure_date) last_proc
    from procedure_occurrences
    group by d_person_id
), range_med as (
    select d_person_id, min(date_of_medication) first_med, max(date_of_medication) last_med
    from medications
    group by d_person_id
)
-- select * from range_visit
select d_person_id
, cohort
, dx_date
, least(coalesce(first_visit, 99999)
    , coalesce(first_dx, 99999)
    , coalesce(first_proc, 99999)
    , coalesce(first_med, 99999)
    ) first_visit_date
, greatest(coalesce(last_visit, 0)
    , coalesce(last_dx, 0)
    , coalesce(last_proc, 0)
    , coalesce(last_med, 0)
    ) last_visit_date
from v_s4_lca_cohort
join _lca_dx using (d_person_id)
left join range_visit using (d_person_id)
left join range_dx using (d_person_id)
left join range_proc using (d_person_id)
left join range_med using (d_person_id)
group by d_person_id, cohort, dx_date
"""
# print(data.shape)
# some patients (n=201) has no visit data?
# data.first_visit_date.isna().sum()


# %%
connection.execute("drop table if exists _lca_range")
tmp = connection.execute(text(f"""
create table _lca_range as 
{sql}
"""))


# %%
#debug
tmp = pd.read_sql("""
    select count(distinct d_person_id) patients
    from v_s4_lca_cohort
    left join visits using (d_person_id)
    where visit_date is null
    """, connection)


# %%
#debug
tmp = pd.read_sql("""
    select dx_date is not null, count(distinct d_person_id)
    from _lca_dx
    group by dx_date is not null
    """, connection)


# %%
# debug
tmp = pd.read_sql("""
    select d_person_id
    , min(event_date) first_icd_date
    from person_icd_codes
    where (icd_code like 'C34%' or icd_code like '162%')
    group by d_person_id
    """, connection)
# tmp.first_icd_date.isna().sum()


# %%
tmp = pd.read_sql(text("""
    select d_person_id
    , min(event_date) first_icd_date
    from person_icd_codes
    where (icd_code like 'C34%' or icd_code like '162%')
        and event_date is not null
    group by d_person_id
    """), connection)
# tmp.first_icd_date.isna().sum() #353
# len(tmp) #12863


# %%
data = pd.read_sql("""
select * from _lca_range
""", connection)


# %%
data = pd.concat([
        data.assign(followup_days=data.dx_date-data.first_visit_date+1, followup_type='pre_dx'),
        data.assign(followup_days=data.last_visit_date-data.dx_date+1, followup_type='post_dx'),
])


# %%
#quick check # more thorough check later.
followup_days = data.followup_days
#sum(followup_days.isna())
#followup_days = data.followup_days.fillna()
#sum(data.first_visit_date.isna())
#sum(data.last_visit_date.isna())
# sum(data.dx_date.isna()) #686


# %%
tmp = pd.read_sql("""
select count(*) from _lca_dx where dx_date is null
""", connection)
#343


# %%
#quickfix the outliers
followup_days = data.followup_days.fillna(0)
followup_days[followup_days<0] = 0
data = data.assign(followup_days=followup_days)
followup_days[followup_days<1] = 1
data = data.assign(log10_followup_days=np.log10(followup_days))
#data.shape


# %%
#summary
followup_type_order = ['pre_dx', 'post_dx']
#print ('Summary statistics for follow up days in Log10 scale:')
#summ = data.groupby(['cohort', 'followup_type']).log10_followup_days.describe()
summ = data.groupby(['cohort', 'followup_type']).followup_days.describe()
multi_index = pd.MultiIndex.from_product([cohort_order, followup_type_order])
summ.reindex(multi_index)[describe_order]

# %% [markdown]
# Visualization:
# * A violin plot: the y axis is observation days in log10 scale.

# %%
#plot
g = sns.catplot(data=data, kind='violin',
                x="cohort", y="log10_followup_days", 
                        hue="followup_type", hue_order=followup_type_order,
                        split=True,
                        inner="quartile",
                        scale="count" ,scale_hue = False, cut=0,
                  )
g

# %% [markdown]
# Finding:
# * The path-confirmed LCa patients have longer followup time after Dx, but shorter observation time before Dx, compared with the other two cohorts (statistic tests not performed). 
# 
# ## IV 2: For GGO cohort (cohort of patients that meet GGO cohort specifications per above in III)
# 
# Approach: see section III.
# Summary:
# * Unique patient count for the GGO cohort - breakdown of these numbers for each LCa cohort

# %%
# filter patients by age>=18 at the first ct chest
first_ct_chest = pd.read_sql("""
select d_person_id
, min(radiology_date) first_ct_chest_date
, min(year_radiology_date) first_ct_chest_year
, cast(floor(min(radiology_date)/365.25) as signed) first_ct_chest_age
from _ct_chest
group by d_person_id
""", connection)
#7019 (50%) patients has any ct_chest report
sqldf("""
    select count(*)
    from first_ct_chest
    where first_ct_chest_age<18 or first_ct_chest_year not between 2003 and 2020
    """, locals())
# 83 patients will be fitered off

# %% [markdown]
# ## Cohort GGO before diagnosis

# %%
# precheck the notes_ggo
# pd.read_sql("""select * from notes_ggo limit 100""", connection)
pd.read_sql(text("""
    select count(*), count(distinct d_person_id, note_date) notes, count(distinct d_person_id) total_patients
    from notes_ggo 
    join v_s4_lca_cohort using (d_person_id)
    join _lca_dx using (d_person_id)
    where ggo_level1 like 'GGO_term%'
        and dx_date>=note_date
    """), connection)


# %%
connection.execute(text("""
    drop table if exists _note_ggo_before_dx;
    """))
connection.execute(text("""
    create table _cohort_ggo_before_dx as (
        select distinct d_person_id
        from notes_ggo 
        join v_s4_lca_cohort using (d_person_id)
        join _lca_dx using (d_person_id)
        where ggo_level1 like 'GGO_term%'
            and dx_date>=note_date
    )
    """))

# %% [markdown]
# 

%run -i -n code/func.py
#from func import qc_base, run_to_csv
# %%
# create baseline_size_max biggest size on the first report with size, together with ggo_date

base = """
    with _size as (
        select d_person_id, note_date ggo_date, ggo_level2 size_cat, cast(ggo_level3 as float) size
        from _cohort_ggo_before_dx
        join notes_ggo using (d_person_id)
        where ggo_level1='GGO_size'
        -- group by d_person_id, ggo_date
    ), result as (
        select *
        from (select *, row_number() over(
                partition by d_person_id
                order by ggo_date, size desc) rn
            from _size) tmp
        where rn=1
    )
"""
qc_base(base)
tablename='baseline_size_cat'
run_to_csv(tablename, base)


## baseline GGO number
def get_base_first_max(ggo_level1):
    return """
    with _cat as (
        select d_person_id, note_date ggo_date, ggo_level2 cat
        from _cohort_ggo_before_dx
        join notes_ggo using (d_person_id)
        where ggo_level1='{ggo_level1}'
    ), result as (
        select *
        from (select *, row_number() over(
                partition by d_person_id
                order by ggo_date, cat desc) rn
            from _cat) tmp
        where rn=1
    )
"""
#qc
base = get_base_first_max(ggo_level1)
qc_base(base)
tablename='baseline_number_cat'
run_to_csv(tablename, base)

## baseline exclusive location
# %%
# create the assistant table
base = """with _location as (
        select distinct d_person_id, note_date ggo_date, concat(ggo_level2, ':', ggo_level3) cat
        from _cohort_ggo_before_dx
        join notes_ggo using (d_person_id)
        where ggo_level1='GGO_location'
            and ifnull(ggo_level3, 'other') != 'other'
    ), _exclusive as (
        select d_person_id, ggo_date
        from _location
        group by d_person_id, ggo_date
        having (count(*)=1)     
    ), result as (
        select *
        from (select *, row_number() over(
                partition by d_person_id
                order by ggo_date, cat) rn
            from _location 
            join _exclusive using (d_person_id, ggo_date)) tmp
        where rn=1      
    )"""
qc_base(base)
tablename = 'baseline_ex_location_uml'
df = run_to_table(tablename, base)
df.to_csv(f'{working_dir}/{tablename}.csv', index=False)

#from each patient gather the follow columns at index_date (ggo_date)
#smoking_status: latest 
#gender
#race
#age
#ctab = make_patient_info_from_db(connection)
## create_patient_info.sql
tablename = 'baseline_ex_location_patient_info'
pd.read_sql(f'select * from {tablename}', connection)\
    .to_csv(f'{working_dir}/{tablename}.csv', index=False)

#compile the patient data for clinical summary by GT R package
#plot clinical table in R
#atable(df, group_col=cat, taget_cols=c(age_group, gender, race))

## last status to dx
#make a status_change rank table later
level2_case = """case ggo_level2
            when 'resolved/disappeared' then '01.resolved'
            when 'decreased/improved/reduced/shrink' then '02.decreased'
            when 'stable/no change/persistent' then '03.stable'
            when 'increased/progressed' then '04.increased'
            end"""
base = f"""
    with _cat as (
        select d_person_id, note_date ggo_date
        , {level2_case} cat
        from _cohort_ggo_before_dx
        join notes_ggo using (d_person_id)
        join _lca_dx using (d_person_id)
        """  """where ggo_level1='GGO_status_change'
            and ifnull(ggo_level2, '') != ''
            and note_date<=dx_date
    ), result as (
        select *
        from (select *, row_number() over(
                partition by d_person_id
                order by ggo_date, cat desc) rn
            from _cat) tmp
        where rn=1
    )
"""
qc_base(base)
tablename='last_status_cat'
run_to_csv(tablename, base)

## first potential reason before dx
level2_case = """case ggo_level2
    when 'infectious_inflammatory' then '02.infect/inflam'
    when 'other' then '01.other'
    when 'malignant neoplasm' then '04.malignant'
    when 'premalignancy' then '03.premalig'
    end"""
level1_name = 'GGO_potential_cause
base = f"""
    with _cat as (
        select d_person_id, note_date ggo_date
        , {level2_case} cat
        from _cohort_ggo_before_dx
        join notes_ggo using (d_person_id)
        join _lca_dx using (d_person_id)
        """  """where ggo_level1='{level1_name}'
            and ifnull(ggo_level2, '') != ''
            and note_date<=dx_date
    ), result as (
        select *
        from (select *, row_number() over(
                partition by d_person_id
                order by ggo_date, cat desc) rn
            from _cat) tmp
        where rn=1
    )
"""
##### run the draft_v2.2.R for summary and heatmap

    with ggo_note as (
        select distinct d_person_id, note_date, radiology_id is not null as is_radiology
        from notes_ggo
        where GGO_level1 like 'GGO_term%'
        order by d_person_id, note_date, is_radiology
    ), first_ggo as (
        select d_person_id
        , min(note_date) first_ggo_date
        from ggo_note
        group by d_person_id
    ), first_ct_chest as (
       select d_person_id
        , cast(floor(min(radiology_date)/365.25) as signed) first_ct_chest_age
        , min(year_radiology_date) first_ct_chest_year
        from _ct_chest
        group by d_person_id 
    ), cohort as (
        select d_person_id
        , first_ggo_date
        from first_ggo
        left join first_ct_chest using(d_person_id)
        where (first_ct_chest_age is null or first_ct_chest_age >= 18)
            and (first_ct_chest_year is null or first_ct_chest_year between 2003 and 2020)
    ), patient_ggo_rads as (
        select d_person_id
        , count(distinct note_date) ggo_rads
        from ggo_note
        where is_radiology
        group by d_person_id
    ), patient_ggo_other as (
        select d_person_id
        , count(distinct note_date) ggo_other
        from ggo_note
        where not is_radiology
        group by d_person_id
    ), patient_rads as (
        select d_person_id
        , count(distinct radiology_date) rads -- consider rads reported in the same day as one
        from radiologies
        group by d_person_id
    )
    -- select *
    select d_person_id, cohort, first_ggo_date
    , coalesce(rads, 0) total_radiology_reports
    , coalesce(ggo_rads, 0) radiology_reports_with_ggo
    , coalesce(ggo_other, 0) other_reports_with_ggo
    -- , first_scan_date, last_scan_date -- nullable
    from cohort
    left join patient_rads using (d_person_id)
    left join patient_ggo_rads using (d_person_id)
    left join patient_ggo_other using (d_person_id)
    -- left join patient_first_last_scan using (d_person_id)
    join v_s4_lca_cohort using (d_person_id)
    """
tmp = connection.execute(text(f"""
    create or replace view _patient_ggo as 
    {sql}"""))
#patient_ggo = pd.read_sql(text(sql), connection)
#patient_cohort = pd.read_sql("select * from v_s4_lca_cohort", connection)
#data = sqldf("""select * from patient_ggo join patient_cohort using (d_person_id)""", locals())
#patient_ggo


# %%
data = pd.read_sql("select * from _patient_ggo",
                   connection) 


# %%
pd.DataFrame(dict(total_patients=[len(data)])).set_index('total_patients')


# %%
#summary --to be refined later
data.set_index(['d_person_id']).groupby(['cohort']).size().to_frame(name='patients').loc[cohort_order, :]

# %% [markdown]
# ### 2a. Total number of radiology reports available, number of (radiology) reports with GGO findings – breakdown of these numbers for each LCa sub-cohort
# Approach: 
# * Radiology reports are counted for each patient from the radiologies table.
# * GGO reports are counted for each patient from the notes_ggo table.
#     * Radiology reported in the same day are counted as one report.
#     * Other notes reported in the same day are counted as one report. 
# Summary: 

# %%
report_type_order = ['total_radiology_reports', 'radiology_reports_with_ggo', 'other_reports_with_ggo']
data_melt = data.melt(id_vars=['d_person_id', 'cohort'],
          value_vars=report_type_order,
          var_name='report_type', value_name='reports')
summ = data_melt.set_index(['d_person_id']).groupby(['cohort', 'report_type']).reports.describe()
multi_index = pd.MultiIndex.from_product([cohort_order, report_type_order])
summ.reindex(multi_index)[describe_order]


# %%
# plot
g = sns.catplot(data=data_melt, kind='box', #.assign(log10_of_reports=np.log10(data_melt.reports+1)),
            x="cohort", order=cohort_order,
            y="reports", #y="log10_of_reports",
            hue="report_type", hue_order=report_type_order,
            aspect=1.4
            )

# %% [markdown]
# Visualization:
# * Box plots of each type of reports per patient, stratified by cohorts.
#  
# Finding: 
# * The percentage of radiology reports with a GGO finding is higher in the path-confirmed and LCa-likely cohorts as compared to the LCa-unlikely cohort (11.3% path-confirmed vs 7.8% in unlikely). 
# 
# ### 2.b. Distribution of follow-up scans for the GGO cohort (following index qualifying scan) in time, regardless of if a follow-up scan had GGO finding.
# Approach: 
# * Used the CT chest radiology reports after the index report of GGO (first GGO from radiology or other reports). 
# 
# Summary:  
# * The distribution of the days between the index GGO report and the first CT chest scan after that day for each patient. 
# 
# Visualization: 
# * A histogram of the same distribution:  days are on log10 scale. Note that a patient can be counted only in one bin.

# %%
sql = """
    select d_person_id, cohort
    , min(radiology_date-first_ggo_date) as closest_scan_post_ggo
    from _patient_ggo
    join _ct_chest using (d_person_id)
    where radiology_date>first_ggo_date
    group by d_person_id, cohort
    """
data = pd.read_sql(sql, connection)
#data


# %%
# summary
#data.set_index('d_person_id').groupby('cohort').closest_scan_post_ggo.describe().loc[cohort_order, describe_order]
data.set_index('d_person_id').describe().loc[describe_order, :]


# %%
# plot
g = sns.displot(data, x='closest_scan_post_ggo', log_scale=True)
g = g.set_axis_labels('Days to the closest CT chest scan after the index GGO report', 'Patients')

# %% [markdown]
# 
# ### 2.c. Summary statistics for number of scans per patient with a GGO finding and distribution in time
# Approach: 
# * Radiology reports with a GGO finding are used for this analysis. 
# 
# Summary: 
# * Distribution of the total GGO scans per patient. 
# * Distribution of the timing (days) of GGO scans relative to LCa Dx date. (Negative days mean scans reported before the LCa Dx) 
# 
# Visualization: 
# * A Histogram of the relative days from GGO scans to LCa Dx date for each patient.  Note that the y axis is count of patients with a scan within the relevant bin, and a patient could be counted in more than one bins (if have multiple GGO scans spanning multiple bins). 
# 
# Finding: 
# * More GGO are reported after the Cancer Dx.
# * About half of the GGO patients has a GGO finding within 50 days to the LCa Dx date. 

# %%
sql = """
with ggo_note as (
      select distinct d_person_id, note_date, radiology_id is not null as is_radiology
        from notes_ggo
        where GGO_level1 like 'GGO_term%'
        order by d_person_id, note_date, is_radiology
)
select d_person_id, cohort
, note_date - dx_date relative_day_to_dx
from ggo_note
join _patient_ggo using(d_person_id)
join _lca_dx using (d_person_id)
    where is_radiology
-- group by d_person_id, cohort
"""
data = pd.read_sql(text(sql), connection)
#data


# %%
# summary
data.groupby('d_person_id').size().describe().to_frame(name='GGO radiology reports per patient').loc[describe_order,:]


# %%
data.relative_day_to_dx.describe().to_frame(name='Relative days of GGO scan to LCa Dx').loc[describe_order, :]


# %%
g = sns.displot(data=data,
            x='relative_day_to_dx',
            aspect=2,
           )
g = g.set(xlim=(-3000, 3000))
g = g.set_axis_labels('Relative day of GGO report to LCa Dx', 'Patients')

# %% [markdown]
# ### 2.d. Assess temporality of report with GGO finding with respect to LCa Dx code:
# Request: 
# * Pre-LCa Dx period: # of scans with GGO findings in the pre-path confirmed LCa diagnosis date (for cohort 1), pre-‘proxy LCa’ confirmation date (for cohort 2a, 2b) 
# * Post-LCa Dx period: GGO findings in scans occurring in the post-period for each cohort 
# 
# Approach: 
# * Count number of radiology reports with a GGO finding before and after LCa diagnosis, respectively, for each patient.
# * Then further stratify the counts by the three cohorts. 
# 
# Summary:
# * Descriptive statistics of the counts above. Note that a patient could have GGO findings in both pre- and post- LCa Dx. 
# 
# Visualization:
# * Split violin plots of the distribution of the number of GGO findings, stratified by cohort and pre/post-Dx. 
# 
# Finding: 
# * The path-confirmed patients have more GGO finding post cancer Dx (average 2.6 vs 1.7 for unlikely). 

# %%
sql = """
with ggo_note as (
      select distinct d_person_id, note_date, radiology_id is not null as is_radiology
        from notes_ggo
        where GGO_level1 like 'GGO_term%'
        order by d_person_id, note_date, is_radiology
)
select d_person_id, cohort
, case when note_date < dx_date then 'pre_dx' else 'post_dx' end as ggo_timing
, count(*) ggo_scans
from ggo_note
join _patient_ggo using(d_person_id)
join _lca_dx using (d_person_id)
    where is_radiology
group by d_person_id, cohort, note_date < dx_date
"""
data = pd.read_sql(text(sql), connection)
#data


# %%
# summary
summ = data.set_index('d_person_id').groupby(['cohort', 'ggo_timing']).describe().ggo_scans
summ.reindex(pd.MultiIndex.from_product([cohort_order, followup_type_order]))[['count']+ describe_order]


# %%
# plot
g = sns.catplot(data=data, kind='violin', #.assign(log2_ggo_scans=np.log2(data.ggo_scans)),
               x="cohort", order=cohort_order,
               y="ggo_scans",
               hue="ggo_timing", hue_order=followup_type_order,
               split=True,
               inner="quartile",
               scale='count'
              )

# %% [markdown]
# ### 2.e. Observation time for the GGO cohort (pre- and post LCa Dx) in the database
# Approach:
# * Similar as in 1.d. 
# 
# Summary:
# * Statistics for followup days in normal scale.
# 
# Visualization: 
# * Split violin plots. 
# 
# Finding:
# * Similar as in 1.d.

# %%
data = pd.read_sql(text("""
select d_person_id, lr.cohort, dx_date
, first_visit_date
, last_visit_date
from _patient_ggo pg
join _lca_range lr using (d_person_id)
"""), connection)
#data


# %%
data = pd.concat([
        data.assign(followup_days=data.dx_date-data.first_visit_date+1, followup_type='pre_dx'),
        data.assign(followup_days=data.last_visit_date-data.dx_date+1, followup_type='post_dx'),
])


# %%
#quickfix the outliers
followup_days = data.followup_days.fillna(0)
followup_days[followup_days<0] = 0
data = data.assign(followup_days=followup_days)

followup_days[followup_days<1] = 1
data = data.assign(log10_followup_days=np.log10(followup_days))
#data.shape


# %%
#summary
followup_type_order = ['pre_dx', 'post_dx']
summ = data.groupby(['cohort', 'followup_type']).followup_days.describe()
multi_index = pd.MultiIndex.from_product([cohort_order, followup_type_order])
summ.reindex(multi_index)[describe_order]


# %%
#plot
g = sns.catplot(data=data, kind='violin',
               x="cohort", order=cohort_order,
               y="log10_followup_days", 
               hue="followup_type", hue_order=followup_type_order,
               split=True,
               inner="quartile",
               scale="count" ,scale_hue = False, cut=0, aspect=1.4)

# %% [markdown]
# ### 2.f. Distribution of CPT “incidental” vs. “screening” procedure codes (Table 1) for the GGO cohort
# Approach:  
# * CT chest scan type of incidental or screening is defined using the procedure codes according to the manually annotated reference table (ref_ggo_ct_type.csv) 
# * CT chest scan reported within 30 days before or at date of the index GGO report are summarized 
# 
# Visualization:
# * A Venn diagram of number of patients with incidental, or screening, or both (the overlapping area) types of CT chest scan reported as described above. 
# 
# Finding:
# * The vast majority of patients with a GGO report have an incidental CT chest scan reported within a month before the index GGO report. 

# %%
tmp = pd.read_csv('../ref_ggo_ct_type.csv')
tmp.to_sql('ref_ggo_ct_type', con=connection, if_exists='replace')


# %%
data = pd.read_sql("""
    select distinct d_person_id, procedure_type
    from _procedure_ct_type
    join _patient_ggo using (d_person_id)
    where first_ggo_date > procedure_date
        and first_ggo_date-procedure_date<30
    """, connection)
#data


# %%
grouped = data.groupby('procedure_type')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g = venn.venn(data_)

# %% [markdown]
# ### 2.g. Frequency distribution of ICD codes within (±) a 30-day window of the report with GGO finding (Table 2) if they appeared in structured data
# Approach: 
# * The ICD codes predefined in Table 2 were used to search the diagnoses table. 
# 
# Summary: 
# * The number of patients matched any of the ICD code reported within 30 days before or after the index GGO report.
# * The number of patients matched stratified by each ICD code. 

# %%
tmp = pd.read_csv('../ref_ggo_icd_cm.csv')
tmp.to_sql('ref_ggo_icd_cm', con=connection, if_exists='replace')


# %%
res = pd.read_sql(text("""
    with m as (
        select count(distinct d_person_id) patients
        from _patient_ggo p
        join person_icd_codes pi using (d_person_id)
        join ref_ggo_icd_cm r on pi.icd_code like concat(r.diagnosis_code, '%')
        where abs(event_date-first_ggo_date)<30
    ), total as (
        select count(distinct d_person_id) patients
        from _patient_ggo p
    )
    select m.patients matched_patients
    , m.patients/total.patients * 100.0 percentage_matched
    from m join total on 1
    """),  connection, index_col='matched_patients')
res


# %%
# pd.read_sql("""select * from ref_ggo_icd_cm""", connection)
pd.read_sql(text("""
    select diagnosis_code, dx_description
    , count(distinct d_person_id) patients
    from _patient_ggo p
    join person_icd_codes pi using (d_person_id)
    join ref_ggo_icd_cm r on pi.icd_code like concat(r.diagnosis_code, '%')
    where abs(event_date-first_ggo_date)<30
    group by diagnosis_code, dx_description
    """), connection)

# %% [markdown]
# Finding: 
# * The most reported ICD codes within one month around the index GGO report is R91.
# 
# ### 2.h. Frequency distribution of each GGO concept/characteristic 
# 
# Approach:
# * The GGO relevant entities recognized by the NLP pipeline, are normalized to categories defined by experts from J&J, for each of the following semantic groups. 
# 
# ### 2.h - Potential causes:  
# Summary:
# * Patients with each category ever reported. 
# 
# Visualization:
# * A Venn diagram of number of patients with one or more categories reported. Overlapping means those patients have more than one category ever reported. 
# %% [markdown]
# 

# %%
data = pd.read_sql("""
    select GGO_level2 GGO_potential_cause
    , count(distinct d_person_id) patients
    from notes_ggo
    where GGO_level1='GGO_potential_cause'
    group by GGO_level2
    """, connection)
data


# %%
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_potential_cause'
    group by d_person_id, GGO_level2
    """, connection)
#data


# %%
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g = venn.venn(data_)

# %% [markdown]
# Finding: 
# * The most reported potential causes are infectious/inflammatory and malignancy, and a substantial proportion of those patients have both causes reported (half of those with malignancy causes also with malignancy causes). 
# 
# ### 2.h - GGO_size 
# Summary:  
# * Patients with each category ever reported. 
# * A frequency table summarizing number of patients with each category at baseline and endpoint.  Note that 
#     * Patients with GGO size reported in only a single date are excluded. 
#     * The largest category is used if there are more than one category reported in the same day. 
# 
# Visualization:
# * A 2d-histogram with size in mm at baseline (x) and endpoint (y).  Note that 
#     * Patients with GGO size available in only single date are excluded from the plot 
#     * The largest size is used if there are more than one GGO sizes reported in the same day. 
#     * The GGO sizes are in log10 scale
#     * the boxplots summarized the distribute for baseline and endpoint respectively. 
#     * Blue line is for 6mm and red line for 20 mm 
#     * The color scale is for number of patients in that bin (square) 

# %%
tmp = pd.read_sql("""
    select GGO_level2 GGO_size_category
    , count(distinct d_person_id) patients
    from notes_ggo
    where GGO_level1='GGO_size' and GGO_level2 is not null -- quickfix
    group by GGO_level2
    """, connection)


# %%
cat_order = ['<6mm(0.6cm)', '6-20mm(0.6-2cm)', '>20mm(2cm)']
tmp.set_index('GGO_size_category').loc[cat_order, :]


# %%
data = pd.read_sql("""
with _res as (
    select distinct d_person_id person_id
    , GGO_level2 ggo_size_category
    , cast(GGO_level3 as float) ggo_size_in_mm
    , note_date ggo_date
    from notes_ggo
    where GGO_level1='GGO_size' and note_date is not null 
        and GGO_level3 is not null
), res as (
    select *
    from _res
    where ggo_size_in_mm < 1000 -- quickfix the outliers
), earliest as (
    select person_id
    , ggo_size_in_mm
    , ggo_size_category
    , ggo_date
    from (select *, row_number() over (
            partition by person_id
            order by ggo_date, ggo_size_in_mm desc) rn
        from res) tmp
    where rn=1
), latest as (
    select person_id
    , ggo_size_in_mm
    , ggo_size_category
    , ggo_date
    from (select *, row_number() over (
            partition by person_id
            order by ggo_date desc, ggo_size_in_mm desc) rn
        from res) tmp
    where rn=1
)
select person_id
, earliest.ggo_size_in_mm baseline_ggo_size_in_mm
, earliest.ggo_size_category baseline_ggo_size_category
, latest.ggo_size_in_mm latest_ggo_size_in_mm
, latest.ggo_size_category latest_ggo_size_category
from earliest
join latest using (person_id)
where earliest.ggo_date != latest.ggo_date
;
""", connection)
#data
#data.baseline_ggo_size_in_mm.plot()


# %%
cat = sqldf("""
    select baseline_ggo_size_category, latest_ggo_size_category
    , count(distinct person_id) patients
    from data
    where baseline_ggo_size_category is not null
        and latest_ggo_size_category is not null
    group by baseline_ggo_size_category, latest_ggo_size_category

    """, locals())

cat.pivot('baseline_ggo_size_category', 'latest_ggo_size_category', 'patients').loc[cat_order, cat_order]


# %%
g = sns.JointGrid(data=data, x="baseline_ggo_size_in_mm", y='latest_ggo_size_in_mm')
g.plot_joint(sns.histplot, cbar=True, log_scale=True)
g.plot_marginals(sns.boxplot)
g.ax_joint.axvline(6)
g.ax_joint.axhline(6)
g.ax_joint.axhline(20, color='red')
g.ax_joint.axvline(20, color='red')
g.fig.set_figwidth(8)

# %% [markdown]
# Findings: 
# * The median GGO sizes among all relevant patients are smaller at endpoint (see the boxplots). 
# * In cases where the baseline GGO size is large (>20mm), they are more likely to be treated and have a mediate/small size reported at the endpoint (see the bottom right corner split by the red lines). 
#   
# ### 2.h - GGO_location 
# Summary:
# * Number of patients with each location category ever reported. Level2: left/right, Level3: lower/upper. 
# 
# Visualization:
# * A Venn diagram of patients with one or more categories ever reported at level2 (left/right lung). 

# %%
pd.read_sql("""
    select GGO_level2 GGO_location_level2, GGO_level3 GGO_location_level3
    , count(distinct d_person_id) patients
    from notes_ggo
    where GGO_level1='GGO_location' and GGO_level2 is not null -- quickfix
    group by GGO_level2, GGO_level3
    """, connection)


# %%
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_location'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g = venn.venn(data_)

# %% [markdown]
# Findings: 
# * Not surprisingly, more patients reported as right lung only vs left lung only. 
# * There are more patients reported as left and right separately, than those reported as ‘both lungs’. 
# 
# ### 2.h - GGO number 
# Visualization:
# * A Venn diagram of patients with each category ever reported. 
# 
# Finding:
# * Most patients are reported to have multiple GGOs. 

# %%
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_number'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g = venn.venn(data_)

# %% [markdown]
# ### 2.h - GGO shape margin
# Visualization:
# * A Venn diagram of patients with each category ever reported  
# 
# Findings:  
# * A vast majority of the patients are only reported as have irregular shape of GGO margins.  
# * Lobulated shape is rarely reported. 

# %%
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_shape_margin'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g= venn.venn(data_)

# %% [markdown]
# ### 2.h - GGO term
# Visualization:
# * A Venn diagram of patients with each category ever reported. 
# 
# Findings: 
# * A vast majority of the patients are only reported as have GGO mixed (with a solid part).  
# * Most patients with a pure GGO reported also have been reported as ‘mixed.’ 

# %%
# ggo_term
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_term'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g=venn.venn(data_)

# %% [markdown]
# ### 2.h - GGO solidity change
# Approach: 
# * Only the solidity changes directly extracted from notes are included in this report. 
# 
# Visualization: 
# * A Venn diagram of number of patients with each category ever reported. 
# 
# Findings: 
# * GGO solidity changes are rarely reported explicitly in the radiology notes. 
# * If reported, it is usually reported as increased solidity.

# %%
# ggo_solidity
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_solidity'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g=venn.venn(data_)

# %% [markdown]
# ### 2.h - GGO status change
# Visualization:   
# * A Venn diagram of number of patients with each category ever reported. 

# %%
# ggo_status_change
data = pd.read_sql("""
    select d_person_id, GGO_level2
    , count(*)
    from notes_ggo
    where GGO_level1='GGO_status_change'
    group by d_person_id, GGO_level2
    """, connection)
#data
grouped = data.groupby('GGO_level2')
data_ = {g: set(x['d_person_id']) for g, x in grouped}
g=venn.venn(data_)

# %% [markdown]
# * Visualize the temporality (sequence of status changes) as Sankey diagram. Note that 
#     * The most severe status change (see order below) is picked if more than one reported in a day. 
#     * Only the patients with more than one status change reports are plotted. 
#     * For each patient with more than ten status change reports, only the first ten status change reports are plotted. 

# %%
# sankey diagram
_status_change_order = pd.DataFrame(list(enumerate(['resolved/disappeared',
                       'decreased/improved/reduced/shrink',
                       'stable/no change/persistent',
                       'increased/progressed']))
                        , columns=['GGO_level2_order', 'GGO_level2'])
#_status_change_order.to_sql('_status_change_order', connection)
_status_change_order


# %%
df = pd.read_sql("""
with pick as (   
    select d_person_id, note_date
    , max(GGO_level2_order) GGO_level2_order
    from notes_ggo
    join _status_change_order using (GGO_level2)
    where GGO_level1='GGO_status_change' and GGO_level2 is not null and note_date is not null
    group by d_person_id, note_date
), raw as (
    select d_person_id, note_date, GGO_level2
    from pick
    join _status_change_order using (GGO_level2_order)
), first_ten as (
    select d_person_id person_id
    , substring(GGO_level2, 1, 3) cat
    , rnk
    from (select *, row_number() over (
            partition by d_person_id
            order by note_date) rnk
        from raw
        ) tmp
    where rnk < 10+1
)
select * 
from first_ten
order by person_id, rnk, cat
""", connection)
#df.to_csv('test_sankey.csv')


# %%
#!pip install psutil
#import psutil
from sankey import sankey_from_person_cat_rnk
g = sankey_from_person_cat_rnk(df)
g.write_html('status_change_first_10.html')
#g.write_image('status_change_first_10.png')


# %%
from IPython.display import HTML
#HTML(filename='status_change_first_10.html')

# %% [markdown]
# <!-- * a saved static version -->
# ![status_change_first_10.png](attachment:status_change_first_10.png)
# %% [markdown]
# Findings: 
# * Most patients have a stable status change reported, for many of them stable is the only status ever reported (see the Venn diagram) 
# * For the patients reported as stable (sta), the next report is usually stable again, followed by increased (inc).  See the Sankey diagram. 
#   
# ### 2.i. Can pre-malignancy (AAH/AIS) also be curated from pathology report for the GGO cohort instead of just the radiology report?  
# Answer:
# * Pre-malignancy data (i.e. AAH/AIS) are available in billing data and pathology/other notes. We have premalignancy conditions extracted in GGO reports for this project. 
# %% [markdown]
# ### 2.j. Identification of a persistent GGO cohort
# Approach: 
# * Patients with multiple GGO reports, except for those with the last status changed reported as resolved. 
# * Or patients with only one GGO report, but reported with a worsened status (number, size or solidity). 
# 
# Results: 
# * The persistent cohort can be found as a view: v_s4_persistent_cohort. 
# * For convenience, the persistent info can also be found in the view for note summary: v_s4_notes_ggo_summary. 

# %%
query = """
select d_person_id
, date_count total_reports
-- , radiology_reports 
, cohort
from notes_ggo
join (
    select d_person_id, count(distinct note_date) as date_count, max(note_date) as note_date
    from notes_ggo
    group by d_person_id
) _date_sum using (d_person_id, note_date)
join v_s4_lca_cohort using d_person_id
order by d_person_id
;"""


# %%
query = """
select d_person_id, note_date latest_date, date_count, ggo_level1, ggo_level2
from notes_ggo
join (
    select d_person_id, count(distinct note_date) as date_count, max(note_date) as note_date
    from notes_ggo
    group by d_person_id
) _date_sum using (d_person_id, note_date)
order by d_person_id
;
"""
# pd.read_sql(query, connection)
tmp = connection.execute(f"""create or replace view _person_summary as {query}""")


# %%
query = """
select *
from (
    SELECT d_person_id
    from _person_summary
    where date_count >1
    and d_person_id not in (
        SELECT d_person_id
        from _person_summary
        where date_count >1 -- and not with resolved in the last day report
        and ggo_level1 = 'GGO_status_change' and ggo_level2 = 'resolved/disappeared'
    )
) as muti_not_resolved
union
select *
from (
    SELECT d_person_id
	from _person_summary
    where date_count=1 -- and with any stable/worsen reported
        and (
            (ggo_level1 = 'GGO_status_change' and ggo_level2 in ('stable/no change/persistent', 'increased/progressed'))
            or ( ggo_level1 = 'GGO_solidity' and ggo_level2 in ('stable','increased'))
        )
) as single_changed
;
"""
# pd.read_sql(query, connection)
tmp = connection.execute(f"""create or replace view v_s4_persistent_cohort as {query}""")

# %% [markdown]
# ## v_s4_notes_ggo_summary: a report level summary of GGO.
# * D_patient_id: patient identifier 
# * Note_date: the deidentified note date 
# * Term_types: the types of GGO_terms (pure, mixed) 
# * Max_size: the maximal size (in mm) of the GGOs in the report 
# * Locations: the body locations of the reported GGOs 
# * potential_causes: the potential cause category 
# * Status_changes: the GGO status change categorized (increase, decrease, stable, resolved, …) 
# * Shape_margins: the shape of GGO margins (round, irregular, …) 
# * Solidity_changes: the solidity change category if available 
# * Numbers: the number of GGO as (single, multiple) 
# * is_persistent: 'YES' if the patient has persistent GGO 

# %%
query = """select *
from (select distinct d_person_id, note_date from notes_ggo
) as note_info
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as term_types
    from notes_ggo
    where ggo_level1='GGO_term'
    group by d_person_id, note_date
) as term using (d_person_id, note_date)
left join (select d_person_id, note_date
    , max(cast(ggo_level3 as decimal)) as max_size
    from notes_ggo
    where ggo_level1='GGO_size'
    group by d_person_id, note_date
) as size using (d_person_id, note_date)
left join  (select d_person_id, note_date
    , group_concat(distinct concat(COALESCE(ggo_level2, '_'), '::', COALESCE(ggo_level3, '_')) 
        order by concat(COALESCE(ggo_level2, '_'), '::', COALESCE(ggo_level3, '_')) separator ', ') as locations
    from notes_ggo
    where ggo_level1='GGO_location'
    group by d_person_id, note_date
) as loca using (d_person_id, note_date)
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as potential_causes
    from notes_ggo
    where ggo_level1='GGO_potential_cause'
    group by d_person_id, note_date
) as caus using (d_person_id, note_date)
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as shape_margins
    from notes_ggo
    where ggo_level1='GGO_shape_margin'
    group by d_person_id, note_date
) as shap using (d_person_id, note_date)
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as solidity_changes
    from notes_ggo
    where ggo_level1='GGO_solidity'
    group by d_person_id, note_date
) as soli using (d_person_id, note_date)
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as numbers
    from notes_ggo
    where ggo_level1='GGO_number'
    group by d_person_id, note_date
) as num using (d_person_id, note_date)
left join (select d_person_id, note_date
    , group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as status_changes
    from notes_ggo
    where ggo_level1='GGO_status_change'
    group by d_person_id, note_date
) as status_change using (d_person_id, note_date)
left join (select d_person_id, 'Yes' as is_persistent
    from v_s4_persistent_cohort) persistent using (d_person_id)
order by d_person_id, note_date
"""
#pd.read_sql(query, connection)
g = connection.execute(text(f"""create or replace view v_s4_notes_ggo_summary as {query}"""))


# %%
#check
pd.read_sql(f"""select * from v_s4_notes_ggo_summary""", connection)


# %%
connection.close()
"""
#jupyter nbconvert ../note_ggo_views.ipynb --to html --no-input
#html edit: add 
.jp-Cell-outputWrapper { margin-left: 100px; }
just before the last </style>
# change the first img to width=500px
"""


# %%



