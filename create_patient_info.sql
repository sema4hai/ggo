
-- demographics
create or replace view _cohort_ggo_before_dx_demo as
with raw as (
	select d_person_id, year_of_birth, date_of_death
	, gender_name
	, ifnull(race_name, 'Unknown') race_name
	, ifnull(ethnicity_name, 'Unknown') ethnicity_name
	from _cohort_ggo_before_dx cgbd 
	join people using (d_person_id)
	left join genders using (gender_id)
	left join races using (race_id)
	left join ethnicities using (ethnicity_id)
)
-- select count(*) records, count(distinct d_person_id) patients from raw;
	-- 1706
select * from raw;
/*  
select count(*) from (
	-- select distinct d_person_id, race_name -- ok
	select distinct d_person_id, ethnicity_name -- ok
	-- select distinct d_person_id, gender_name -- ok
	from raw) tmp
;
select gender_name, race_name, ethnicity_name
, count(*)
from _cohort_ggo_before_dx_demo
group by gender_name, race_name, ethnicity_name
order by gender_name, race_name, ethnicity_name
;
*/

create table mapping_ethnicity_race as
select distinct ethnicity_name, race_name,
case when ethnicity_name = 'Hispanic or Latino'
	then 'hispanic'
	when race_name = 'Asian'
	then 'asian'
	when race_name = 'White'
	then 'white'
	when race_name = 'Black or African American'
	then 'aa'
	when race_name in ('Other', 'American Indian or Alaska Native', 'Native Hawaiian or Other Pacific Islander')
	then 'other'
	when race_name = 'Unknown'
	then 'unknown'
end as race_generic
from _cohort_ggo_before_dx_demo
order by ethnicity_name;

create or replace view cohort_ggo_before_dx_demo as
select d.*, race_generic race_ethnicity
from _cohort_ggo_before_dx_demo d
left join mapping_ethnicity_race using (race_name, ethnicity_name)
;
/*
select race_ethnicity, count(*), count(distinct d_person_id)
from cohort_ggo_before_dx_demo
group by race_ethnicity
;
 */
create or replace view _cohort_ggo_before_dx_smoking as
with raw as (
	select *
	from _cohort_ggo_before_dx
	left join person_smoking_statuses using (d_person_id)
	left join smoking_statuses using (smoking_status_id)
)
, win as (
	select *
		, row_number() over (
			partition by d_person_id
			order by date_of_record desc) rownum
		from raw
)
, latest as (
	select *
	from win
	where rownum=1
)
select * from latest;
/*
-- select count(*) records, count(distinct d_person_id) patients from raw
	-- 3010, 1706
	-- 1579, 855
select count(*) records, count(distinct d_person_id) patients from latest
	-- 855
;
*/
create or replace view cohort_ggo_before_dx_smoking as
select *
, case smoking_status_name 
	when 'Never Smoker' then 'Never Smoker'
	when 'Passive Smoker' then 'Never Smoker'
	when 'Former Smoker' then 'Former Smoker'
	when 'Smoker' then 'Current Smoker'
	else 'Unknown' -- Unknown, Not reported, NULL
	end as smoking_cat
from _cohort_ggo_before_dx_smoking
;
/*
select smoking_status_name, count(*)
from _cohort_ggo_before_dx_smoking
group by smoking_status_name
;
select smoking_cat, count(*)
from cohort_ggo_before_dx_smoking
group by smoking_cat
;
select count(*) from (
	select distinct d_person_id, race_name -- ok
	-- select distinct d_person_id, ethnicity_name -- ok
	from raw) tmp;
;
select count(*), count(distinct d_person_id)
from baseline_ex_location_uml;
*/

-- drop view baseline_ex_location_patient_info;
create table baseline_ex_location_patient_info as
select * 
, ggo_date/365.25 age
, case when ggo_date/365.25 <= 60 then '<=60'
	else '60+'
	end age_group
from baseline_ex_location_uml
left join cohort_ggo_before_dx_demo using (d_person_id)
left join cohort_ggo_before_dx_smoking using (d_person_id)
;
/*
 select count(*), count(distinct d_person_id)
from baseline_ex_location_patient_info;
*/