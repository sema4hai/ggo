/* calc patient numbers for each size_cat and those with more than one cats
input: notes_ggo
*/
   with catrnk as (
   	select '<6mm(0.6cm)' cat, 1 rnk union
	select '>20mm(2cm)', 3 union
	select '6-20mm(0.6-2cm)', 2
	)
	, daycat as (
        select d_person_id, note_date ggo_date
        , max(rnk) rnk
        -- from _cohort_ggo_before_dx
        from notes_ggo
        join catrnk on cat=GGO_level2
        where ggo_level1='GGO_size'
        group by d_person_id, ggo_date
    )
   	, prnks as (
   	select d_person_id
   	, group_concat(distinct rnk) rnks
   	from daycat
   	group by d_person_id
   	)
   	, pcat as (
   	select d_person_id, cat
   	from prnks 
   	left join catrnk on rnks=cast(rnk as char)
   	)
   	select cat, count(*) patients
   	from pcat
   	group by cat
   	;