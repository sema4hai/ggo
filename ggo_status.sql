/* summarize ggo status
input: notes_ggo
output: _ggo_status, ggo_status_summary, cat or mix summary
*/
create table _ggo_status as
with raw as (
    select d_person_id, note_date, ggo_level2 cat
    from notes_ggo ng 
    where ggo_level1='GGO_status_change'
    order by d_person_id, note_date
)
, ord as (select 'stable/no change/persistent' cat, 2 rnk
    union all select 'increased/progressed', 1
    union all select 'decreased/improved/reduced/shrink', 3
    union all select 'resolved/disappeared', 4
)
select d_person_id, note_date, cat
from (select *, row_number() over (
		partition by d_person_id, note_date 
		order by rnk) rownum
	from raw
	join ord using (cat)) tmp
where rownum=1
;

create view ggo_status_summary as
with raw as (
    select *, row_number() over (
            partition by d_person_id
            order by note_date) idx
    from _ggo_status
)
, cat_pair as (
    select l.d_person_id, l.cat as cat, ifnull(r.cat, 'end') as cat_next
    from raw l 
    left join raw r on r.d_person_id=l.d_person_id and r.idx=l.idx+1
)
, cat_pc as (
    select cat, cat_next
    , count(distinct d_person_id) patients
    from cat_pair
    group by cat, cat_next
)
, cat_sum as (
    select cat, sum(patients) subtotal
    from cat_pc
    group by cat
)
select *, patients/subtotal perc
from cat_pc
left join cat_sum using (cat)
;

-- for each cat or mix (dec, res as one - better), count patients
with catrnk as (
    select 'stable/no change/persistent' cat, 2 rnk
    union all select 'increased/progressed', 1
    union all select 'decreased/improved/reduced/shrink', 3
    union all select 'resolved/disappeared', 3
)
, prnk as (
    select d_person_id
    , group_concat(distinct rnk) rnks
    from catrnk
    join _ggo_status using(cat)
    group by d_person_id
)
-- select count(*) from prnk; --2337
select cat
, count(*) patients
from prnk
left join catrnk on cast(rnk as char)=rnks
group by cat
;