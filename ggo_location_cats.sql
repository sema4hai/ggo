with cat as (select distinct GGO_level2 cat
   from notes_ggo
   where ggo_level1='GGO_location'
)
, pcat as (
    select d_person_id
    , group_concat(distinct ggo_level2) cats
    from notes_ggo       
    where ggo_level1='GGO_location'
    group by d_person_id
)
select count(*) from pcat;
select ifnull(cat, 'more than one cats') cat
, count(*) patients
from pcat
left join cat on cats=cat
group by cat
;
/*but more than one cats most likely to be bilateral, so combined with it in the final table
*/