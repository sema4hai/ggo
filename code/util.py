def left_join_person_date(alias, level1, agg):
    return f"""\
left join (
    select d_person_id, note_date
    , {agg}
    from notes_ggo
    where ggo_level1='{level1}'
    group by d_person_id, note_date
) as {alias} using (d_person_id, note_date)"""

def test_left_join_person_data_single():
    res = left_join_person_date('term', 'GGO_term',
          agg="""group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as term_types""")
    print(res)

def test_left_join_person_date_multiple():
    sql = f"""select *
from (select distinct d_person_id, note_date from notes_ggo) as note_info
{left_join_person_date('term', 'GGO_term',
    agg="group_concat(distinct ggo_level2 order by ggo_level2 separator ', ') as term_types")}
{left_join_person_date('size', 'GGO_size',
    agg="max(cast(ggo_level3 as decimal)) as max_size")}
left join (
    select d_person_id, 'Yes' as is_persistent
    from v_s4_persistent_cohort
) as persistent using (d_person_id)
order by d_person_id, note_date
"""
    print (sql)
