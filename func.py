
case_days = """case when days_ggo_before_dx<180 then '<6M'
            when days_ggo_before_dx<365 then '6M-1y'
            when days_ggo_before_dx<365*3 then '1-3y'
            else '>=3y'
            end """

def qc_base(base):
    res = pd.read_sql(text(f"""
        {base}
        select count(*) records
        , count(distinct concat(d_person_id, ':',  ggo_date)) days
        , count(distinct d_person_id) patients
        from result -- 1049
        """), connection)
    print (res)

#using case_days, working_dir
def run_to_table(table_name, base):
    # create the summary table
    connection.execute(f"""drop table if exists {tablename}""")
    connection.execute(text(f"""create table {tablename} as
        {base}
        , raw as (
            select *, dx_date-ggo_date days_ggo_before_dx
            from result
            join _lca_dx using (d_person_id)
            where dx_date>=ggo_date
        )
        select *
        , {case_days} as date_cat
        from raw
        """)) #, connection)
    df = pd.read_sql(f'select * from {tablename}', connection)
    return df
def run_to_csv(table_name, base):
    df = run_to_table(table_name, base)
    df.to_csv(f'{working_dir}/{tablename}.csv', index=False)

def make_patient_info_from_db(connection, cohort='cohort'):
    sql =