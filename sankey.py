import os, numpy as np, pandas as pd
from pandasql import sqldf
import plotly.graph_objects as go

def sankey_from_person_cat_rnk(df):
    """return a sankey fig from a df with (person_id, cat, rnk, ..) preordered.
    
    - person_id: the identifier for a person, in which a flow goes.
    - cat: the category name of a treatment/status
    - rnk: the order the flow following, ascending.

    Note that:
    * the person_id with only one rnk will be excluded
    """
    label = [f'{c}.{r}' for c, r in set(zip(df.cat, df.rnk))]

    # make src and tgt
    src_tgt = sqldf('''
        select src.person_id, src.cat src_cat, src.rnk src_rnk
        , tgt.cat tgt_cat, tgt.rnk tgt_rnk
        from df as src
        join df as tgt on src.person_id=tgt.person_id and src.rnk+1=tgt.rnk
        order by src.person_id, src_rnk, tgt_rnk
        ''', locals())
    # add value: number of persons with this flow
    src_tgt_value = sqldf('''
        select src_cat, src_rnk, tgt_cat, tgt_rnk
        , count(*) value
        from src_tgt
        group by src_cat, src_rnk, tgt_cat, tgt_rnk
        ''', locals())


    label_lookup = {k: i for i, k in enumerate(label)}
    source = [label_lookup[f'{cat}.{rnk}'] for cat, rnk in src_tgt_value[['src_cat', 'src_rnk']].values]
    target = [label_lookup[f'{cat}.{rnk}'] for cat, rnk in src_tgt_value[['tgt_cat', 'tgt_rnk']].values]
    value = src_tgt_value['value']
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = label,
          color = "blue"
        ),
        link = dict(
          source = source,
          target = target,
          value = value
      ))])
    return fig


### not tested: simplified version, regardless of order
def sankey_from_src_tgt(src_tgt):
    src_tgt_value = sqldf.run('''
        select src_cat, tgt_cat
        , count(*) value
        from src_tgt
        group by src_cat, tgt_cat
        ''')
    src_tgt_value.head()

    import plotly.graph_objects as go
    label = list(set(src_tgt_value['src_cat']).union(src_tgt_value['tgt_cat']))
    label_lookup = {k: i for i, k in enumerate(label)}
    source = [label_lookup[cat] for cat in src_tgt_value['src_cat'].values]
    target = [label_lookup[cat] for cat in src_tgt_value['tgt_cat'].values]
    value = src_tgt_value['value']
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = label,
          color = "blue"
        ),
        link = dict(
          source = source,
          target = target,
          value = value
      ))])
    return fig
#fig.show()

# not tested
def _sankey_fig(source_names, target_names, values):
    """build a go figure using src, trt and values.
    """
    value = values
    label = list(set(source_names) | set(target_names))
    label_lookup = {s: i for i, s in enumerate(label)}
    source = [label_lookup[s] for s in source_names]
    target = [label_lookup[s] for s in target_names]
    
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = label,
          color = "blue"
        ),
        link = dict(
          source = source,
          target = target,
          value = value
      ))])
    return fig

def _test_sankey_fig():
    fig = sankey_fig(['a', 'b', 'b'], ['b', 'a', 'b'], [1, 2, 3])
    fig.show()

def test_sankey_from_person_cat_ordered():
    # read and check the input
    df = pd.read_csv('test_sankey.csv')
    fig = sankey_from_person_cat_rnk(df)
    fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
    fig.show()

