#!/usr/bin/env python
# coding: utf-8

# In[34]:


import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns 
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
import plotly.express as px


# In[28]:


con = duckdb.connect("health_enforcement.duckdb")
df_mart = con.execute("SELECT * FROM enriched_mart").df()


# In[29]:


df_counts = df_mart['PENALTY_TYPE'].value_counts().reset_index()
df_counts.columns = ['PENALTY_TYPE', 'count']

df_counts['percent'] = df_counts['count'] / df_counts['count'].sum() * 100

fig = px.bar(
    df_counts,
    x='PENALTY_TYPE',
    y='count',
    title='Penalty Type Distribution',
    hover_data={'percent': ':.2f'}  
)

fig.show()


# In[39]:


import pandas as pd
import plotly.express as px

heat = pd.crosstab(
    df_mart['DISTRICT_OFFICE'],
    df_mart['PENALTY_TYPE']
)

# convert to percent within penalty type
heat_pct = heat.div(heat.sum(axis=0), axis=1) * 100

# focus on top locations
heat_pct = heat_pct.loc[heat_pct.sum(axis=1).nlargest(20).index]

fig = px.imshow(
    heat_pct,
    text_auto=".1f",
    color_continuous_scale="Reds",
    title="Penalty Type Distribution by Location (%)"
)

fig.update_layout(
    height=700,
    width=900,
    coloraxis_colorbar_title="% of Category"
)

fig.show()


# In[87]:


import plotly.express as px
import pandas as pd

# ── Coordinates ───────────────────────────────────────────────────────────────
district_coords = {
    'Santa Rosa'               : (38.4404,  -122.7141),
    'East Bay'                 : (37.8044,  -122.2712),
    'Sacramento'               : (38.5816,  -121.4944),
    'Stockton'                 : (37.9577,  -121.2908),
    'Fresno'                   : (36.7378,  -119.7871),
    'Bakersfield'              : (35.3733,  -119.0187),
    'Ventura'                  : (34.2805,  -119.2945),
    'Orange'                   : (33.7879,  -117.8531),
    'San Jose'                 : (37.3382,  -121.8863),
    'San Francisco'            : (37.7749,  -122.4194),
    'San Diego'                : (32.7157,  -117.1611),
    'San Bernardino'           : (34.1083,  -117.2898),
    'LA ICF/DD/Clinics'        : (34.0195,  -118.5500),
    'State Facilities Section' : (38.5816,  -121.4944),
    'Chico'                    : (39.7285,  -121.8375),
    'Riverside'                : (33.9806,  -117.3755),
    'LA Region 3'              : (33.8500,  -118.3500),
    'LA Region 2'              : (34.0700,  -118.5500),
    'LA Region 1'              : (34.1000,  -118.1500),
    'LA HHA/Hospice'           : (34.0000,  -118.2000),
    'LA Acute/Ancillary'       : (33.9500,  -118.3000),
}

# ── Aggregate ─────────────────────────────────────────────────────────────────
district_stats = (df_mart
    .groupby('DISTRICT_OFFICE')
    .agg(
        total_actions     = ('PENALTY_NUMBER', 'count'),
        serious_citations = ('CLASS_FINAL', lambda x: x.isin(['AA', 'AP IJ', 'A']).sum()),
        death_related     = ('DEATH_RELATED', 'sum'),
        total_balance     = ('TOTAL_BALANCE_DUE', 'sum'),
    )
    .reset_index()
)

district_stats['lat'] = district_stats['DISTRICT_OFFICE'].map(
    lambda x: district_coords.get(x, (None, None))[0])
district_stats['lon'] = district_stats['DISTRICT_OFFICE'].map(
    lambda x: district_coords.get(x, (None, None))[1])

total_all = district_stats['total_actions'].sum()
serious_all = district_stats['serious_citations'].sum()

# size = share of ALL citations statewide (0-100%)
district_stats['citation_share_pct'] = (
    district_stats['total_actions'] / total_all * 100
).round(1)

# color = share of ALL serious citations statewide (0-100%)
district_stats['serious_share_pct'] = (
    district_stats['serious_citations'] / serious_all * 100
).round(1)

# scale size so differences are obvious — square root dampens but keeps contrast
import numpy as np
district_stats['size_scaled'] = np.sqrt(district_stats['citation_share_pct']) ** 2.5

district_stats['serious_rate'] = (
    district_stats['serious_citations'] / district_stats['total_actions'] * 100
).round(1)

# ── Map ───────────────────────────────────────────────────────────────────────
fig = px.scatter_mapbox(
    district_stats.dropna(subset=['lat', 'lon']),
    lat='lat',
    lon='lon',
    size='size_scaled',
    color='serious_share_pct',
    hover_name='DISTRICT_OFFICE',
    hover_data={
        'total_actions'       : True,
        'citation_share_pct'  : True,
        'serious_citations'   : True,
        'serious_share_pct'   : True,
        'death_related'       : True,
        'serious_rate'        : True,
        'size_scaled'         : False,
        'lat'                 : False,
        'lon'                 : False,
    },
    color_continuous_scale=[
        [0.0,  '#E2E8F0'],
        [0.15, '#0D9488'],
        [1.0,  '#0F1F3D'],
    ],
    size_max=25,
    zoom=5.2,
    center=dict(lat=36.7783, lon=-119.8),
    mapbox_style='carto-positron',
    title='Share of Statewide Citations & Serious Violations by District',
    labels={
        'total_actions'      : 'Total Actions',
        'citation_share_pct' : '% of All Citations',
        'serious_citations'  : 'Serious Citations',
        'serious_share_pct'  : '% of All Serious Citations',
        'death_related'      : 'Death-Related',
        'serious_rate'       : 'Serious Rate (%)',
    }
)

fig.update_layout(
    font=dict(family='Calibri', color='#0F1F3D'),  # global font
    font_family='Calibri',
    paper_bgcolor='#F8FAFC',
    title_font_size=18,
    title_font_color='#0F1F3D',
    coloraxis_colorbar=dict(
        title='% of All<br>Serious Citations',
        thickness=12,
        len=0.5,
        tickfont=dict(family='Calibri', color='#64748B'),
        title_font=dict(family='Calibri', color='#0F1F3D'),
    ),
    margin=dict(l=0, r=0, t=50, b=0),
    height=650,
)

fig.show()


# In[105]:


import plotly.graph_objects as go

flow = (df_mart
    .groupby(['CLASS_ASSESSED_INITIAL', 'CLASS_FINAL'])
    .size()
    .reset_index(name='count')
)

flow = flow[flow['CLASS_ASSESSED_INITIAL'] != flow['CLASS_FINAL']]

top_classes = ['AA', 'A', 'B', 'AP IJ', 'AP NON-IJ', 'AP NHPPD', 'FTR AE', 'FTR BR']
flow = flow[flow['CLASS_ASSESSED_INITIAL'].isin(top_classes)]

flow = flow[flow['count'] >= 3]

initial_nodes = [f"{c} (Initial)" for c in flow['CLASS_ASSESSED_INITIAL'].unique()]
final_nodes   = flow['CLASS_FINAL'].unique().tolist()
all_nodes     = initial_nodes + final_nodes
node_idx      = {n: i for i, n in enumerate(all_nodes)}

sources, targets, values, colors = [], [], [], []

for _, row in flow.iterrows():
    src = f"{row['CLASS_ASSESSED_INITIAL']} (Initial)"
    tgt = row['CLASS_FINAL']
    sources.append(node_idx[src])
    targets.append(node_idx[tgt])
    values.append(row['count'])

    if tgt in ['Dismissed by court', 'REVERSED', 'APPEAL WITHDRAWN BY FACILITY']:
        colors.append('rgba(220, 38, 38, 0.3)')
    elif tgt == 'Drop>Deficiency':
        colors.append('rgba(217, 119, 6, 0.3)')
    else:
        colors.append('rgba(100, 116, 139, 0.2)')

def node_color(n):
    if 'AA' in n or 'AP IJ' in n: return '#DC2626'
    if '(Initial)' in n:          return '#0D9488'
    if n in ['Dismissed by court', 'REVERSED', 'APPEAL WITHDRAWN BY FACILITY']: return '#DC2626'
    if n == 'Drop>Deficiency':     return '#D97706'
    return '#64748B'

fig1 = go.Figure(go.Sankey(
    arrangement='snap',
    node=dict(
        pad=20, thickness=20,
        line=dict(color='white', width=0.5),
        label=all_nodes,
        color=[node_color(n) for n in all_nodes],
        hovertemplate='%{label}<br>Count: %{value:,}<extra></extra>',
    ),
    link=dict(
        source=sources, target=targets,
        value=values, color=colors,
        hovertemplate='%{source.label} → %{target.label}<br>%{value:,} cases<extra></extra>',
    )
))

fig1.update_layout(
    font=dict(family='Calibri', size=12, color='#0F1F3D'),
    title=dict(
        text='Appeal Results',
        font=dict(size=18, color='#0F1F3D', family='Calibri'),
    ),
    paper_bgcolor='#F8FAFC',
    margin=dict(l=20, r=20, t=70, b=20),
    height=600,
)


fig1.show()


# # In[99]:


# # for each facility, sort by penalty date and compute gap between consecutive events
# inspection_gaps = (df_mart
#     .groupby(['FACID', 'EVENTID'])['PENALTY_ISSUE_DATE']
#     .min()  # first penalty date per event = proxy for inspection date
#     .reset_index()
#     .sort_values(['FACID', 'PENALTY_ISSUE_DATE'])
# )

# inspection_gaps['days_to_next'] = (
#     inspection_gaps.groupby('FACID')['PENALTY_ISSUE_DATE']
#     .diff()
#     .dt.days
# )

# # average gap per facility then average across all
# avg_gap = inspection_gaps['days_to_next'].dropna()

# print(f"Mean days between inspections:   {avg_gap.mean():.0f}")
# print(f"Median days between inspections: {avg_gap.median():.0f}")

# # breakdown by LTC vs hospital
# inspection_gaps = inspection_gaps.merge(
#     df_mart[['FACID', 'LTC']].drop_duplicates(), on='FACID', how='left'
# )

# print(inspection_gaps.groupby('LTC')['days_to_next'].median())


# In[18]:


import plotly.graph_objects as go
from plotly.subplots import make_subplots

penalty_types = ["Administrative Penalty", "Citation", "Failure to Report Penalty"]

fig = make_subplots(
    rows=1, cols=3,
    subplot_titles=penalty_types,
    horizontal_spacing=0.08
)

for i, ptype in enumerate(penalty_types, start=1):
    subset = df_mart[df_mart['PENALTY_TYPE'] == ptype]

    counts = (
        subset['COMPLAINT_COUNT']
        .value_counts()
        .sort_index()
    )

    fig.add_trace(
        go.Bar(
            x=counts.values,
            y=counts.index.astype(str),
            orientation='h',
            text=[f'{v} ({v/counts.sum()*100:.1f}%)' for v in counts.values],
            textposition='outside',
            showlegend=False
        ),
        row=1, col=i
    )

fig.update_layout(
    title='Complaint Count Distribution by Penalty Type',
    template='plotly_white',
    height=650,
    width=1500
)

# allow different scales for each subplot
fig.update_xaxes(matches=None, title_text='Count')

fig.update_yaxes(title_text='Complaint Count')

fig.show()


# In[19]:


penalty_types = ["Administrative Penalty", "Citation", "Failure to Report Penalty"]

fig = make_subplots(
    rows=1, cols=3,
    subplot_titles=penalty_types,
    horizontal_spacing=0.12
)

for i, ptype in enumerate(penalty_types, start=1):
    subset = df_mart[df_mart['PENALTY_TYPE'] == ptype]
    counts = subset['CLASS_FINAL'].value_counts().sort_values()  # ascending for horizontal

    fig.add_trace(
        go.Bar(
            x=counts.values,
            y=counts.index,
            orientation='h',
            text=[f'{v} ({v/counts.sum()*100:.1f}%)' for v in counts.values],
            textposition='outside',
            name=ptype,
            showlegend=False
        ),
        row=1, col=i
    )

fig.update_layout(
    title='Class Assessed Final by Penalty Type',
    height=500,
    width=1400,
    template='plotly_white'
)

fig.update_xaxes(title_text='Count')

fig.show()


# In[ ]:





# In[49]:


import plotly.express as px

# total cases per penalty type
totals = df_mart.groupby('PENALTY_TYPE').size()

# appealed cases per penalty type
appealed = df_mart[df_mart['APPEALED'] == 1].groupby('PENALTY_TYPE').size()

# combine
appeal_df = (
    totals.to_frame('total')
    .join(appealed.to_frame('appealed'))
    .fillna(0)
    .reset_index()
)

# appeal rate
appeal_df['appeal_rate'] = appeal_df['appealed'] / appeal_df['total'] * 100

fig = px.bar(
    appeal_df,
    x='PENALTY_TYPE',
    y='appealed',
    title='Appealed Penalties by Penalty Type',
    text=appeal_df.apply(
        lambda r: f"{int(r['appealed'])} ({r['appeal_rate']:.1f}%)",
        axis=1
    )
)

fig.update_traces(textposition='outside')

fig.update_layout(
    template='plotly_white',
    yaxis_title='Appealed Count',
    xaxis_title='Penalty Type'
)

fig.show()


# In[21]:


df_mart.columns


# In[22]:


import pandas as pd
import plotly.express as px

# Make sure the violation date is datetime
df_mart['PENALTY_ISSUE_DATE'] = pd.to_datetime(df_mart['PENALTY_ISSUE_DATE'], errors='coerce')

# Drop rows where the date is missing
df_time = df_mart.dropna(subset=['PENALTY_ISSUE_DATE']).copy()

# Extract year
df_time['Year'] = df_time['PENALTY_ISSUE_DATE'].dt.year

# Count violations per year
violations_per_year = df_time.groupby('Year').size().reset_index(name='Violation_Count')


# In[23]:


fig = px.bar(
    violations_per_year,
    x='Year',
    y='Violation_Count',
    title='Number of Violations per Year (Based on Violation From Date)',
    labels={'Violation_Count': 'Number of Violations', 'Year': 'Year'}
)

fig.update_layout(template='plotly_white')
fig.show()


# In[13]:


# df_mart[['APPEALED','TOTAL_AMOUNT_INITIAL', 'TOTAL_AMOUNT_DUE_FINAL', 'TOTAL_BALANCE_DUE',
#        'TOTAL_COLLECTED_AMOUNT']].describe()


# In[12]:


import plotly.express as px
import numpy as np

df_balance = df_mart[df_mart["TOTAL_BALANCE_DUE"] > 0]["TOTAL_BALANCE_DUE"]

# Compute log10 for better scaling
log_balance = np.log10(df_balance)

fig = px.histogram(
    log_balance,
    nbins=50,
    title="Total Balance Due Distribution (log scale)",
    labels={"value": "Log10(Total Balance Due)"}
)

fig.update_xaxes(
    tickvals=[1, 2, 3, 4, 5],
    ticktext=['$10', '$100', '$1K', '$10K', '$100K']
)

fig.update_yaxes(title="Frequency")
fig.update_layout(template="plotly_white")

fig.show()


# In[ ]:


df_mart['FAC_TYPE_CODE'].value_counts()


# In[ ]:


penalty_loc = (
    df_mart.groupby(["DISTRICT_OFFICE","PENALTY_TYPE"])
    .size()
    .reset_index(name="count")
)

penalty_loc["percent"] = (
    penalty_loc.groupby("PENALTY_TYPE")["count"]
    .transform(lambda x: x/x.sum()*100)
)


# In[107]:


con.close()





