# -*- coding: utf-8 -*-
# MANUFACTURING ANALYTICS - STEP 5
# INTERACTIVE POWER BI STYLE DASHBOARD (PLOTLY)
#
# Is step mein sikhenge:
#   1. Plotly kya hai - interactive charts
#   2. 6 alag interactive charts banana
#   3. Sab charts ek page pe (Dashboard layout)
#   4. HTML file mein save karna - browser mein khulega
#   5. Hover, zoom, filter - sab interactive features
#
# Install: pip install plotly
# Run    : python step5_dashboard.py
# Output : manufacturing_dashboard.html (browser mein kholo)

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pyodbc
import pandas as pd
import numpy as np
import pickle

# Plotly = interactive charts ke liye
import plotly.graph_objects as go
# graph_objects = low-level control (zyada customization)

import plotly.express as px
# express = quick charts (kam code mein)

from plotly.subplots import make_subplots
# make_subplots = multiple charts ek page pe

print("=" * 60)
print("  STEP 5 - Interactive Power BI Style Dashboard")
print("=" * 60)
print()

# ---------------------------------------------------------------
# Plotly vs Matplotlib - Kya farak hai?
#
#  Matplotlib:
#    - Static images (PNG, JPG)
#    - Zoom/filter nahi kar sakte
#    - Reports aur papers ke liye
#
#  Plotly:
#    - Interactive HTML
#    - Hover karke exact values dekho
#    - Zoom in/out, pan karo
#    - Legend click karke series hide karo
#    - Browser mein khulta hai (share karna easy)
#    - Power BI jaisa feel
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# SECTION A: DATA LOAD
# ---------------------------------------------------------------
SERVER_NAME   = "LAPTOP-LG4BEQ1J\\SQLEXPRESS"
DATABASE_NAME = "FactoryAnalytics"

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=" + SERVER_NAME + ";"
    "Database=" + DATABASE_NAME + ";"
    "Trusted_Connection=yes;",
    autocommit=True
)

df = pd.read_sql_query(
    "SELECT p.*, m.machine_name, m.department, m.age_years, "
    "e.emp_name, e.experience_yrs, "
    "pr.product_name, pr.category, pr.price_per_unit "
    "FROM production p "
    "INNER JOIN machines  m  ON p.machine_id = m.machine_id "
    "INNER JOIN employees e  ON p.emp_id     = e.emp_id "
    "INNER JOIN products  pr ON p.product_id = pr.product_id",
    conn
)

df_sales = pd.read_sql_query(
    "SELECT s.*, pr.product_name, pr.category "
    "FROM sales s INNER JOIN products pr ON s.product_id = pr.product_id",
    conn
)

df_maint = pd.read_sql_query(
    "SELECT ma.*, m.machine_name, m.department "
    "FROM maintenance ma INNER JOIN machines m ON ma.machine_id = m.machine_id",
    conn
)
conn.close()

# Data prepare
df['prod_date']    = pd.to_datetime(df['prod_date'])
df['defect_rate']  = (df['units_defective'] / df['units_made'] * 100).round(2)
df['efficiency']   = ((df['units_made'] - df['units_defective']) / df['units_made'] * 100).round(2)
df['revenue_lost'] = (df['units_defective'] * df['price_per_unit']).round(2)
df['month_label']  = df['prod_date'].dt.strftime('%Y-%m')
df['month_period'] = df['prod_date'].dt.to_period('M')

df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])
df_maint['maint_date']= pd.to_datetime(df_maint['maint_date'])

# Aggregated data
df_machine = df.groupby('machine_name').agg(
    defect_rate  = ('defect_rate',  'mean'),
    avg_downtime = ('downtime_hrs', 'mean'),
    total_units  = ('units_made',   'sum'),
    avg_temp     = ('temperature',  'mean')
).reset_index().round(2).sort_values('defect_rate', ascending=False)

df_monthly = df.groupby('month_label').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    avg_temp      = ('temperature',     'mean')
).reset_index()
df_monthly['defect_pct'] = (df_monthly['total_defects'] / df_monthly['total_units'] * 100).round(2)

df_shift = df.groupby('shift').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    avg_downtime  = ('downtime_hrs',    'mean')
).reset_index()
df_shift['defect_rate'] = (df_shift['total_defects'] / df_shift['total_units'] * 100).round(2)

df_dept = df.groupby('department').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    total_downtime= ('downtime_hrs',    'sum'),
    revenue_lost  = ('revenue_lost',    'sum')
).reset_index()
df_dept['defect_rate']   = (df_dept['total_defects'] / df_dept['total_units'] * 100).round(2)
df_dept['total_downtime']= df_dept['total_downtime'].round(1)
df_dept['revenue_lost']  = df_dept['revenue_lost'].round(0)

df_prod_rev = df.groupby('product_name')['revenue_lost'].sum().reset_index()
df_prod_rev = df_prod_rev.sort_values('revenue_lost', ascending=True)

df_sales_monthly = df_sales.groupby(
    df_sales['sale_date'].dt.strftime('%Y-%m')
)['revenue'].sum().reset_index()
df_sales_monthly.columns = ['month', 'revenue']

df_maint_cost = df_maint.groupby('maint_type')['cost'].sum().reset_index()

print("  [OK] Data loaded and prepared!")
print()

# ---------------------------------------------------------------
# SECTION B: ML MODEL LOAD (Step 4 se)
# ---------------------------------------------------------------
try:
    model       = pickle.load(open('defect_prediction_model.pkl', 'rb'))
    encoders    = pickle.load(open('label_encoders.pkl', 'rb'))
    feature_cols= pickle.load(open('feature_cols.pkl', 'rb'))

    # ML predictions add karo main dataframe mein
    le_shift = encoders['shift']
    le_dept  = encoders['dept']
    df['shift_num'] = le_shift.transform(df['shift'])
    df['dept_num']  = le_dept.transform(df['department'])
    df['ml_prediction'] = model.predict(df[feature_cols])
    df['ml_probability'] = model.predict_proba(df[feature_cols])[:, 1]
    print("  [OK] ML model loaded - predictions added!")
except:
    print("  [!] ML model file nahi mila - Step 4 pehle chalao")
    df['ml_prediction']  = 0
    df['ml_probability'] = 0.0
print()

# ---------------------------------------------------------------
# SECTION C: COLOR SCHEME
# Plotly mein colors hex codes ya named colors
# ---------------------------------------------------------------
COLORS = {
    'blue':   '#2E86AB',
    'red':    '#C73E1D',
    'green':  '#3BB273',
    'orange': '#F18F01',
    'purple': '#7B2FBE',
    'gray':   '#888888',
}

MACHINE_COLORS = [
    '#2E86AB','#C73E1D','#3BB273',
    '#F18F01','#7B2FBE','#888888'
]

# ---------------------------------------------------------------
# SECTION D: DASHBOARD LAYOUT BANAO
#
# make_subplots = ek grid mein multiple charts
#   rows=3, cols=3 = 3x3 = 9 slots
#   specs = har slot ka type (xy=normal, pie=pie chart, etc.)
#   subplot_titles = har chart ka title
#
# Dashboard structure:
#   Row 1: KPI Cards (3 metric boxes)
#   Row 2: Monthly Trend | Machine Bar | Shift Donut
#   Row 3: Scatter Plot  | Heatmap     | Revenue Bar
# ---------------------------------------------------------------
print("-" * 60)
print("  DASHBOARD LAYOUT BANANA")
print("-" * 60)

fig = make_subplots(
    rows=3, cols=3,
    subplot_titles=(
        '',                              # Row 1: KPI cards (manual add honge)
        '',
        '',
        'Monthly Production & Defect %', # Row 2
        'Machine Defect Rate (%)',
        'Shift Production Share',
        'Temperature vs Defect Rate',    # Row 3
        'Department Performance',
        'Revenue Lost by Product'
    ),
    specs=[
        # Row 1: 3 indicator (metric) boxes
        [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
        # Row 2: xy chart, xy chart, pie chart
        [{"type": "xy", "secondary_y": True}, {"type": "xy"}, {"type": "pie"}],
        # Row 3: xy charts
        [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
    ],
    vertical_spacing=0.12,
    horizontal_spacing=0.08,
    row_heights=[0.20, 0.40, 0.40]   # Row 1 chhoti, 2 aur 3 badi
)

# ---------------------------------------------------------------
# SECTION E: KPI CARDS (Row 1)
# go.Indicator = ek bada number dikhane ke liye
# ---------------------------------------------------------------
total_units    = int(df['units_made'].sum())
overall_defect = round(df['units_defective'].sum() / df['units_made'].sum() * 100, 2)
total_revenue  = round(df_sales['revenue'].sum(), 0)

# KPI 1: Total Units
fig.add_trace(go.Indicator(
    mode="number+delta",
    # mode = kya dikhana hai: number, delta (change), gauge
    value=total_units,
    title={"text": "Total Units Produced<br><span style='font-size:12px;color:gray'>Jan-Jun 2024</span>"},
    number={"valueformat": ",", "font": {"size": 36, "color": COLORS['blue']}},
    delta={"reference": 900000, "valueformat": ","},
    # delta = target se compare karo
), row=1, col=1)

# KPI 2: Defect Rate
fig.add_trace(go.Indicator(
    mode="number+gauge",
    value=overall_defect,
    title={"text": "Overall Defect Rate<br><span style='font-size:12px;color:gray'>Target: < 3%</span>"},
    number={"suffix": "%", "font": {"size": 36, "color": COLORS['red']}},
    gauge={
        "axis":  {"range": [0, 8]},
        "bar":   {"color": COLORS['red']},
        "steps": [
            {"range": [0, 3], "color": "#d4edda"},   # green = good
            {"range": [3, 5], "color": "#fff3cd"},    # yellow = warning
            {"range": [5, 8], "color": "#f8d7da"},    # red = danger
        ],
        "threshold": {
            "line":  {"color": "darkred", "width": 3},
            "value": 3.0    # target line
        }
    }
), row=1, col=2)

# KPI 3: Total Revenue
fig.add_trace(go.Indicator(
    mode="number",
    value=total_revenue,
    title={"text": "Total Sales Revenue<br><span style='font-size:12px;color:gray'>All Products</span>"},
    number={
        "prefix": "Rs.",
        "valueformat": ",.0f",
        "font": {"size": 30, "color": COLORS['green']}
    }
), row=1, col=3)

print("  [OK] Row 1: KPI Cards")

# ---------------------------------------------------------------
# SECTION F: MONTHLY TREND (Row 2, Col 1)
# Bar + Line combo - dual Y axis
# ---------------------------------------------------------------
# Bar chart - units
fig.add_trace(go.Bar(
    x=df_monthly['month_label'],
    y=df_monthly['total_units'],
    name='Units Made',
    marker_color=COLORS['blue'],
    opacity=0.7,
    hovertemplate='Month: %{x}<br>Units: %{y:,}<extra></extra>'
    # hovertemplate = hover pe kya dikhao
    # %{x} = x value, %{y:,} = y value with comma formatting
    # <extra></extra> = trace name hide karo
), row=2, col=1, secondary_y=False)

# Line chart - defect %
fig.add_trace(go.Scatter(
    x=df_monthly['month_label'],
    y=df_monthly['defect_pct'],
    name='Defect %',
    mode='lines+markers',   # line aur dots dono
    line=dict(color=COLORS['red'], width=2.5),
    marker=dict(size=8, color=COLORS['red']),
    hovertemplate='Month: %{x}<br>Defect: %{y:.2f}%<extra></extra>'
), row=2, col=1, secondary_y=True)

# Y axis labels
fig.update_yaxes(title_text="Units Made", row=2, col=1, secondary_y=False,
                 title_font_color=COLORS['blue'])
fig.update_yaxes(title_text="Defect %",   row=2, col=1, secondary_y=True,
                 title_font_color=COLORS['red'])

print("  [OK] Row 2, Col 1: Monthly Trend")

# ---------------------------------------------------------------
# SECTION G: MACHINE BAR CHART (Row 2, Col 2)
# Color = defect rate pe depend karta hai (conditional coloring)
# ---------------------------------------------------------------
bar_colors = [
    COLORS['red']    if x > df_machine['defect_rate'].mean() + 0.5
    else COLORS['orange'] if x > df_machine['defect_rate'].mean()
    else COLORS['green']
    for x in df_machine['defect_rate']
]
# Red = bahut zyada, Orange = average se upar, Green = acha

fig.add_trace(go.Bar(
    x=df_machine['machine_name'],
    y=df_machine['defect_rate'],
    name='Defect Rate',
    marker_color=bar_colors,
    text=df_machine['defect_rate'].astype(str) + '%',
    textposition='outside',   # bar ke upar text
    hovertemplate=(
        'Machine: %{x}<br>'
        'Defect Rate: %{y:.2f}%<br>'
        'Total Units: %{customdata[0]:,}<br>'
        'Avg Temp: %{customdata[1]:.1f} degC'
        '<extra></extra>'
    ),
    customdata=df_machine[['total_units','avg_temp']].values
    # customdata = extra info hover mein dikhao
), row=2, col=2)

# Average line - go.Scatter use karo (add_hline Indicator subplot se conflict karta hai)
avg_val = round(df_machine['defect_rate'].mean(), 1)
fig.add_trace(go.Scatter(
    x=df_machine['machine_name'],
    y=[avg_val] * len(df_machine),
    mode='lines',
    name='Avg: ' + str(avg_val) + '%',
    line=dict(color='gray', width=1.5, dash='dash'),
    hovertemplate='Average: ' + str(avg_val) + '%<extra></extra>'
), row=2, col=2)

print("  [OK] Row 2, Col 2: Machine Bar Chart")

# ---------------------------------------------------------------
# SECTION H: SHIFT PIE CHART (Row 2, Col 3)
# ---------------------------------------------------------------
fig.add_trace(go.Pie(
    labels=df_shift['shift'],
    values=df_shift['total_units'],
    name='Shift Share',
    hole=0.45,   # hole = donut chart
    marker_colors=[COLORS['orange'], COLORS['blue'], COLORS['purple']],
    hovertemplate=(
        'Shift: %{label}<br>'
        'Units: %{value:,}<br>'
        'Share: %{percent}<br>'
        'Defect Rate: %{customdata:.2f}%'
        '<extra></extra>'
    ),
    customdata=df_shift['defect_rate'],
    textinfo='label+percent'
), row=2, col=3)

print("  [OK] Row 2, Col 3: Shift Pie")

# ---------------------------------------------------------------
# SECTION I: SCATTER PLOT (Row 3, Col 1)
# Color = department, Size = units_made
# ---------------------------------------------------------------
depts = df['department'].unique()
dept_colors = dict(zip(depts, [COLORS['blue'], COLORS['red'],
              COLORS['green'], COLORS['orange'], COLORS['purple']]))

for dept in depts:
    mask = df['department'] == dept
    fig.add_trace(go.Scatter(
        x=df[mask]['temperature'],
        y=df[mask]['defect_rate'],
        mode='markers',
        name=dept,
        marker=dict(
            size=6,
            color=dept_colors.get(dept, COLORS['gray']),
            opacity=0.4
        ),
        hovertemplate=(
            'Dept: ' + dept + '<br>'
            'Temp: %{x:.1f} degC<br>'
            'Defect: %{y:.2f}%<br>'
            'Machine: %{customdata}'
            '<extra></extra>'
        ),
        customdata=df[mask]['machine_name'],
        legendgroup='scatter',
        showlegend=True
    ), row=3, col=1)

# Trend line
z = np.polyfit(df['temperature'], df['defect_rate'], 1)
p = np.poly1d(z)
x_range = np.linspace(df['temperature'].min(), df['temperature'].max(), 100)
fig.add_trace(go.Scatter(
    x=x_range,
    y=p(x_range),
    mode='lines',
    name='Trend',
    line=dict(color='black', width=2, dash='dash'),
    legendgroup='scatter'
), row=3, col=1)

fig.update_xaxes(title_text="Temperature (degC)", row=3, col=1)
fig.update_yaxes(title_text="Defect Rate (%)",    row=3, col=1)
print("  [OK] Row 3, Col 1: Temperature Scatter")

# ---------------------------------------------------------------
# SECTION J: DEPARTMENT BAR (Row 3, Col 2)
# Grouped bar - multiple values ek saath compare
# ---------------------------------------------------------------
fig.add_trace(go.Bar(
    x=df_dept['department'],
    y=df_dept['defect_rate'],
    name='Defect Rate %',
    marker_color=COLORS['red'],
    opacity=0.8,
    hovertemplate='Dept: %{x}<br>Defect: %{y:.2f}%<extra></extra>'
), row=3, col=2)

fig.add_trace(go.Bar(
    x=df_dept['department'],
    y=df_dept['total_downtime'],
    name='Downtime (hrs)',
    marker_color=COLORS['orange'],
    opacity=0.8,
    hovertemplate='Dept: %{x}<br>Downtime: %{y:.1f} hrs<extra></extra>'
), row=3, col=2)

# barmode = 'group' = side by side bars
fig.update_layout(barmode='group')
print("  [OK] Row 3, Col 2: Department Grouped Bar")

# ---------------------------------------------------------------
# SECTION K: REVENUE LOST BAR (Row 3, Col 3)
# ---------------------------------------------------------------
rev_colors = [
    COLORS['red']    if v == df_prod_rev['revenue_lost'].max()
    else COLORS['orange'] if v > df_prod_rev['revenue_lost'].mean()
    else COLORS['blue']
    for v in df_prod_rev['revenue_lost']
]

fig.add_trace(go.Bar(
    y=df_prod_rev['product_name'],
    x=df_prod_rev['revenue_lost'] / 1000,
    orientation='h',   # horizontal bar
    name='Revenue Lost',
    marker_color=rev_colors,
    opacity=0.85,
    hovertemplate='Product: %{y}<br>Revenue Lost: Rs.%{x:,.0f}K<extra></extra>'
), row=3, col=3)

fig.update_xaxes(title_text="Revenue Lost (Rs. thousands)", row=3, col=3)
print("  [OK] Row 3, Col 3: Revenue Lost")

# ---------------------------------------------------------------
# SECTION L: OVERALL DASHBOARD STYLING
# ---------------------------------------------------------------
fig.update_layout(
    title=dict(
        text=(
            "Manufacturing Analytics Dashboard | FactoryAnalytics DB | "
            "Jan-Jun 2024"
        ),
        font=dict(size=18, color='#2c3e50'),
        x=0.5,    # Center
        xanchor='center'
    ),
    height=1050,
    paper_bgcolor='#f8f9fa',    # Dashboard background
    plot_bgcolor='white',        # Chart area background
    font=dict(family='Arial, sans-serif', size=11),
    showlegend=True,
    legend=dict(
        orientation='h',         # Horizontal legend
        yanchor='bottom',
        y=-0.08,
        xanchor='center',
        x=0.5,
        bgcolor='rgba(255,255,255,0.8)',
        bordercolor='lightgray',
        borderwidth=1
    ),
    hoverlabel=dict(
        bgcolor='white',
        font_size=12,
        bordercolor='lightgray'
    )
)

# Gridlines subtle karo
fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='#e0e0e0')
fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='#e0e0e0')

print()
print("-" * 60)
print("  DASHBOARD SAVE KARNA")
print("-" * 60)

# HTML file mein save karo
# include_plotlyjs = 'cdn' = plotly JS CDN se load hoga (file chhoti)
# include_plotlyjs = True  = JS embed karo (file badi, offline bhi chalega)
fig.write_html(
    'manufacturing_dashboard.html',
    include_plotlyjs='cdn',
    full_html=True
)
print()
print("  [OK] manufacturing_dashboard.html saved!")
print()
print("  Browser mein kholne ke liye:")
print("  manufacturing_dashboard.html pe double-click karo")
print()

# ---------------------------------------------------------------
# SECTION M: EXTRA - SALES TREND DASHBOARD (alag file)
# ---------------------------------------------------------------
print("-" * 60)
print("  BONUS: SALES DASHBOARD BANANA")
print("-" * 60)

fig2 = make_subplots(
    rows=2, cols=2,
    subplot_titles=(
        'Monthly Sales Revenue',
        'Revenue by Product',
        'Revenue by Region',
        'Maintenance Cost by Type'
    ),
    specs=[
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "pie"}, {"type": "pie"}],
    ]
)

# Sales trend line
fig2.add_trace(go.Scatter(
    x=df_sales_monthly['month'],
    y=df_sales_monthly['revenue'],
    mode='lines+markers',
    fill='tozeroy',    # Area chart - line ke neeche fill
    name='Revenue',
    line=dict(color=COLORS['green'], width=2),
    marker=dict(size=8),
    hovertemplate='Month: %{x}<br>Revenue: Rs.%{y:,.0f}<extra></extra>'
), row=1, col=1)

# Product revenue bar
df_prod_sales = df_sales.groupby('product_name')['revenue'].sum().reset_index()
df_prod_sales = df_prod_sales.sort_values('revenue', ascending=False)
fig2.add_trace(go.Bar(
    x=df_prod_sales['product_name'],
    y=df_prod_sales['revenue'],
    name='Product Revenue',
    marker_color=COLORS['blue'],
    opacity=0.85,
    hovertemplate='%{x}<br>Rs.%{y:,.0f}<extra></extra>'
), row=1, col=2)

# Region pie
df_region = df_sales.groupby('region')['revenue'].sum().reset_index()
fig2.add_trace(go.Pie(
    labels=df_region['region'],
    values=df_region['revenue'],
    hole=0.4,
    name='Region',
    marker_colors=[COLORS['blue'], COLORS['green'],
                   COLORS['orange'], COLORS['purple']],
    textinfo='label+percent'
), row=2, col=1)

# Maintenance cost pie
fig2.add_trace(go.Pie(
    labels=df_maint_cost['maint_type'],
    values=df_maint_cost['cost'],
    hole=0.4,
    name='Maint Type',
    marker_colors=[COLORS['red'], COLORS['orange'], COLORS['blue']],
    textinfo='label+percent'
), row=2, col=2)

fig2.update_layout(
    title=dict(
        text="Sales & Maintenance Dashboard | FactoryAnalytics",
        font=dict(size=16, color='#2c3e50'),
        x=0.5, xanchor='center'
    ),
    height=700,
    paper_bgcolor='#f8f9fa',
    font=dict(family='Arial, sans-serif', size=11),
    showlegend=False
)

fig2.write_html('sales_dashboard.html', include_plotlyjs='cdn', full_html=True)
print()
print("  [OK] sales_dashboard.html saved!")
print()

print("=" * 60)
print("  STEP 5 COMPLETE!")
print("=" * 60)
print()
print("  Files saved:")
print("  - manufacturing_dashboard.html  (main dashboard)")
print("  - sales_dashboard.html          (sales + maintenance)")
print()
print("  Features available in browser:")
print("  - Hover     = exact values dekho")
print("  - Zoom      = scroll ya drag karo")
print("  - Pan       = chart move karo")
print("  - Legend    = click karke series hide/show")
print("  - Download  = camera icon se PNG save karo")
print()
print("  Aaj jo seekha:")
print("  - go.Indicator  = KPI cards/gauges")
print("  - go.Bar        = bar chart (color pe conditions)")
print("  - go.Scatter    = line + scatter chart")
print("  - go.Pie        = pie/donut chart")
print("  - make_subplots = multiple charts ek page")
print("  - hovertemplate = custom hover text")
print("  - fig.write_html= browser mein kholne wali file")
print()
print("  Next: Step 6 - Automated HTML Report")
print("  Python se professional report generate hogi")
print("  Print ya email kar sakte ho!")