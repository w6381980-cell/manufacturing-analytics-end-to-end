# -*- coding: utf-8 -*-
# MANUFACTURING ANALYTICS - STEP 3
# CHARTS & VISUALIZATIONS
#
# Is step mein sikhenge:
#   1. Matplotlib kya hai aur kaise use karte hain
#   2. Seaborn se sundar charts banana
#   3. 6 alag types ke charts - bar, line, scatter, heatmap, pie, box
#   4. Charts ko image file mein save karna
#   5. Multiple charts ek saath (subplots)
#
# Install: pip install matplotlib seaborn
# Run    : python step3_charts.py

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ---------------------------------------------------------------
# Matplotlib kya hai?
#   - Python ka sabse popular charting library
#   - plt.figure() = ek blank canvas banao
#   - plt.subplot() = canvas ko sections mein todo
#   - plt.savefig() = image file mein save karo
#
# Seaborn kya hai?
#   - Matplotlib ke upar bana hua - zyada sundar charts
#   - sns.heatmap(), sns.boxplot() etc.
#   - Default mein achhe colors aur styling
# ---------------------------------------------------------------

print("=" * 60)
print("  STEP 3 - Charts and Visualizations")
print("=" * 60)
print()

# ---------------------------------------------------------------
# CONNECTION + DATA LOAD (Step 2 jaisa)
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

# Poora data ek baar load karo
df = pd.read_sql_query(
    "SELECT p.*, m.machine_name, m.department, m.age_years, "
    "e.experience_yrs, pr.product_name, pr.category, pr.price_per_unit "
    "FROM production p "
    "INNER JOIN machines  m  ON p.machine_id = m.machine_id "
    "INNER JOIN employees e  ON p.emp_id     = e.emp_id "
    "INNER JOIN products  pr ON p.product_id = pr.product_id",
    conn
)
df['prod_date']    = pd.to_datetime(df['prod_date'])
df['defect_rate']  = df['units_defective'] / df['units_made'] * 100
df['efficiency']   = (df['units_made'] - df['units_defective']) / df['units_made'] * 100
df['revenue_lost'] = df['units_defective'] * df['price_per_unit']
df['month_str']    = df['prod_date'].dt.strftime('%b %Y')   # "Jan 2024" format
df['month_num']    = df['prod_date'].dt.to_period('M')

df_sales = pd.read_sql_query(
    "SELECT s.*, pr.product_name FROM sales s "
    "INNER JOIN products pr ON s.product_id = pr.product_id",
    conn
)
conn.close()

print("  [OK] Data loaded: " + str(len(df)) + " rows")
print()

# ---------------------------------------------------------------
# AGGREGATED DATA PREPARE KARO (charts ke liye)
# ---------------------------------------------------------------

# Machine summary
df_mach = df.groupby('machine_name').agg(
    defect_rate  = ('defect_rate',  'mean'),
    avg_downtime = ('downtime_hrs', 'mean'),
    total_units  = ('units_made',   'sum')
).reset_index().sort_values('defect_rate', ascending=False)
df_mach['defect_rate']  = df_mach['defect_rate'].round(2)
df_mach['avg_downtime'] = df_mach['avg_downtime'].round(2)

# Monthly summary
df_mon = df.groupby('month_num').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    avg_temp      = ('temperature',     'mean')
).reset_index()
df_mon['defect_pct'] = (df_mon['total_defects'] / df_mon['total_units'] * 100).round(2)
df_mon['month_label']= df_mon['month_num'].dt.strftime('%b')  # "Jan", "Feb"...

# Shift summary
df_shift = df.groupby('shift').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum')
).reset_index()
df_shift['defect_rate'] = (df_shift['total_defects'] / df_shift['total_units'] * 100).round(2)

# Product revenue lost
df_prod = df.groupby('product_name')['revenue_lost'].sum().reset_index()
df_prod = df_prod.sort_values('revenue_lost', ascending=False)

# Sales by product
df_sal = df_sales.groupby('product_name')['revenue'].sum().reset_index()
df_sal = df_sal.sort_values('revenue', ascending=False)

# Machine x Shift heatmap data
df_heat = df.groupby(['machine_name','shift'])['defect_rate'].mean().round(2)
df_pivot = df_heat.unstack()   # pivot table banao - machine rows, shift columns
# unstack() = long format se wide format (heatmap ke liye chahiye)

print("-" * 60)
print("  CHARTS BANANA SHURU")
print("-" * 60)
print()

# ---------------------------------------------------------------
# CHART STYLE SET KARO
#
# plt.style.use() = chart ka overall look/feel
# Common styles: 'seaborn-v0_8', 'ggplot', 'bmh', 'fivethirtyeight'
#
# figsize = (width, height) in inches
# dpi     = dots per inch - zyada = zyada sharp image
# ---------------------------------------------------------------
plt.style.use('seaborn-v0_8-whitegrid')   # Clean white background with grid

# Color palette define karo
# Yeh colors sab charts mein consistent rahenge
C_BLUE   = '#2E86AB'   # Main blue
C_RED    = '#C73E1D'   # Red - danger/bad
C_GREEN  = '#3BB273'   # Green - good
C_ORANGE = '#F18F01'   # Orange - warning
C_PURPLE = '#7B2FBE'   # Purple - extra
COLORS   = [C_BLUE, C_RED, C_GREEN, C_ORANGE, C_PURPLE, '#888888']

# ---------------------------------------------------------------
# FIGURE BANANA - 6 Charts ek page pe
#
# plt.figure()   = ek blank canvas
# fig, axes      = subplots return karta hai
# subplots(2, 3) = 2 rows, 3 columns = 6 charts
# figsize        = total size
# ---------------------------------------------------------------
fig, axes = plt.subplots(2, 3, figsize=(18, 10))
# axes[0][0] = top-left chart
# axes[0][1] = top-middle chart
# axes[1][2] = bottom-right chart

fig.suptitle(
    'Manufacturing Analytics Dashboard - FactoryAnalytics DB',
    fontsize=16, fontweight='bold', y=1.01
)
# suptitle = sabse upar ek bada title
# y=1.01 = thoda upar jaao taaki charts se overlap na ho

# ---------------------------------------------------------------
# CHART 1: BAR CHART - Machine Wise Defect Rate
# Bar chart kab use karein?
#   - Categories compare karna ho (machine A vs B vs C)
#   - Ranking dikhani ho
# ---------------------------------------------------------------
ax1 = axes[0, 0]
# axes[row, col] - 0-indexed

bars = ax1.bar(
    df_mach['machine_name'],    # X axis - machine names
    df_mach['defect_rate'],     # Y axis - defect rate
    color=COLORS[:len(df_mach)],  # har bar ka alag color
    alpha=0.85,                 # transparency (0=invisible, 1=solid)
    edgecolor='white',          # bar ki border
    linewidth=0.5
)

# Har bar ke upar value likho
for bar in bars:
    height = bar.get_height()
    ax1.text(
        bar.get_x() + bar.get_width() / 2,   # x position = bar ka center
        height + 0.05,                         # y position = bar ke thoda upar
        str(round(height, 1)) + '%',          # text
        ha='center', va='bottom',              # alignment
        fontsize=8, fontweight='bold'
    )

# Average line dikhao - reference ke liye
avg = df_mach['defect_rate'].mean()
ax1.axhline(y=avg, color=C_RED, linestyle='--', linewidth=1.2,
            label='Average (' + str(round(avg,1)) + '%)')
# axhline = horizontal line (ax = axes, h = horizontal, line)

ax1.set_title('Machine Wise Defect Rate', fontweight='bold', fontsize=11)
ax1.set_xlabel('Machine')
ax1.set_ylabel('Defect Rate (%)')
ax1.tick_params(axis='x', rotation=30)   # X labels 30 degree ghuma do
ax1.legend(fontsize=8)
ax1.set_ylim(0, df_mach['defect_rate'].max() * 1.3)   # Y axis thodi badi

print("  [OK] Chart 1: Machine Wise Defect Rate (Bar Chart)")

# ---------------------------------------------------------------
# CHART 2: LINE CHART - Monthly Trend
# Line chart kab use karein?
#   - Time ke saath kya hua dikhana ho
#   - Trend/pattern dikhana ho
# ---------------------------------------------------------------
ax2 = axes[0, 1]

# Bar - units made (left Y axis)
bars2 = ax2.bar(
    df_mon['month_label'],
    df_mon['total_units'] / 1000,    # 1000 se divide = "thousands" mein
    color=C_BLUE, alpha=0.6, label='Units (thousands)'
)

# Line - defect rate (right Y axis)
# twinx() = doosra Y axis banana (same X axis pe)
ax2_twin = ax2.twinx()
ax2_twin.plot(
    df_mon['month_label'],
    df_mon['defect_pct'],
    color=C_RED, marker='o',          # 'o' = circle markers
    linewidth=2, markersize=6,
    label='Defect %'
)

ax2.set_title('Monthly Production vs Defect Rate', fontweight='bold', fontsize=11)
ax2.set_xlabel('Month')
ax2.set_ylabel('Units Made (thousands)', color=C_BLUE)
ax2_twin.set_ylabel('Defect Rate %', color=C_RED)
ax2.tick_params(axis='y', labelcolor=C_BLUE)
ax2_twin.tick_params(axis='y', labelcolor=C_RED)
ax2.legend(loc='upper left',  fontsize=8)
ax2_twin.legend(loc='upper right', fontsize=8)

print("  [OK] Chart 2: Monthly Trend (Bar + Line combo)")

# ---------------------------------------------------------------
# CHART 3: PIE/DONUT CHART - Shift wise Production Share
# Pie chart kab use karein?
#   - Parts of a whole dikhana ho (percentages)
#   - Max 4-5 categories
# ---------------------------------------------------------------
ax3 = axes[0, 2]

wedge_colors = [C_ORANGE, C_BLUE, C_PURPLE]  # Morning, Evening, Night
wedges, texts, autotexts = ax3.pie(
    df_shift['total_units'],
    labels=df_shift['shift'],
    colors=wedge_colors,
    autopct='%1.1f%%',        # percentage format "45.6%"
    startangle=90,             # kahan se start ho
    wedgeprops={'edgecolor': 'white', 'linewidth': 2},   # border
    pctdistance=0.75           # percentage text position
)
# Donut banane ke liye center mein white circle daalo
centre_circle = plt.Circle((0, 0), 0.5, fc='white')
ax3.add_patch(centre_circle)

# Percentage text bold karo
for autotext in autotexts:
    autotext.set_fontweight('bold')
    autotext.set_fontsize(9)

ax3.set_title('Shift Wise Production Share', fontweight='bold', fontsize=11)
print("  [OK] Chart 3: Shift Distribution (Donut Chart)")

# ---------------------------------------------------------------
# CHART 4: SCATTER PLOT - Temperature vs Defect Rate
# Scatter chart kab use karein?
#   - Relationship dikhana ho (correlation)
#   - Patterns ya clusters dhundhna ho
# ---------------------------------------------------------------
ax4 = axes[1, 0]

# Har machine ke points alag color mein
machines = df['machine_name'].unique()
for i, machine in enumerate(machines):
    mask = df['machine_name'] == machine   # filter - sirf us machine ka data
    ax4.scatter(
        df[mask]['temperature'],
        df[mask]['defect_rate'],
        alpha=0.25,                         # transparent - overlap dikhega
        s=12,                               # point size
        color=COLORS[i % len(COLORS)],
        label=machine[:12]                  # sirf 12 characters ka naam
    )

# Trend line (best fit line) dikhao
z = np.polyfit(df['temperature'], df['defect_rate'], 1)
# polyfit(x, y, degree) = best fit polynomial
# degree=1 = straight line (y = mx + c)
p = np.poly1d(z)   # poly1d = polynomial function banao
x_line = np.linspace(df['temperature'].min(), df['temperature'].max(), 100)
ax4.plot(x_line, p(x_line), color=C_RED, linewidth=2,
         linestyle='--', label='Trend Line')

ax4.set_title('Temperature vs Defect Rate', fontweight='bold', fontsize=11)
ax4.set_xlabel('Temperature (degC)')
ax4.set_ylabel('Defect Rate (%)')
ax4.legend(fontsize=6, ncol=2)   # ncol=2 = 2 columns mein legend

print("  [OK] Chart 4: Temperature Scatter Plot")

# ---------------------------------------------------------------
# CHART 5: HEATMAP - Machine x Shift Defect Rate
# Heatmap kab use karein?
#   - 2D grid mein values compare karni ho
#   - Color se value dikho (dark = high, light = low)
# ---------------------------------------------------------------
ax5 = axes[1, 1]

sns.heatmap(
    df_pivot,                       # 2D data (pivot table)
    ax=ax5,
    cmap='YlOrRd',                  # Color map: Yellow -> Orange -> Red
    annot=True,                     # annot=True = har cell mein value likho
    fmt='.1f',                      # format: 1 decimal place
    linewidths=0.5,                 # cell borders
    linecolor='white',
    cbar_kws={'label': 'Defect %', 'shrink': 0.8}
)
# YlOrRd = Yellow (low) se Red (high) jaata hai
# annot = annotation = cell ke andar value
# fmt = format string (printf style)

ax5.set_title('Machine x Shift Defect Heatmap', fontweight='bold', fontsize=11)
ax5.set_xlabel('Shift')
ax5.set_ylabel('Machine')
ax5.tick_params(axis='y', rotation=0)   # Y labels seedhe rakho

print("  [OK] Chart 5: Machine x Shift Heatmap")

# ---------------------------------------------------------------
# CHART 6: HORIZONTAL BAR - Revenue Lost by Product
# Horizontal bar kab use karein?
#   - Zyada categories hon (vertical bar mein labels overlap hoge)
#   - Ranking/comparison
# ---------------------------------------------------------------
ax6 = axes[1, 2]

bar_colors = [C_RED if x == df_prod['revenue_lost'].max()
              else C_ORANGE if x > df_prod['revenue_lost'].mean()
              else C_BLUE for x in df_prod['revenue_lost']]
# List comprehension se conditional colors:
# Red = highest, Orange = above avg, Blue = below avg

bars6 = ax6.barh(   # barh = horizontal bar chart
    df_prod['product_name'],
    df_prod['revenue_lost'] / 1000,    # thousands mein
    color=bar_colors,
    alpha=0.85,
    edgecolor='white'
)

# Har bar pe value likho
for bar in bars6:
    width = bar.get_width()
    ax6.text(
        width + 20,                        # thoda right
        bar.get_y() + bar.get_height()/2,  # vertically center
        'Rs.' + str(round(width, 0)) + 'K',
        va='center', fontsize=7.5
    )

ax6.set_title('Revenue Lost by Product (Defects)', fontweight='bold', fontsize=11)
ax6.set_xlabel('Revenue Lost (Rs. thousands)')
ax6.set_ylabel('Product')
ax6.set_xlim(0, df_prod['revenue_lost'].max() / 1000 * 1.35)

print("  [OK] Chart 6: Revenue Lost Horizontal Bar")

# ---------------------------------------------------------------
# SAVE + SHOW
# tight_layout() = charts ke beech spacing auto-adjust karo
# savefig()      = file mein save karo
#   dpi=150      = image quality (72=web, 150=good, 300=print)
#   bbox_inches='tight' = extra white space hatao
# ---------------------------------------------------------------
plt.tight_layout()
plt.savefig('step3_dashboard.png', dpi=150, bbox_inches='tight')
print()
print("  [OK] step3_dashboard.png saved!")

plt.show()    # Screen pe dikhao (window khulegi)
print()

# ---------------------------------------------------------------
# BONUS: INDIVIDUAL CHARTS ALAG ALAG SAVE KARNA
# Agar kisi ek chart ko bade size mein chahiye
# ---------------------------------------------------------------
print("-" * 60)
print("  BONUS - Individual Charts Save Karna")
print("-" * 60)
print()

# --- Bonus Chart 1: Department Comparison (Grouped Bar) ---
fig2, ax = plt.subplots(figsize=(10, 5))

df_dept = df.groupby('department').agg(
    defect_rate  = ('defect_rate',  'mean'),
    avg_downtime = ('downtime_hrs', 'mean'),
    avg_temp     = ('temperature',  'mean')
).reset_index().round(2)
df_dept = df_dept.sort_values('defect_rate', ascending=False)

x     = np.arange(len(df_dept))   # [0, 1, 2, 3, 4]
width = 0.28   # bar ki width

bar_a = ax.bar(x - width, df_dept['defect_rate'],  width, label='Defect Rate %',
               color=C_RED,    alpha=0.85)
bar_b = ax.bar(x,          df_dept['avg_downtime'], width, label='Avg Downtime (hrs)',
               color=C_ORANGE, alpha=0.85)
bar_c = ax.bar(x + width,  df_dept['avg_temp']/10,  width, label='Avg Temp/10 (degC)',
               color=C_BLUE,   alpha=0.85)
# avg_temp/10 = scale down karo taki bars comparable dikhen

ax.set_title('Department Performance Comparison', fontweight='bold', fontsize=13)
ax.set_xlabel('Department')
ax.set_ylabel('Value')
ax.set_xticks(x)
ax.set_xticklabels(df_dept['department'])
ax.legend()
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('step3_dept_comparison.png', dpi=150, bbox_inches='tight')
print("  [OK] step3_dept_comparison.png saved!")
plt.show()

# --- Bonus Chart 2: Seaborn Boxplot - Defect Rate by Shift ---
fig3, ax = plt.subplots(figsize=(8, 5))

sns.boxplot(
    data=df,
    x='shift',
    y='defect_rate',
    palette={'Morning': C_GREEN, 'Evening': C_ORANGE, 'Night': C_PURPLE},
    order=['Morning', 'Evening', 'Night'],
    width=0.5,
    ax=ax
)
# Boxplot kya dikhata hai?
#   Box = 25% se 75% data (middle 50%)
#   Line in box = median (50%)
#   Whiskers = min aur max (outliers chhod ke)
#   Dots = outliers (bahut upar ya neeche)

# Individual points bhi dikhao (strip plot)
sns.stripplot(
    data=df,
    x='shift',
    y='defect_rate',
    order=['Morning', 'Evening', 'Night'],
    color='black', alpha=0.1, size=2, ax=ax
)

ax.set_title('Defect Rate Distribution by Shift', fontweight='bold', fontsize=13)
ax.set_xlabel('Shift')
ax.set_ylabel('Defect Rate (%)')
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('step3_shift_boxplot.png', dpi=150, bbox_inches='tight')
print("  [OK] step3_shift_boxplot.png saved!")
plt.show()

# --- Bonus Chart 3: Correlation Heatmap ---
fig4, ax = plt.subplots(figsize=(8, 6))

corr_cols = ['defect_rate','temperature','pressure','downtime_hrs',
             'age_years','vibration_level','humidity','experience_yrs',
             'units_made','efficiency']
corr_matrix = df[corr_cols].corr().round(2)
# corr() = pairwise correlation matrix
# Har column ki har doosre column ke saath correlation

mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
# triu = upper triangle - sirf lower triangle dikhayenge
# (symmetric hai, upper aur lower same hote hain)

sns.heatmap(
    corr_matrix,
    mask=mask,       # upper triangle chhupa do
    annot=True,
    fmt='.2f',
    cmap='RdYlGn',   # Red (negative) -> Yellow (0) -> Green (positive)
    center=0,        # 0 pe center karo (yellow)
    square=True,     # cells square shape mein
    linewidths=0.5,
    cbar_kws={'label': 'Correlation'},
    ax=ax
)

ax.set_title('Full Correlation Matrix', fontweight='bold', fontsize=13)
plt.tight_layout()
plt.savefig('step3_correlation_matrix.png', dpi=150, bbox_inches='tight')
print("  [OK] step3_correlation_matrix.png saved!")
plt.show()

print()
print("=" * 60)
print("  STEP 3 COMPLETE!")
print("=" * 60)
print()
print("  Files saved:")
print("  - step3_dashboard.png        (6-in-1 main dashboard)")
print("  - step3_dept_comparison.png  (department grouped bars)")
print("  - step3_shift_boxplot.png    (shift distribution)")
print("  - step3_correlation_matrix.png (full correlation)")
print()
print("  Aaj jo seekha:")
print("  - plt.subplots()    = multiple charts ek canvas pe")
print("  - ax.bar()          = bar chart")
print("  - ax.plot()         = line chart")
print("  - ax.pie()          = pie/donut chart")
print("  - ax.scatter()      = scatter plot")
print("  - sns.heatmap()     = heatmap")
print("  - ax.barh()         = horizontal bar")
print("  - sns.boxplot()     = distribution chart")
print("  - plt.savefig()     = image save karna")
print()
print("  Next: Step 4 - ML Prediction Model")
print("  Machine seekhega: kab HIGH DEFECT hoga?")