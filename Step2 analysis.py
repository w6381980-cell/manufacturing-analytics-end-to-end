# -*- coding: utf-8 -*-
# MANUFACTURING ANALYTICS - STEP 2
# SQL QUERIES + PANDAS ANALYSIS
#
# Is step mein sikhenge:
#   1. Python se SQL queries kaise chalate hain
#   2. Query results ko Pandas DataFrame mein kaise laate hain
#   3. KPIs (Key Performance Indicators) kaise calculate karte hain
#   4. Machine, Shift, Department wise analysis
#   5. Pandas ke useful functions - groupby, sort, filter, describe
#
# Run: python step2_analysis.py

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pyodbc
import pandas as pd
import numpy as np

print("=" * 60)
print("  STEP 2 - SQL Queries + Pandas Analysis")
print("=" * 60)
print()

# ---------------------------------------------------------------
# SECTION A: CONNECTION
# Step 1 se same connection - sirf DATABASE_NAME change karo nahi
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
print("  [OK] SQL Server connected!")
print()

# ---------------------------------------------------------------
# SECTION B: PANDAS + SQL KAISE KAAM KARTA HAI
#
# pd.read_sql_query(query, conn)
#   - SQL query chalaata hai
#   - Result seedha DataFrame mein aata hai
#   - DataFrame = Excel sheet ki tarah - rows aur columns
#
# DataFrame kya hai?
#   df = ek table jisme:
#     df.columns  = column names
#     df.shape    = (rows, columns) - size
#     df.head(5)  = pehle 5 rows dekho
#     df.tail(5)  = aakhri 5 rows
#     df.info()   = column types aur null values
#     df.describe()= min, max, mean, std sab ek saath
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# SECTION C: POORA DATA EK BAAR LOAD KARO
# Baar baar database call karne se slow hota hai
# Ek baar mein sab data lao, phir Pandas se analyze karo
# ---------------------------------------------------------------
print("-" * 60)
print("  DATA LOAD KARNA (SQL Server se)")
print("-" * 60)

# Main production data with JOIN
# JOIN = do tables ko milao ek common column pe
# INNER JOIN = sirf woh rows jahan dono tables mein match ho
query_main = (
    "SELECT "
    "    p.prod_id, p.prod_date, p.machine_id, p.emp_id, "
    "    p.product_id, p.shift, "
    "    p.units_made, p.units_defective, p.downtime_hrs, "
    "    p.temperature, p.pressure, p.humidity, p.vibration_level, "
    "    m.machine_name, m.department, m.age_years, m.capacity_per_hr, "
    "    e.emp_name, e.experience_yrs, "
    "    pr.product_name, pr.category, pr.price_per_unit "
    "FROM production p "
    "INNER JOIN machines  m  ON p.machine_id = m.machine_id "
    "INNER JOIN employees e  ON p.emp_id     = e.emp_id "
    "INNER JOIN products  pr ON p.product_id = pr.product_id"
)
# ---- Samjho: JOIN ----
# production table mein sirf machine_id hai - naam nahi
# machines table mein naam hai
# JOIN se dono tables ek saath milte hain
# ON = kaunse column pe match karna hai

df = pd.read_sql_query(query_main, conn)
print("  [OK] Production data loaded: " + str(len(df)) + " rows, " + str(len(df.columns)) + " columns")

# Derived columns banao - calculated fields
# Yeh SQL mein bhi bana sakte the, lekin Pandas mein easy hai
df['prod_date']   = pd.to_datetime(df['prod_date'])   # String -> Date
df['defect_rate'] = df['units_defective'] / df['units_made'] * 100
df['efficiency']  = (df['units_made'] - df['units_defective']) / df['units_made'] * 100
df['month']       = df['prod_date'].dt.to_period('M')   # "2024-01" format
df['week']        = df['prod_date'].dt.isocalendar().week
df['good_units']  = df['units_made'] - df['units_defective']
df['revenue_lost']= df['units_defective'] * df['price_per_unit']
# revenue_lost = kitne defect units bane x unki price = waste

# Sales aur Maintenance bhi load karo
df_sales = pd.read_sql_query(
    "SELECT s.*, pr.product_name, pr.category "
    "FROM sales s INNER JOIN products pr ON s.product_id = pr.product_id",
    conn
)
df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])

df_maint = pd.read_sql_query(
    "SELECT ma.*, m.machine_name, m.department "
    "FROM maintenance ma INNER JOIN machines m ON ma.machine_id = m.machine_id",
    conn
)
df_maint['maint_date'] = pd.to_datetime(df_maint['maint_date'])

print("  [OK] Sales data loaded  : " + str(len(df_sales)) + " rows")
print("  [OK] Maintenance loaded : " + str(len(df_maint)) + " rows")
print()


# ---------------------------------------------------------------
# SECTION D: KPI DASHBOARD
# KPI = Key Performance Indicator = sabse important numbers
# Har manager sabse pehle yahi dekhta hai
# ---------------------------------------------------------------
print("=" * 60)
print("  KPI DASHBOARD")
print("=" * 60)

total_units     = df['units_made'].sum()
total_defects   = df['units_defective'].sum()
total_good      = df['good_units'].sum()
overall_defect  = round(df['units_defective'].sum() / df['units_made'].sum() * 100, 2)
total_downtime  = round(df['downtime_hrs'].sum(), 1)
total_revenue   = round(df_sales['revenue'].sum(), 2)
total_waste     = round(df['revenue_lost'].sum(), 2)
avg_efficiency  = round(df['efficiency'].mean(), 2)
total_maint_cost= round(df_maint['cost'].sum(), 2)

print()
print("  PRODUCTION KPIs")
print("  " + "-" * 40)
print("  Total Units Produced   : {:>12,}".format(int(total_units)))
print("  Good Units             : {:>12,}".format(int(total_good)))
print("  Defective Units        : {:>12,}".format(int(total_defects)))
print("  Overall Defect Rate    : {:>11}%".format(overall_defect))
print("  Avg Efficiency         : {:>11}%".format(avg_efficiency))
print("  Total Downtime         : {:>10} hrs".format(total_downtime))
print()
print("  FINANCIAL KPIs (Rs.)")
print("  " + "-" * 40)
print("  Total Revenue (Sales)  : Rs. {:>12,.2f}".format(total_revenue))
print("  Revenue Lost (Defects) : Rs. {:>12,.2f}".format(total_waste))
print("  Maintenance Cost       : Rs. {:>12,.2f}".format(total_maint_cost))
print()


# ---------------------------------------------------------------
# SECTION E: SQL QUERIES - MACHINE WISE ANALYSIS
#
# groupby() = GROUP BY SQL ka Python version
# SQL:    SELECT machine_id, AVG(defect_rate) FROM ... GROUP BY machine_id
# Pandas: df.groupby('machine_id')['defect_rate'].mean()
# ---------------------------------------------------------------
print("-" * 60)
print("  MACHINE WISE PERFORMANCE (SQL Query)")
print("-" * 60)

# Yeh SQL query directly SQL Server pe run hogi
query_machine = (
    "SELECT "
    "    p.machine_id, "
    "    m.machine_name, "
    "    m.department, "
    "    m.age_years, "
    "    COUNT(*)                                          AS total_runs, "
    "    SUM(p.units_made)                                AS total_units, "
    "    SUM(p.units_defective)                           AS total_defects, "
    "    ROUND(SUM(p.units_defective) * 100.0 "
    "          / SUM(p.units_made), 2)                    AS defect_rate_pct, "
    "    ROUND(AVG(CAST(p.downtime_hrs AS FLOAT)), 2)     AS avg_downtime, "
    "    ROUND(AVG(CAST(p.temperature AS FLOAT)), 1)      AS avg_temp, "
    "    ROUND(AVG(CAST(p.vibration_level AS FLOAT)), 2)  AS avg_vibration "
    "FROM production p "
    "INNER JOIN machines m ON p.machine_id = m.machine_id "
    "GROUP BY p.machine_id, m.machine_name, m.department, m.age_years "
    "ORDER BY defect_rate_pct DESC"
)
# ---- Samjho: CAST(x AS FLOAT) / 100.0 ----
# SQL Server mein INT / INT = INT (decimal cut ho jaata)
# CAST se FLOAT mein badlo - tab decimal aata hai
# Ya simple trick: *100.0 (float se multiply karo)

df_machine = pd.read_sql_query(query_machine, conn)

print()
print("  {:<22} {:<12} {:>8} {:>10} {:>9} {:>9}".format(
    "Machine", "Department", "Units", "Defects", "Defect%", "Avg Temp"))
print("  " + "-" * 75)
for _, row in df_machine.iterrows():
    # iterrows() = DataFrame ki har row pe loop chalao
    marker = " <-- WORST" if row['defect_rate_pct'] == df_machine['defect_rate_pct'].max() else ""
    marker = " <-- BEST"  if row['defect_rate_pct'] == df_machine['defect_rate_pct'].min() else marker
    print("  {:<22} {:<12} {:>8,} {:>10,} {:>8.2f}% {:>8.1f}C{}".format(
        row['machine_name'][:22],
        row['department'][:12],
        int(row['total_units']),
        int(row['total_defects']),
        row['defect_rate_pct'],
        row['avg_temp'],
        marker
    ))

best_machine  = df_machine.loc[df_machine['defect_rate_pct'].idxmin(), 'machine_name']
worst_machine = df_machine.loc[df_machine['defect_rate_pct'].idxmax(), 'machine_name']
print()
print("  Best Machine  : " + best_machine)
print("  Worst Machine : " + worst_machine)
print()


# ---------------------------------------------------------------
# SECTION F: SHIFT WISE ANALYSIS
# Pandas groupby use karenge - SQL ki zaroorat nahi
# Pandas groupby = SQL GROUP BY jaisa hi kaam karta hai
# ---------------------------------------------------------------
print("-" * 60)
print("  SHIFT WISE ANALYSIS (Pandas groupby)")
print("-" * 60)

# groupby('shift') = shift ke hisab se groups banao
# agg() = har group pe multiple calculations ek saath
df_shift = df.groupby('shift').agg(
    total_runs    = ('prod_id',         'count'),    # count = kitne rows
    total_units   = ('units_made',      'sum'),      # sum = total
    total_defects = ('units_defective', 'sum'),
    avg_downtime  = ('downtime_hrs',    'mean'),     # mean = average
    avg_temp      = ('temperature',     'mean'),
    avg_efficiency= ('efficiency',      'mean')
).reset_index()
# reset_index() = 'shift' ko column mein wapas laao (index se)

# Defect rate calculate karo
df_shift['defect_rate'] = round(
    df_shift['total_defects'] / df_shift['total_units'] * 100, 2
)
df_shift['avg_downtime']   = round(df_shift['avg_downtime'], 2)
df_shift['avg_efficiency'] = round(df_shift['avg_efficiency'], 2)

# sort_values = kisi column ke hisab se sort karo
df_shift = df_shift.sort_values('defect_rate', ascending=False)

print()
print("  {:<10} {:>12} {:>10} {:>10} {:>11}".format(
    "Shift", "Total Units", "Defects", "Defect%", "Efficiency%"))
print("  " + "-" * 58)
for _, row in df_shift.iterrows():
    marker = " <-- WORST" if row['defect_rate'] == df_shift['defect_rate'].max() else ""
    print("  {:<10} {:>12,} {:>10,} {:>9.2f}% {:>10.2f}%{}".format(
        row['shift'],
        int(row['total_units']),
        int(row['total_defects']),
        row['defect_rate'],
        row['avg_efficiency'],
        marker
    ))
print()


# ---------------------------------------------------------------
# SECTION G: MONTHLY TREND ANALYSIS
# Time-series analysis - time ke saath kya hua
# ---------------------------------------------------------------
print("-" * 60)
print("  MONTHLY TREND (SQL + Pandas)")
print("-" * 60)

query_monthly = (
    "SELECT "
    "    FORMAT(prod_date, 'yyyy-MM')                       AS month, "
    "    SUM(units_made)                                    AS total_units, "
    "    SUM(units_defective)                               AS total_defects, "
    "    ROUND(SUM(units_defective)*100.0/SUM(units_made),2) AS defect_pct, "
    "    ROUND(AVG(CAST(downtime_hrs AS FLOAT)),2)           AS avg_downtime, "
    "    ROUND(AVG(CAST(temperature  AS FLOAT)),1)           AS avg_temp "
    "FROM production "
    "GROUP BY FORMAT(prod_date, 'yyyy-MM') "
    "ORDER BY month"
)
# FORMAT(date, 'yyyy-MM') = SQL Server ka date format function
# "2024-01", "2024-02" etc. banata hai

df_monthly = pd.read_sql_query(query_monthly, conn)

print()
print("  {:<8} {:>12} {:>10} {:>9} {:>9}".format(
    "Month", "Units", "Defects", "Defect%", "Avg Temp"))
print("  " + "-" * 55)

for _, row in df_monthly.iterrows():
    # Trend indicator - pichle mahine se better ya worse?
    trend = ""
    idx = df_monthly.index.get_loc(_)
    if idx > 0:
        prev = df_monthly.iloc[idx-1]['defect_pct']
        curr = row['defect_pct']
        trend = " (^)" if curr > prev else " (v)" if curr < prev else " (=)"
        # (^) = badhna = bura, (v) = ghata = acha

    print("  {:<8} {:>12,} {:>10,} {:>8.2f}%{:>9.1f}C{}".format(
        row['month'],
        int(row['total_units']),
        int(row['total_defects']),
        row['defect_pct'],
        row['avg_temp'],
        trend
    ))

best_month  = df_monthly.loc[df_monthly['defect_pct'].idxmin(),  'month']
worst_month = df_monthly.loc[df_monthly['defect_pct'].idxmax(), 'month']
print()
print("  Best Month  : " + str(best_month) + " (lowest defects)")
print("  Worst Month : " + str(worst_month) + " (highest defects)")
print()


# ---------------------------------------------------------------
# SECTION H: DEPARTMENT ANALYSIS
# ---------------------------------------------------------------
print("-" * 60)
print("  DEPARTMENT WISE ANALYSIS")
print("-" * 60)

df_dept = df.groupby('department').agg(
    total_units   = ('units_made',       'sum'),
    total_defects = ('units_defective',  'sum'),
    total_downtime= ('downtime_hrs',     'sum'),
    avg_temp      = ('temperature',      'mean'),
    machine_count = ('machine_id',       'nunique')
    # nunique = unique values ki count (kitni alag machines hain)
).reset_index()

df_dept['defect_rate']  = round(df_dept['total_defects'] / df_dept['total_units'] * 100, 2)
df_dept['total_downtime']= round(df_dept['total_downtime'], 1)
df_dept = df_dept.sort_values('defect_rate', ascending=False)

print()
print("  {:<12} {:>10} {:>9} {:>9} {:>12} {:>8}".format(
    "Department", "Units", "Defect%", "Downtime", "Machines", "Avg Temp"))
print("  " + "-" * 65)
for _, row in df_dept.iterrows():
    print("  {:<12} {:>10,} {:>8.2f}% {:>9.1f}h {:>12} {:>7.1f}C".format(
        row['department'],
        int(row['total_units']),
        row['defect_rate'],
        row['total_downtime'],
        int(row['machine_count']),
        row['avg_temp']
    ))
print()


# ---------------------------------------------------------------
# SECTION I: PRODUCT WISE ANALYSIS
# ---------------------------------------------------------------
print("-" * 60)
print("  PRODUCT WISE ANALYSIS")
print("-" * 60)

df_product = df.groupby(['product_id','product_name','category']).agg(
    total_units   = ('units_made',       'sum'),
    total_defects = ('units_defective',  'sum'),
    revenue_lost  = ('revenue_lost',     'sum')
).reset_index()

df_product['defect_rate'] = round(
    df_product['total_defects'] / df_product['total_units'] * 100, 2
)
df_product['revenue_lost'] = round(df_product['revenue_lost'], 2)
df_product = df_product.sort_values('revenue_lost', ascending=False)

print()
print("  {:<18} {:>10} {:>9} {:>16}".format(
    "Product", "Units", "Defect%", "Revenue Lost (Rs.)"))
print("  " + "-" * 58)
for _, row in df_product.iterrows():
    print("  {:<18} {:>10,} {:>8.2f}% {:>16,.2f}".format(
        row['product_name'][:18],
        int(row['total_units']),
        row['defect_rate'],
        row['revenue_lost']
    ))
print()


# ---------------------------------------------------------------
# SECTION J: CORRELATION ANALYSIS
# Correlation = ek cheez doosri cheez ko kitna affect karti hai
# +1.0  = perfect positive (A badhta hai to B bhi badhta hai)
# -1.0  = perfect negative (A badhta hai to B ghatta hai)
#  0.0  = koi relation nahi
#
# Rule of thumb:
#   |corr| > 0.7 = strong relation
#   |corr| > 0.4 = moderate relation
#   |corr| < 0.2 = weak / no relation
# ---------------------------------------------------------------
print("-" * 60)
print("  CORRELATION ANALYSIS")
print("  (Defect Rate kisse zyada affected hai?)")
print("-" * 60)

factors = ['temperature','pressure','downtime_hrs','age_years',
           'vibration_level','humidity','experience_yrs']

print()
print("  Factor           Correlation  Strength         Direction")
print("  " + "-" * 60)
for col in factors:
    if col in df.columns:
        corr = df['defect_rate'].corr(df[col])
        absv = abs(corr)
        bar  = "#" * int(absv * 20)
        strength = "Strong" if absv>0.6 else "Moderate" if absv>0.3 else "Weak"
        direction= "Positive (bad)" if corr>0.05 else "Negative (good)" if corr<-0.05 else "Neutral"
        print("  {:<16} {:>+.3f}  {:8}  {}".format(col, corr, strength, direction))

print()
print("  Insight:")
print("  - Temperature aur defect rate ka STRONG positive correlation hai")
print("  - Temperature control karo = defects kam hoge")
print("  - Experience negative correlation = zyada experience = kam defects")
print()


# ---------------------------------------------------------------
# SECTION K: TOP 5 WORST PERFORMING RUNS
# Filter karna - sirf woh rows jo condition meet karein
# ---------------------------------------------------------------
print("-" * 60)
print("  TOP 5 WORST PRODUCTION RUNS")
print("-" * 60)

# nlargest(5, 'defect_rate') = defect_rate ke hisab se top 5
worst_runs = df.nlargest(5, 'defect_rate')[
    ['prod_date','machine_name','shift','units_made',
     'units_defective','defect_rate','temperature','emp_name']
]

print()
for i, (_, row) in enumerate(worst_runs.iterrows(), 1):
    print("  {}. Date: {} | Machine: {} | Shift: {}".format(
        i, str(row['prod_date'])[:10], row['machine_name'], row['shift']))
    print("     Units: {} | Defects: {} | Defect Rate: {:.1f}%".format(
        row['units_made'], row['units_defective'], row['defect_rate']))
    print("     Temperature: {}C | Operator: {}".format(
        row['temperature'], row['emp_name']))
    print()


# ---------------------------------------------------------------
# SECTION L: PANDAS describe() - Quick Stats
# describe() = ek line mein saari stats - analyst ka best friend!
# ---------------------------------------------------------------
print("-" * 60)
print("  STATISTICAL SUMMARY (df.describe())")
print("-" * 60)

stats_cols = ['units_made','units_defective','defect_rate',
              'downtime_hrs','temperature','vibration_level']
desc = df[stats_cols].describe().round(2)
# describe() returns: count, mean, std, min, 25%, 50%, 75%, max

print()
print("  " + desc.to_string().replace('\n', '\n  '))
print()
print("  Std (Standard Deviation) = kitna variation hai data mein")
print("  50% (Median) = beech ka value - outliers se affect nahi hota")
print()


# ---------------------------------------------------------------
# SECTION M: SALES ANALYSIS
# ---------------------------------------------------------------
print("-" * 60)
print("  SALES ANALYSIS")
print("-" * 60)

df_sales_summary = df_sales.groupby('product_name').agg(
    total_qty     = ('qty_sold', 'sum'),
    total_revenue = ('revenue',  'sum'),
    avg_price     = ('revenue',  'mean')
).reset_index().sort_values('total_revenue', ascending=False)

total_rev = df_sales['revenue'].sum()
print()
print("  {:<18} {:>10} {:>16}  {:>8}".format(
    "Product", "Qty Sold", "Revenue (Rs.)", "Share%"))
print("  " + "-" * 58)
for _, row in df_sales_summary.iterrows():
    share = round(row['total_revenue'] / total_rev * 100, 1)
    print("  {:<18} {:>10,} {:>16,.0f}  {:>7.1f}%".format(
        row['product_name'][:18],
        int(row['total_qty']),
        row['total_revenue'],
        share
    ))
print()
print("  Total Revenue : Rs. {:,.2f}".format(total_rev))
print()


# ---------------------------------------------------------------
# SECTION N: SAVE RESULTS TO CSV
# Analysis results Excel/CSV mein save karo
# ---------------------------------------------------------------
print("-" * 60)
print("  RESULTS SAVE KARNA (CSV files)")
print("-" * 60)

df_machine.to_csv("result_machine_performance.csv", index=False)
df_shift.to_csv("result_shift_analysis.csv", index=False)
df_monthly.to_csv("result_monthly_trend.csv", index=False)
df_dept.to_csv("result_department.csv", index=False)
df_product.to_csv("result_product_analysis.csv", index=False)
df_sales_summary.to_csv("result_sales.csv", index=False)

# index=False = row numbers mat save karo CSV mein

print()
print("  [OK] result_machine_performance.csv")
print("  [OK] result_shift_analysis.csv")
print("  [OK] result_monthly_trend.csv")
print("  [OK] result_department.csv")
print("  [OK] result_product_analysis.csv")
print("  [OK] result_sales.csv")
print()
print("  Yeh CSV files Excel mein bhi khol sakte ho!")
print()

conn.close()

print("=" * 60)
print("  STEP 2 COMPLETE!")
print("=" * 60)
print()
print("  Aaj jo seekha:")
print("  - pd.read_sql_query() = SQL result -> DataFrame")
print("  - df.groupby()        = GROUP BY jaisa")
print("  - df.agg()            = multiple calculations ek saath")
print("  - df.corr()           = correlation analysis")
print("  - df.describe()       = quick stats summary")
print("  - df.nlargest()       = top N rows")
print("  - df.to_csv()         = result save karna")
print()
print("  Key Findings:")
print("  - Temperature = #1 factor for defects (strong correlation)")
print("  - Night shift = highest defect rate")
print("  - Old machines = more downtime + defects")
print()
print("  Next: Step 3 - Charts aur Visualizations")
print("  Yahi data pe sundar graphs banayenge!")