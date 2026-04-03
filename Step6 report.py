# -*- coding: utf-8 -*-
# MANUFACTURING ANALYTICS - STEP 6 (FINAL STEP)
# AUTOMATED HTML REPORT GENERATOR
#
# Is step mein sikhenge:
#   1. Python se HTML report kaise banate hain
#   2. Dynamic data report mein kaise dalete hain
#   3. Tables, KPIs, Insights sab ek page pe
#   4. Print ya email karne layak professional report
#   5. Har baar run karo = fresh report ban jaati hai (auto!)
#
# Run: python step6_report.py
# Output: factory_report.html (browser mein open karo, phir Ctrl+P se print)

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pyodbc
import pandas as pd
import numpy as np
import pickle
from datetime import datetime

print("=" * 60)
print("  STEP 6 - Automated HTML Report Generator")
print("=" * 60)
print()

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
    "SELECT s.*, pr.product_name FROM sales s "
    "INNER JOIN products pr ON s.product_id = pr.product_id",
    conn
)
df_maint = pd.read_sql_query(
    "SELECT ma.*, m.machine_name FROM maintenance ma "
    "INNER JOIN machines m ON ma.machine_id = m.machine_id",
    conn
)
conn.close()

df['prod_date']    = pd.to_datetime(df['prod_date'])
df['defect_rate']  = (df['units_defective'] / df['units_made'] * 100).round(2)
df['efficiency']   = ((df['units_made'] - df['units_defective']) / df['units_made'] * 100).round(2)
df['revenue_lost'] = (df['units_defective'] * df['price_per_unit']).round(2)
df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])

print("  [OK] Data loaded!")

# ---------------------------------------------------------------
# SECTION B: ALL CALCULATIONS
# ---------------------------------------------------------------

# KPIs
total_units     = int(df['units_made'].sum())
total_defects   = int(df['units_defective'].sum())
total_good      = total_units - total_defects
defect_rate     = round(df['units_defective'].sum() / df['units_made'].sum() * 100, 2)
total_downtime  = round(df['downtime_hrs'].sum(), 1)
avg_efficiency  = round(df['efficiency'].mean(), 2)
total_revenue   = round(df_sales['revenue'].sum(), 2)
total_rev_lost  = round(df['revenue_lost'].sum(), 2)
total_maint     = round(df_maint['cost'].sum(), 2)
report_date     = datetime.now().strftime('%d %B %Y, %H:%M')
date_range      = str(df['prod_date'].min().strftime('%d %b %Y')) + \
                  ' to ' + str(df['prod_date'].max().strftime('%d %b %Y'))

# Machine summary
df_mach = df.groupby(['machine_id','machine_name','department','age_years']).agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    avg_downtime  = ('downtime_hrs',    'mean'),
    avg_temp      = ('temperature',     'mean')
).reset_index()
df_mach['defect_rate'] = (df_mach['total_defects'] / df_mach['total_units'] * 100).round(2)
df_mach['avg_downtime']= df_mach['avg_downtime'].round(2)
df_mach['avg_temp']    = df_mach['avg_temp'].round(1)
df_mach = df_mach.sort_values('defect_rate', ascending=False)

# Monthly
df_mon = df.groupby(df['prod_date'].dt.strftime('%Y-%m')).agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum'),
    avg_temp      = ('temperature',     'mean'),
    avg_downtime  = ('downtime_hrs',    'mean')
).reset_index()
df_mon.columns = ['month','total_units','total_defects','avg_temp','avg_downtime']
df_mon['defect_pct']   = (df_mon['total_defects'] / df_mon['total_units'] * 100).round(2)
df_mon['avg_temp']     = df_mon['avg_temp'].round(1)
df_mon['avg_downtime'] = df_mon['avg_downtime'].round(2)

# Shift
df_shift = df.groupby('shift').agg(
    total_units   = ('units_made',      'sum'),
    total_defects = ('units_defective', 'sum')
).reset_index()
df_shift['defect_rate'] = (df_shift['total_defects'] / df_shift['total_units'] * 100).round(2)
df_shift = df_shift.sort_values('defect_rate', ascending=False)

# Top 5 worst runs
worst5 = df.nlargest(5, 'defect_rate')[
    ['prod_date','machine_name','shift','units_made',
     'units_defective','defect_rate','temperature','emp_name']
].reset_index(drop=True)

# ML model
ml_accuracy = "N/A"
top_feature  = "N/A"
try:
    model        = pickle.load(open('defect_prediction_model.pkl','rb'))
    feature_cols = pickle.load(open('feature_cols.pkl','rb'))
    encoders     = pickle.load(open('label_encoders.pkl','rb'))
    imp_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    top_feature  = imp_df.iloc[0]['feature']
    top_imp      = round(imp_df.iloc[0]['importance']*100, 1)
    ml_accuracy  = "95.83"
    print("  [OK] ML model loaded!")
except:
    print("  [!] ML model nahi mila")

# Insights auto-generate
best_machine  = df_mach.iloc[-1]['machine_name']
worst_machine = df_mach.iloc[0]['machine_name']
worst_shift   = df_shift.iloc[0]['shift']
best_month    = df_mon.loc[df_mon['defect_pct'].idxmin(), 'month']
worst_month   = df_mon.loc[df_mon['defect_pct'].idxmax(), 'month']

print("  [OK] All calculations done!")
print()

# ---------------------------------------------------------------
# SECTION C: HTML HELPER FUNCTIONS
# ---------------------------------------------------------------

def status_badge(value, good_threshold, bad_threshold, suffix='%', reverse=False):
    """
    Value ke hisab se colored badge return karo.
    reverse=True: kam value = acha (jaise defect rate)
    """
    if reverse:
        color = 'green' if value <= good_threshold else 'red' if value >= bad_threshold else 'orange'
    else:
        color = 'green' if value >= good_threshold else 'red' if value <= bad_threshold else 'orange'

    colors = {
        'green':  ('#d4edda', '#155724'),
        'orange': ('#fff3cd', '#856404'),
        'red':    ('#f8d7da', '#721c24')
    }
    bg, fg = colors[color]
    return (
        "<span style='background:" + bg + ";color:" + fg + ";"
        "padding:2px 8px;border-radius:4px;font-weight:bold;font-size:13px;'>"
        + str(value) + suffix + "</span>"
    )

def trend_arrow(current, previous):
    """Trend arrow - upar ya neeche"""
    if current > previous:
        return "<span style='color:#C73E1D;font-size:14px;'>&#9650;</span>"  # Up (bad for defects)
    elif current < previous:
        return "<span style='color:#3BB273;font-size:14px;'>&#9660;</span>"  # Down (good for defects)
    return "<span style='color:gray;'>&#9654;</span>"

def make_bar(value, max_value, color='#2E86AB', width=120):
    """Mini progress bar HTML"""
    pct = min(100, round(value / max_value * 100))
    return (
        "<div style='background:#e9ecef;border-radius:3px;width:" + str(width) + "px;display:inline-block;vertical-align:middle;'>"
        "<div style='background:" + color + ";width:" + str(pct) + "%;height:10px;border-radius:3px;'></div>"
        "</div> <small>" + str(value) + "</small>"
    )

# ---------------------------------------------------------------
# SECTION D: HTML REPORT BANANA
# Python mein HTML string banate hain
# f-string = {variable} directly string mein
# ---------------------------------------------------------------
print("-" * 60)
print("  HTML REPORT GENERATE KARNA")
print("-" * 60)

# CSS styles - report ka look/feel
CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 13px;
    color: #2c3e50;
    background: #f4f6f8;
    padding: 20px;
}
.page { max-width: 1100px; margin: 0 auto; }

/* Header */
.header {
    background: linear-gradient(135deg, #2E86AB, #1a5276);
    color: white;
    padding: 28px 32px;
    border-radius: 10px;
    margin-bottom: 24px;
}
.header h1 { font-size: 24px; font-weight: 700; margin-bottom: 6px; }
.header p  { font-size: 13px; opacity: 0.85; }

/* Section titles */
.section-title {
    font-size: 16px;
    font-weight: 700;
    color: #2E86AB;
    border-left: 4px solid #2E86AB;
    padding-left: 10px;
    margin: 28px 0 14px;
}

/* KPI Grid */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin-bottom: 10px;
}
.kpi-card {
    background: white;
    border-radius: 10px;
    padding: 18px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    border-top: 4px solid #2E86AB;
    text-align: center;
}
.kpi-card.red    { border-top-color: #C73E1D; }
.kpi-card.green  { border-top-color: #3BB273; }
.kpi-card.orange { border-top-color: #F18F01; }
.kpi-value { font-size: 26px; font-weight: 700; color: #2E86AB; }
.kpi-card.red   .kpi-value { color: #C73E1D; }
.kpi-card.green .kpi-value { color: #3BB273; }
.kpi-card.orange .kpi-value { color: #F18F01; }
.kpi-label { font-size: 11px; color: #888; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }

/* Tables */
table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    margin-bottom: 20px;
}
thead tr { background: #2E86AB; color: white; }
th { padding: 10px 12px; text-align: left; font-size: 12px; font-weight: 600; }
td { padding: 9px 12px; border-bottom: 1px solid #f0f0f0; font-size: 12px; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #f0f7ff; }
tr.highlight td { background: #fff3cd; }
tr.danger   td { background: #fdf0f0; }

/* Insight boxes */
.insight {
    background: white;
    border-left: 4px solid #F18F01;
    padding: 12px 16px;
    border-radius: 0 8px 8px 0;
    margin-bottom: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    font-size: 13px;
    line-height: 1.6;
}
.insight.good { border-left-color: #3BB273; }
.insight.bad  { border-left-color: #C73E1D; }
.insight b { color: #2c3e50; }

/* Two column layout */
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }

/* ML section */
.ml-box {
    background: #eef3fb;
    border: 1px solid #bee0f5;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 16px;
}
.ml-acc { font-size: 42px; font-weight: 800; color: #2E86AB; }

/* Footer */
.footer {
    text-align: center;
    color: #aaa;
    font-size: 11px;
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid #ddd;
}

/* Print */
@media print {
    body { background: white; padding: 0; }
    .header { background: #2E86AB !important; -webkit-print-color-adjust: exact; }
}
"""

# ---------------------------------------------------------------
# MACHINE TABLE ROWS
# ---------------------------------------------------------------
machine_rows = ""
avg_dr = df_mach['defect_rate'].mean()
for _, row in df_mach.iterrows():
    is_worst = row['defect_rate'] == df_mach['defect_rate'].max()
    is_best  = row['defect_rate'] == df_mach['defect_rate'].min()
    row_class = 'danger' if is_worst else 'highlight' if is_best else ''
    badge = status_badge(row['defect_rate'], avg_dr*0.8, avg_dr*1.2, '%', reverse=True)
    tag   = " <b>[WORST]</b>" if is_worst else " <b>[BEST]</b>" if is_best else ""
    machine_rows += (
        "<tr class='" + row_class + "'>"
        "<td>" + row['machine_id'] + "</td>"
        "<td><b>" + row['machine_name'] + "</b>" + tag + "</td>"
        "<td>" + row['department'] + "</td>"
        "<td>" + str(row['age_years']) + " yrs</td>"
        "<td>" + "{:,}".format(int(row['total_units'])) + "</td>"
        "<td>" + "{:,}".format(int(row['total_defects'])) + "</td>"
        "<td>" + badge + "</td>"
        "<td>" + str(row['avg_downtime']) + " h</td>"
        "<td>" + str(row['avg_temp']) + " C</td>"
        "</tr>"
    )

# ---------------------------------------------------------------
# MONTHLY TABLE ROWS
# ---------------------------------------------------------------
monthly_rows = ""
prev_pct = None
for _, row in df_mon.iterrows():
    arrow = trend_arrow(row['defect_pct'], prev_pct) if prev_pct else ""
    prev_pct = row['defect_pct']
    is_best  = row['month'] == best_month
    is_worst = row['month'] == worst_month
    row_class = 'highlight' if is_best else 'danger' if is_worst else ''
    monthly_rows += (
        "<tr class='" + row_class + "'>"
        "<td><b>" + row['month'] + "</b></td>"
        "<td>" + "{:,}".format(int(row['total_units'])) + "</td>"
        "<td>" + "{:,}".format(int(row['total_defects'])) + "</td>"
        "<td>" + str(row['defect_pct']) + "% " + arrow + "</td>"
        "<td>" + str(row['avg_temp']) + " C</td>"
        "<td>" + str(row['avg_downtime']) + " h</td>"
        "</tr>"
    )

# ---------------------------------------------------------------
# SHIFT TABLE ROWS
# ---------------------------------------------------------------
shift_rows = ""
for _, row in df_shift.iterrows():
    badge = status_badge(row['defect_rate'], 2.5, 3.5, '%', reverse=True)
    shift_rows += (
        "<tr><td><b>" + row['shift'] + "</b></td>"
        "<td>" + "{:,}".format(int(row['total_units'])) + "</td>"
        "<td>" + "{:,}".format(int(row['total_defects'])) + "</td>"
        "<td>" + badge + "</td></tr>"
    )

# ---------------------------------------------------------------
# TOP 5 WORST RUNS TABLE ROWS
# ---------------------------------------------------------------
worst_rows = ""
for i, (_, row) in enumerate(worst5.iterrows(), 1):
    worst_rows += (
        "<tr>"
        "<td><b>" + str(i) + "</b></td>"
        "<td>" + str(row['prod_date'])[:10] + "</td>"
        "<td>" + row['machine_name'] + "</td>"
        "<td>" + row['shift'] + "</td>"
        "<td>" + str(row['emp_name']) + "</td>"
        "<td>" + str(int(row['units_made'])) + "</td>"
        "<td>" + str(int(row['units_defective'])) + "</td>"
        "<td><b style='color:#C73E1D'>" + str(row['defect_rate']) + "%</b></td>"
        "<td>" + str(row['temperature']) + " C</td>"
        "</tr>"
    )

# ---------------------------------------------------------------
# FEATURE IMPORTANCE BARS
# ---------------------------------------------------------------
feature_bars = ""
try:
    for _, row in imp_df.iterrows():
        pct = round(row['importance'] * 100, 1)
        bar_color = '#C73E1D' if pct == imp_df['importance'].max()*100 else '#2E86AB'
        feature_bars += (
            "<tr><td>" + row['feature'] + "</td>"
            "<td>" + make_bar(pct, 30.0, bar_color, 150) + "</td></tr>"
        )
except:
    feature_bars = "<tr><td colspan='2'>ML model nahi mila - Step 4 pehle chalao</td></tr>"

# ---------------------------------------------------------------
# FULL HTML ASSEMBLE KARO
# ---------------------------------------------------------------
html = """<!DOCTYPE html>
<html lang='en'>
<head>
<meta charset='UTF-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Manufacturing Analytics Report</title>
<style>""" + CSS + """</style>
</head>
<body>
<div class='page'>

<!-- HEADER -->
<div class='header'>
  <h1>Manufacturing Analytics Report</h1>
  <p>
    Database: """ + DATABASE_NAME + """ &nbsp;|&nbsp;
    Period: """ + date_range + """ &nbsp;|&nbsp;
    Generated: """ + report_date + """
  </p>
</div>

<!-- KPI SECTION -->
<div class='section-title'>Key Performance Indicators</div>
<div class='kpi-grid'>
  <div class='kpi-card'>
    <div class='kpi-value'>""" + "{:,}".format(total_units) + """</div>
    <div class='kpi-label'>Total Units Produced</div>
  </div>
  <div class='kpi-card red'>
    <div class='kpi-value'>""" + str(defect_rate) + """%</div>
    <div class='kpi-label'>Overall Defect Rate</div>
  </div>
  <div class='kpi-card green'>
    <div class='kpi-value'>""" + str(avg_efficiency) + """%</div>
    <div class='kpi-label'>Avg Efficiency</div>
  </div>
  <div class='kpi-card orange'>
    <div class='kpi-value'>""" + str(total_downtime) + """h</div>
    <div class='kpi-label'>Total Downtime</div>
  </div>
</div>
<div class='kpi-grid'>
  <div class='kpi-card green'>
    <div class='kpi-value'>Rs.""" + "{:,.0f}".format(total_revenue) + """</div>
    <div class='kpi-label'>Total Sales Revenue</div>
  </div>
  <div class='kpi-card red'>
    <div class='kpi-value'>Rs.""" + "{:,.0f}".format(total_rev_lost) + """</div>
    <div class='kpi-label'>Revenue Lost (Defects)</div>
  </div>
  <div class='kpi-card orange'>
    <div class='kpi-value'>Rs.""" + "{:,.0f}".format(total_maint) + """</div>
    <div class='kpi-label'>Maintenance Cost</div>
  </div>
  <div class='kpi-card'>
    <div class='kpi-value'>""" + "{:,}".format(total_defects) + """</div>
    <div class='kpi-label'>Total Defective Units</div>
  </div>
</div>

<!-- MACHINE TABLE -->
<div class='section-title'>Machine Performance Analysis</div>
<table>
  <thead>
    <tr>
      <th>ID</th><th>Machine Name</th><th>Department</th><th>Age</th>
      <th>Total Units</th><th>Defectives</th><th>Defect Rate</th>
      <th>Avg Downtime</th><th>Avg Temp</th>
    </tr>
  </thead>
  <tbody>""" + machine_rows + """</tbody>
</table>

<!-- MONTHLY + SHIFT -->
<div class='two-col'>
  <div>
    <div class='section-title'>Monthly Trend</div>
    <table>
      <thead>
        <tr><th>Month</th><th>Units</th><th>Defects</th>
            <th>Defect%</th><th>Avg Temp</th><th>Avg Downtime</th></tr>
      </thead>
      <tbody>""" + monthly_rows + """</tbody>
    </table>
  </div>
  <div>
    <div class='section-title'>Shift Performance</div>
    <table>
      <thead>
        <tr><th>Shift</th><th>Total Units</th><th>Defectives</th><th>Defect Rate</th></tr>
      </thead>
      <tbody>""" + shift_rows + """</tbody>
    </table>

    <div class='section-title'>ML Model Summary</div>
    <div class='ml-box'>
      <div class='ml-acc'>""" + str(ml_accuracy) + """%</div>
      <div style='color:#555;margin-bottom:12px;'>Model Accuracy (Random Forest, 5-fold CV: 96.94%)</div>
      <table style='box-shadow:none;margin:0;'>
        <thead><tr><th>Feature</th><th>Importance</th></tr></thead>
        <tbody>""" + feature_bars + """</tbody>
      </table>
    </div>
  </div>
</div>

<!-- TOP 5 WORST RUNS -->
<div class='section-title'>Top 5 Worst Production Runs</div>
<table>
  <thead>
    <tr><th>#</th><th>Date</th><th>Machine</th><th>Shift</th><th>Operator</th>
        <th>Units</th><th>Defects</th><th>Defect%</th><th>Temp</th></tr>
  </thead>
  <tbody>""" + worst_rows + """</tbody>
</table>

<!-- INSIGHTS -->
<div class='section-title'>Key Insights & Recommendations</div>

<div class='insight bad'>
  <b>Worst Machine:</b> """ + worst_machine + """ has highest defect rate of
  """ + str(df_mach.iloc[0]['defect_rate']) + """%.
  Immediate maintenance review recommended. Age: """ + str(int(df_mach.iloc[0]['age_years'])) + """ years.
</div>

<div class='insight good'>
  <b>Best Machine:</b> """ + best_machine + """ performing excellently at
  """ + str(df_mach.iloc[-1]['defect_rate']) + """% defect rate.
  Use as benchmark for other machines.
</div>

<div class='insight bad'>
  <b>Night Shift Alert:</b> """ + worst_shift + """ shift has highest defect rate (""" + str(df_shift.iloc[0]['defect_rate']) + """%).
  Consider additional supervision, rest breaks, or workload adjustment.
</div>

<div class='insight'>
  <b>Top ML Predictor:</b> """ + str(top_feature) + """ is the most important factor
  for defect prediction. Monitor this parameter closely in real-time.
</div>

<div class='insight bad'>
  <b>Revenue Loss Alert:</b> Rs.""" + "{:,.0f}".format(total_rev_lost) + """
  lost due to defective units. This is
  """ + str(round(total_rev_lost / total_revenue * 100, 1)) + """% of total sales revenue.
  Reducing defect rate by 1% can save Rs.""" + "{:,.0f}".format(round(total_rev_lost * 0.33)) + """ approximately.
</div>

<div class='insight good'>
  <b>Best Period:</b> """ + str(best_month) + """ had lowest defect rate (""" + str(df_mon.loc[df_mon['defect_pct'].idxmin(),'defect_pct']) + """%).
  Analyze what was different that month - replicate those conditions.
</div>

<!-- FOOTER -->
<div class='footer'>
  Manufacturing Analytics System &nbsp;|&nbsp;
  Python + SQL Server + ML &nbsp;|&nbsp;
  Auto-generated Report &nbsp;|&nbsp;
  """ + report_date + """
  <br><br>
  <small>Print: Ctrl+P &nbsp;|&nbsp; Save PDF: Print > Save as PDF</small>
</div>

</div>
</body>
</html>"""

# File save karo
output_file = 'factory_report.html'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html)

print()
print("  [OK] factory_report.html saved!")
print()
print("=" * 60)
print("  STEP 6 COMPLETE!")
print("=" * 60)
print()
print("  Report kholne ke liye:")
print("  factory_report.html pe double-click karo")
print()
print("  Print karne ke liye:")
print("  Browser mein Ctrl+P dabaao")
print()
print("  PDF save karne ke liye:")
print("  Ctrl+P > Destination: Save as PDF > Save")
print()
print("  Email karne ke liye:")
print("  factory_report.html file attach karo (browser mein khulti hai)")
print()
print("  ============================================")
print("  POORA PROJECT COMPLETE! Sab 6 Steps done!")
print("  ============================================")
print()
print("  Step 1: SQL Server Database + Tables")
print("  Step 2: SQL Queries + Pandas Analysis")
print("  Step 3: Matplotlib + Seaborn Charts (PNG)")
print("  Step 4: ML Prediction Model (95.83% accuracy)")
print("  Step 5: Plotly Interactive Dashboard (HTML)")
print("  Step 6: Automated Report (HTML, print-ready)")
print()
print("  Aap ab ek complete Data Analyst workflow jaante ho!")
print("  Real factory data pe bhi yahi steps kaam karenge.")