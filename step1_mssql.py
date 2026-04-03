# -*- coding: utf-8 -*-
# MANUFACTURING ANALYTICS - STEP 1 (SQL SERVER VERSION)
# Server  : LAPTOP-LG4BEQ1J\SQLEXPRESS
# Login   : Windows Authentication
# Run     : python step1_mssql.py
# Install : pip install pyodbc pandas numpy

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pyodbc
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

print("=" * 60)
print("  STEP 1 - Manufacturing Analytics (SQL Server)")
print("=" * 60)
print()

# ---------------------------------------------------------------
# SECTION B: CONNECTION
# SERVER_NAME = aapka SQL Server instance
# Trusted_Connection = Windows Auth (username/password nahi)
# ---------------------------------------------------------------
SERVER_NAME   = "LAPTOP-LG4BEQ1J\\SQLEXPRESS"
DATABASE_NAME = "FactoryAnalytics"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=" + SERVER_NAME + ";"
    "Trusted_Connection=yes;"
)

print("  Connecting to: " + SERVER_NAME)

try:
    conn_master = pyodbc.connect(CONN_STR + "Database=master;", autocommit=True)
    print("  [OK] SQL Server connected!")
except pyodbc.Error as e:
    print("  [ERR] Connection failed: " + str(e))
    print()
    print("  Fix karo:")
    print("  1. SQL Server service running hai? (Task Manager > Services)")
    print("  2. ODBC Driver installed? Control Panel > ODBC Data Sources")
    sys.exit(1)

# ---------------------------------------------------------------
# SECTION C: DATABASE CREATE
# SQL Server mein pehle database manually banana padta hai
# sys.databases = SQL Server ka system table (saare DB ki list)
# ---------------------------------------------------------------
cur_m = conn_master.cursor()

cur_m.execute("SELECT COUNT(*) FROM sys.databases WHERE name = ?", DATABASE_NAME)
if cur_m.fetchone()[0]:
    print("  [!] Old database found - dropping it...")
    cur_m.execute("ALTER DATABASE [" + DATABASE_NAME + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
    cur_m.execute("DROP DATABASE [" + DATABASE_NAME + "]")

cur_m.execute("CREATE DATABASE [" + DATABASE_NAME + "]")
conn_master.close()
print("  [OK] Database '" + DATABASE_NAME + "' created!")
print()

# ---------------------------------------------------------------
# SECTION D: FACTORY DB SE CONNECT
# ---------------------------------------------------------------
conn   = pyodbc.connect(CONN_STR + "Database=" + DATABASE_NAME + ";", autocommit=False)
cursor = conn.cursor()
print("  [OK] '" + DATABASE_NAME + "' se connected!")
print()

# ---------------------------------------------------------------
# SECTION E: TABLES
#
# MSSQL Data Types:
#   NVARCHAR(n)   = text, max n chars, Unicode (Hindi bhi chalti)
#   INT           = whole number
#   DECIMAL(10,2) = decimal (10 digits, 2 after point) - prices ke liye
#   DATE          = date only (2024-01-15)
#   IDENTITY(1,1) = auto-increment: 1,2,3... (start=1, step=1)
#   FOREIGN KEY   = doosri table se link (data integrity)
# ---------------------------------------------------------------
print("-" * 60)
print("  CREATING TABLES")
print("-" * 60)

cursor.execute(
    "CREATE TABLE machines ("
    "machine_id      NVARCHAR(10)  PRIMARY KEY,"
    "machine_name    NVARCHAR(100) NOT NULL,"
    "department      NVARCHAR(50)  NOT NULL,"
    "age_years       INT           NOT NULL,"
    "capacity_per_hr INT,"
    "last_service    DATE)"
)
print("  [OK] Table 1: machines")

cursor.execute(
    "CREATE TABLE employees ("
    "emp_id          NVARCHAR(10)  PRIMARY KEY,"
    "emp_name        NVARCHAR(100) NOT NULL,"
    "department      NVARCHAR(50)  NOT NULL,"
    "shift           NVARCHAR(20)  NOT NULL,"
    "experience_yrs  INT,"
    "supervisor      NVARCHAR(100))"
)
print("  [OK] Table 2: employees")

cursor.execute(
    "CREATE TABLE products ("
    "product_id      NVARCHAR(10)  PRIMARY KEY,"
    "product_name    NVARCHAR(100) NOT NULL,"
    "category        NVARCHAR(50)  NOT NULL,"
    "target_qty      INT,"
    "price_per_unit  DECIMAL(10,2))"
)
print("  [OK] Table 3: products")

# Production = MAIN TABLE
# IDENTITY(1,1) = auto ID
# FOREIGN KEY = machines/employees/products se link
cursor.execute(
    "CREATE TABLE production ("
    "prod_id         INT           PRIMARY KEY IDENTITY(1,1),"
    "prod_date       DATE          NOT NULL,"
    "machine_id      NVARCHAR(10)  NOT NULL,"
    "emp_id          NVARCHAR(10)  NOT NULL,"
    "product_id      NVARCHAR(10)  NOT NULL,"
    "shift           NVARCHAR(20)  NOT NULL,"
    "units_made      INT           NOT NULL,"
    "units_defective INT           NOT NULL,"
    "downtime_hrs    DECIMAL(5,2)  NOT NULL,"
    "temperature     DECIMAL(5,1),"
    "pressure        DECIMAL(5,2),"
    "humidity        DECIMAL(5,1),"
    "vibration_level DECIMAL(5,2),"
    "CONSTRAINT fk_machine  FOREIGN KEY (machine_id) REFERENCES machines(machine_id),"
    "CONSTRAINT fk_employee FOREIGN KEY (emp_id)     REFERENCES employees(emp_id),"
    "CONSTRAINT fk_product  FOREIGN KEY (product_id) REFERENCES products(product_id))"
)
print("  [OK] Table 4: production (MAIN TABLE)")

cursor.execute(
    "CREATE TABLE sales ("
    "sale_id     INT           PRIMARY KEY IDENTITY(1,1),"
    "sale_date   DATE          NOT NULL,"
    "product_id  NVARCHAR(10)  NOT NULL,"
    "qty_sold    INT           NOT NULL,"
    "revenue     DECIMAL(12,2) NOT NULL,"
    "region      NVARCHAR(30),"
    "customer    NVARCHAR(100),"
    "CONSTRAINT fk_sale_prod FOREIGN KEY (product_id) REFERENCES products(product_id))"
)
print("  [OK] Table 5: sales")

cursor.execute(
    "CREATE TABLE maintenance ("
    "maint_id    INT           PRIMARY KEY IDENTITY(1,1),"
    "machine_id  NVARCHAR(10)  NOT NULL,"
    "maint_date  DATE          NOT NULL,"
    "maint_type  NVARCHAR(30)  NOT NULL,"
    "cost        DECIMAL(10,2) NOT NULL,"
    "hours_taken DECIMAL(5,1),"
    "technician  NVARCHAR(100),"
    "CONSTRAINT fk_maint_mac FOREIGN KEY (machine_id) REFERENCES machines(machine_id))"
)
print("  [OK] Table 6: maintenance")

conn.commit()
print()
print("  [OK] Saare tables save ho gaye!")
print()

# ---------------------------------------------------------------
# SECTION F: MASTER DATA INSERT
# Pehle machines/employees/products - kyunki production inhe reference karta hai
# fast_executemany = True - large data ke liye bahut fast
# ---------------------------------------------------------------
print("-" * 60)
print("  MASTER DATA INSERT")
print("-" * 60)

cursor.fast_executemany = True

machines_data = [
    ("M001","CNC Lathe Alpha",       "Machining", 2,120,"2024-01-10"),
    ("M002","Hydraulic Press Beta",  "Pressing",  5, 90,"2024-01-05"),
    ("M003","Welding Robot Gamma",   "Assembly",  3,150,"2024-01-15"),
    ("M004","Milling Machine Delta", "Machining", 7, 80,"2023-12-20"),
    ("M005","Injection Mold Epsilon","Molding",   1,200,"2024-01-18"),
    ("M006","Grinding Unit Zeta",    "Finishing", 4,100,"2024-01-08"),
]
cursor.executemany("INSERT INTO machines VALUES (?,?,?,?,?,?)", machines_data)
print("  [OK] " + str(len(machines_data)) + " machines inserted")

employees_data = [
    ("E001","Ramesh Kumar", "Machining","Morning", 8,"Rajiv Shah"),
    ("E002","Suresh Patil", "Machining","Evening", 5,"Rajiv Shah"),
    ("E003","Amit Singh",   "Pressing", "Morning", 3,"Priya Verma"),
    ("E004","Vijay Rao",    "Pressing", "Night",   6,"Priya Verma"),
    ("E005","Priya Sharma", "Assembly", "Morning",10,"Anil Gupta"),
    ("E006","Deepak Mehta", "Assembly", "Evening", 4,"Anil Gupta"),
    ("E007","Sunita Joshi", "Molding",  "Morning", 7,"Rekha Naik"),
    ("E008","Manoj Tiwari", "Molding",  "Night",   2,"Rekha Naik"),
    ("E009","Anita Desai",  "Finishing","Morning", 9,"Sunil More"),
    ("E010","Rohit Mishra", "Finishing","Evening", 1,"Sunil More"),
]
cursor.executemany("INSERT INTO employees VALUES (?,?,?,?,?,?)", employees_data)
print("  [OK] " + str(len(employees_data)) + " employees inserted")

products_data = [
    ("P001","Engine Piston",  "Engine Parts",   500,250.00),
    ("P002","Gear Box Cover", "Transmission",   300,480.00),
    ("P003","Axle Rod 40mm",  "Drive Train",    400,320.00),
    ("P004","Ball Bearing",   "Bearings",       800, 95.00),
    ("P005","Cylinder Head",  "Engine Parts",   200,750.00),
    ("P006","Brake Disc",     "Braking System", 350,380.00),
    ("P007","Clutch Plate",   "Transmission",   450,290.00),
]
cursor.executemany("INSERT INTO products VALUES (?,?,?,?,?)", products_data)
print("  [OK] " + str(len(products_data)) + " products inserted")
conn.commit()
print()

# ---------------------------------------------------------------
# SECTION G: PRODUCTION DATA GENERATE
#
# Realistic patterns:
#   purani machine  -> zyada defects  (age_years * 0.003)
#   high temp       -> zyada defects  ((temp-85) * 0.004)
#   night shift     -> zyada defects  (+0.015)
#   experienced emp -> kam defects    (experience * -0.001)
#   high vibration  -> zyada defects  ((vib-3.5) * 0.01)
# ---------------------------------------------------------------
print("-" * 60)
print("  GENERATING PRODUCTION DATA (180 days x 6 machines)")
print("-" * 60)

np.random.seed(42)
random.seed(42)

machine_ages = {m[0]: m[3] for m in machines_data}
machine_caps = {m[0]: m[4] for m in machines_data}
machine_dept = {m[0]: m[2] for m in machines_data}
emp_shifts   = {e[0]: e[3] for e in employees_data}
emp_exp      = {e[0]: e[4] for e in employees_data}
emp_dept_map = {e[0]: e[2] for e in employees_data}
product_ids  = [p[0] for p in products_data]

dept_emps = {}
for eid, dept in emp_dept_map.items():
    dept_emps.setdefault(dept, []).append(eid)

dates = [datetime(2024,1,1) + timedelta(days=i) for i in range(180)]

prod_rows  = []
sales_rows = []
maint_rows = []

for date in dates:
    ds = date.strftime("%Y-%m-%d")

    for mid in [m[0] for m in machines_data]:
        age  = machine_ages[mid]
        cap  = machine_caps[mid]
        dept = machine_dept[mid]

        eid  = random.choice(dept_emps.get(dept, ["E001"]))
        exp  = emp_exp[eid]
        shft = emp_shifts[eid]
        pid  = random.choice(product_ids)

        temp  = float(max(60.0, min(100.0, np.random.normal(70 + age*1.5, 5))))
        pres  = round(float(np.random.uniform(4.0, 8.0)), 2)
        hum   = round(float(np.random.uniform(40.0, 80.0)), 1)
        vib   = round(float(max(0.5, np.random.normal(2.0 + age*0.3, 0.5))), 2)

        sh    = {"Morning":8, "Evening":7.5, "Night":7.0}[shft]
        units = max(50, int(cap * sh * np.random.uniform(0.7, 1.1)))

        dr = 0.02
        dr += max(0.0, (temp - 85.0) * 0.004)
        dr += age * 0.003
        dr += {"Morning":0.0, "Evening":0.005, "Night":0.015}[shft]
        dr -= exp * 0.001
        dr += max(0.0, (vib - 3.5) * 0.01)
        dr  = max(0.01, min(0.20, dr))

        defects  = int(units * dr)
        downtime = round(float(max(0.0, np.random.normal(0.2 + age*0.1, 0.3))), 2)

        prod_rows.append((
            ds, mid, eid, pid, shft,
            units, defects, downtime,
            round(temp,1), pres, hum, vib
        ))

    for _ in range(random.randint(2,5)):
        p   = random.choice(products_data)
        qty = random.randint(10, 200)
        rev = round(qty * p[4] * random.uniform(0.9, 1.1), 2)
        sales_rows.append((
            ds, p[0], qty, rev,
            random.choice(["North","South","East","West"]),
            random.choice(["Tata Motors","Maruti","Mahindra","Hero","Bajaj"])
        ))

    for mid in [m[0] for m in machines_data]:
        if random.random() < 0.03 + machine_ages[mid] * 0.01:
            maint_rows.append((
                mid, ds,
                random.choice(["Preventive","Preventive","Preventive",
                               "Breakdown","Scheduled","Scheduled"]),
                round(random.uniform(500,15000), 2),
                round(random.uniform(0.5, 8.0), 1),
                random.choice(["Raj Technics","Kumar Services","Fast Fix"])
            ))

print("  Production rows  : " + str(len(prod_rows)))
print("  Sales rows       : " + str(len(sales_rows)))
print("  Maintenance rows : " + str(len(maint_rows)))
print("  Inserting into SQL Server...")

cursor.executemany(
    "INSERT INTO production "
    "(prod_date,machine_id,emp_id,product_id,shift,"
    "units_made,units_defective,downtime_hrs,"
    "temperature,pressure,humidity,vibration_level) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
    prod_rows
)

cursor.executemany(
    "INSERT INTO sales (sale_date,product_id,qty_sold,revenue,region,customer) "
    "VALUES (?,?,?,?,?,?)",
    sales_rows
)

cursor.executemany(
    "INSERT INTO maintenance (machine_id,maint_date,maint_type,cost,hours_taken,technician) "
    "VALUES (?,?,?,?,?,?)",
    maint_rows
)

conn.commit()
print("  [OK] All data saved to SQL Server!")
print()

# ---------------------------------------------------------------
# SECTION H: VERIFY
# CAST(...AS FLOAT) zaroori hai - MSSQL mein INT/INT = INT (galat!)
# FLOAT mein convert karo to decimal result mile
# ---------------------------------------------------------------
print("-" * 60)
print("  VERIFICATION")
print("-" * 60)

for t in ["machines","employees","products","production","sales","maintenance"]:
    cursor.execute("SELECT COUNT(*) FROM " + t)
    print("  {:<15s}: {:>6,} rows".format(t, cursor.fetchone()[0]))

print()
cursor.execute(
    "SELECT COUNT(*), SUM(units_made), SUM(units_defective),"
    "ROUND(AVG(CAST(units_defective AS FLOAT)/units_made*100),2),"
    "ROUND(AVG(CAST(temperature AS FLOAT)),1),"
    "ROUND(SUM(CAST(downtime_hrs AS FLOAT)),0) "
    "FROM production"
)
s = cursor.fetchone()
print("  SUMMARY:")
print("  Total records   : {:,}".format(s[0]))
print("  Total units     : {:,}".format(s[1]))
print("  Total defects   : {:,}".format(s[2]))
print("  Avg defect rate : {}%".format(s[3]))
print("  Avg temperature : {} degC".format(s[4]))
print("  Total downtime  : {} hours".format(int(s[5])))
print()

# ---------------------------------------------------------------
# SECTION I: REAL DATA IMPORT FUNCTIONS
# Apni actual Excel/CSV file import karne ke liye
# ---------------------------------------------------------------
print("-" * 60)
print("  REAL DATA IMPORT (functions ready hain)")
print("-" * 60)

def import_csv(csv_path, table_name):
    """
    Apni CSV file SQL Server mein daal do.

    CSV format: Excel > Save As > CSV (Comma delimited)
    Pehli row: column names (prod_date, machine_id, units_made ...)

    Usage:
        import_csv(r"C:\\MyData\\factory.csv", "production")
    """
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ','_')
        rows = [tuple(r) for r in df.itertuples(index=False)]
        cols = ','.join(df.columns)
        placeholders = ','.join(['?'] * len(df.columns))
        cursor.executemany(
            "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + placeholders + ")",
            rows
        )
        conn.commit()
        print("  [OK] " + str(len(df)) + " rows imported from " + csv_path)
    except FileNotFoundError:
        print("  [!] File not found: " + csv_path)
    except Exception as e:
        print("  [ERR] " + str(e))


def import_excel(excel_path, sheet_name, table_name):
    """
    Excel file SQL Server mein import karo.

    Install: pip install openpyxl

    Usage:
        import_excel(r"C:\\MyData\\factory.xlsx", "Sheet1", "production")
    """
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ','_')
        rows = [tuple(r) for r in df.itertuples(index=False)]
        cols = ','.join(df.columns)
        placeholders = ','.join(['?'] * len(df.columns))
        cursor.executemany(
            "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + placeholders + ")",
            rows
        )
        conn.commit()
        print("  [OK] " + str(len(df)) + " rows imported from Excel!")
    except FileNotFoundError:
        print("  [!] File not found: " + excel_path)
    except Exception as e:
        print("  [ERR] " + str(e))


print("  import_csv()   - CSV file import karne ke liye")
print("  import_excel() - Excel file import karne ke liye")
print()
print("  Example:")
print("  import_csv(r'C:\\Data\\meri_factory.csv', 'production')")
print()

conn.close()

print("=" * 60)
print("  STEP 1 COMPLETE!")
print("=" * 60)
print()
print("  SSMS mein verify karo:")
print("  USE FactoryAnalytics")
print("  GO")
print("  SELECT COUNT(*) FROM production   -- 1080 hona chahiye")
print("  SELECT COUNT(*) FROM sales")
print("  SELECT COUNT(*) FROM maintenance")
print()
print("  Next: Step 2 - SQL Queries + Pandas Analysis")