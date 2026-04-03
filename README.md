# 🏭 Manufacturing Analytics System (End-to-End Project)

## 🚀 Overview

This project is a complete **industrial-level data analytics solution** built for a manufacturing environment.
It covers everything from **database design → data analysis → visualization → machine learning → business dashboards**.

The goal is to analyze production data, detect patterns, and predict defects to improve operational efficiency.

---

## 🧩 Tech Stack

* 🗄️ SQL Server (MSSQL)
* 🐍 Python (Pandas, NumPy)
* 📊 Data Visualization (Matplotlib, Seaborn, Plotly)
* 🤖 Machine Learning (Scikit-learn)
* 📈 Power BI Dashboard
* 📄 HTML Reporting

---

## 📂 Project Structure

```
FactoryAnalytics/
│
├── step1_mssql.py           # Database creation + data generation
├── step2_analysis.py        # SQL + Pandas analysis
├── step3_charts.py          # Static charts (Matplotlib/Seaborn)
├── step4_ml_model.py        # ML model (defect prediction)
├── step5_dashboard.py       # Interactive Plotly dashboard
├── step6_report.py          # Automated HTML report
│
├── manufacturing_dashboard.html
├── factory_report.html
├── step3_dashboard.png
│
└── README.md
```

---

## 📊 Key Features

### 🔹 1. Database Design

* 6 relational tables:

  * production (main)
  * machines
  * employees
  * products
  * sales
  * maintenance

---

### 🔹 2. Data Analysis

* Total Production & Defect Rate
* Machine-wise performance
* Shift-wise analysis
* Correlation analysis (Temperature vs Defects)

---

### 🔹 3. Visualization

* 9+ professional charts:

  * Bar Charts
  * Line Trends
  * Heatmaps
  * Scatter Plots

---

### 🔹 4. Machine Learning Model

* Defect Prediction Model
* Accuracy: **~95%**
* Key factors:

  * Temperature (highest impact)
  * Vibration level
  * Experience

---

### 🔹 5. Interactive Dashboard

* Built using Plotly
* Features:

  * Hover insights
  * Zoom & filtering
  * Multi-panel layout

---

### 🔹 6. Power BI Dashboard

* Live connection with SQL Server
* KPI Cards
* Conditional formatting
* Slicers (Machine / Shift / Date)

---

### 🔹 7. Automated Reporting

* HTML report generation
* Business insights included
* Exportable to PDF

---

## 📈 Business Insights

* Night shift shows higher defect rates
* Temperature strongly impacts defects
* Some machines consistently underperform
* Revenue loss identified due to defects

---

## ▶️ How to Run

### Step 1: Install dependencies

```
pip install pandas numpy matplotlib seaborn scikit-learn plotly pyodbc
```

### Step 2: Run project

```
python step1_mssql.py
python step2_analysis.py
python step3_charts.py
python step4_ml_model.py
python step5_dashboard.py
python step6_report.py
```

---

## 💡 Future Improvements

* Real-time data integration
* Streamlit web dashboard
* API integration
* Advanced ML models

---

## 🙌 Author

Developed as a complete **Data Analyst Portfolio Project** to simulate real-world industrial analytics.

---

## ⭐ If you like this project

Give it a ⭐ on GitHub and share feedback!
