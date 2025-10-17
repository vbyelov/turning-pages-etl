# Turning Pages – ETL Pipeline (Data Engineer Project)

This project implements a full **ETL (Extract–Transform–Load) pipeline** for an **online bookstore** called *Turning Pages*.  
It was developed as the final project for the Data Engineer module at *alfatraining*.

---

## 🧱 Architecture Overview
- **Source (OLTP)**: Business database with customers, books, orders, payments, and reviews  
- **Target (DWH)**: Star schema with `Fact_Sales` and dimensions (`Dim_Customer`, `Dim_Book`, `Dim_PaymentMethod`, `Dim_Date`)
- **ETL Process**: Python-based pipeline with three main steps  
  1. `step1_extract.py` – extracts source tables into CSV (staging)  
  2. `step2_transform.py` – applies transformations and prepares DWH format  
  3. `step3_load.py` – loads data into SQL Server DWH, including **SCD Type 2** via **SHA-256 HashDiff**
  4. `etl_main.py` – orchestrates the pipeline

---

## ⚙️ Technologies
- **Python 3.x**, **pandas**, **pyodbc**, **dotenv**
- **Microsoft SQL Server 2022**
- **Git / GitHub** for version control
- **PowerPoint + Word** documentation and presentation

---

## 🚀 Quick Start
1. Clone the repo:
   ```bash
   git clone https://github.com/vbyelov/turning-pages-etl.git
   cd turning-pages-etl
   ```
2. Create a virtual environment and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and set your database connection info.
4. Run the main ETL:
   ```bash
   python etl_main.py
   ```

---

## 📂 Project Structure
```
turning-pages-etl/
│
├── etl_main.py
├── step1_extract.py
├── step2_transform.py
├── step3_load.py
├── config_and_connect.py
│
├── data/
│   ├── stage/
│   └── transform/
│
├── sql/
│   ├── DWH_CREATE.sql
│   ├── CREATE_INSERT.sql
│
├── docs/
│   ├── Presentation.pdf
│   ├── Projekt-Report-Final.docx
│
└── README.md
```

---

## 🧩 Features
- **SCD Type 2** implementation with **HashDiff (SHA-256)**  
- **Idempotent pipeline**: safe to re-run without duplicates  
- **Extensible architecture** (can be scaled to ELT design)
- **Comprehensive documentation & test scenarios**

---

## 📊 Authors & Credits
Developed by **Volodymyr Byelov**  
as part of the *Data Engineer Abschlussprojekt (Turning Pages)*  
alfatraining | Mainz | 2025
