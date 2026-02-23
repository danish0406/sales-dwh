# 📊 Sales Data Warehouse

![MySQL](https://img.shields.io/badge/MySQL-8.0-blue?logo=mysql)
![Schema](https://img.shields.io/badge/Schema-Star%20Schema-orange)
![ETL](https://img.shields.io/badge/ETL-Custom%20SQL-yellow)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

> A production-ready Sales Data Warehouse built on a **Star Schema** design in MySQL — transforming raw transactional data into a structured analytical model that powers business intelligence and KPI reporting.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Star Schema Design](#star-schema-design)
- [ETL Process](#etl-process)
- [KPIs & Analytics](#kpis--analytics)
- [Performance Optimization](#performance-optimization)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Future Improvements](#future-improvements)

---

## 🔍 Overview

This project implements an end-to-end **Sales Data Warehouse** that enables multi-dimensional business analysis across:

| Dimension | Description |
|-----------|-------------|
| 🕐 Time | Year, Month, Quarter, Day trends |
| 👤 Customer | Demographics and purchase behavior |
| 📦 Product | Product-level revenue and performance |
| 🏙 City | Geographic sales distribution |

---

## 🎯 Problem Statement

Operational sales data is stored in raw, unoptimized format — making it difficult for business stakeholders to derive insights quickly. This warehouse solves that by providing:

- **Revenue insights** across time, geography, and product lines
- **Customer performance** analysis and segmentation
- **Product-level trends** to identify top performers
- **City-wise metrics** for regional strategy
- **Time-based trends** for forecasting and planning

---

## 🏗 Architecture

```
Raw Data (CSV / Source)
        ↓
Staging Tables
        ↓
Dimension Tables (dim_*)
        ↓
Fact Table (fact_sales)
        ↓
KPI Queries / Analytics
```

### Layers Explained

**1️⃣ Staging Layer** — Raw data landing zone
- `staging_sales`
- `staging_customers`
- `staging_products`
- `staging_cities`

**2️⃣ Dimension Tables** — Conformed, surrogate-keyed dimensions
- `dim_customer`
- `dim_product`
- `dim_city`
- `dim_date`

**3️⃣ Fact Table** — Central analytical table
- `fact_sales` — Stores all transactional measures with FK references to all dimensions

---

## ⭐ Star Schema Design

### Fact Table: `fact_sales`

> **Grain:** One row = one sales transaction for one product, by one customer, in one city, on one date.

| Column | Type | Description |
|--------|------|-------------|
| `sale_id` | INT | Primary Key |
| `date_key` | INT | FK → dim_date |
| `customer_key` | INT | FK → dim_customer |
| `product_key` | INT | FK → dim_product |
| `city_key` | INT | FK → dim_city |
| `quantity` | INT | Units sold |
| `unit_price` | DECIMAL | Price per unit |
| `discount` | DECIMAL | Discount applied |
| `revenue` | DECIMAL | Final revenue |

### 🔑 Why Surrogate Keys?

Surrogate keys are used instead of natural business IDs because they:
- Improve join performance across large datasets
- Isolate the warehouse from upstream source system changes
- Enable support for Slowly Changing Dimensions (SCD)
- Maintain consistent historical reporting

---

## 🔄 ETL Process

### Step 1 — Load Staging Tables
Raw CSV data is inserted into staging tables for transformation.

### Step 2 — Populate Dimension Tables
Distinct records are extracted from staging into each dimension:

```sql
INSERT INTO dim_customer (customer_id, age, gender)
SELECT DISTINCT customer_id, age, gender
FROM staging_customers;
```

### Step 3 — Populate Date Dimension
Year, month, day, and quarter are derived from the transaction date and stored in `dim_date`.

### Step 4 — Load Fact Table
Business keys are mapped to surrogate keys via joins before inserting into `fact_sales`:

```sql
INSERT INTO fact_sales (sale_id, date_key, customer_key, product_key, city_key,
                        quantity, unit_price, discount, revenue)
SELECT
    s.sale_id,
    d.date_key,
    c.customer_key,
    p.product_key,
    ci.city_key,
    s.quantity,
    s.unit_price,
    s.discount,
    (s.quantity * s.unit_price) - s.discount AS revenue
FROM staging_sales s
JOIN dim_customer c  ON s.customer_id = c.customer_id
JOIN dim_product p   ON s.product_id  = p.product_id
JOIN dim_city ci     ON s.city        = ci.city_name
JOIN dim_date d      ON s.date        = d.full_date;
```

---

## 📈 KPIs & Analytics

The warehouse is designed to answer the following business questions:

| KPI | Description |
|-----|-------------|
| 💰 Total Revenue | Overall sales performance |
| 📦 Total Orders | Volume of transactions |
| 📊 Avg Order Value | Revenue per transaction |
| 📅 Revenue by Year | Year-over-year growth |
| 🏙 Revenue by City | Best-performing regions |
| 👤 Top 5 Customers | Highest value customers |
| 📦 Revenue by Product | Top-selling products |
| 📆 Monthly Revenue Trend | Seasonality analysis |
| 🚻 Revenue by Gender | Demographic breakdown |

**Example — Revenue by Year:**
```sql
SELECT d.year, SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;
```

---

## ⚡ Performance Optimization

Indexes are added on all foreign key columns in the fact table to accelerate analytical queries:

```sql
CREATE INDEX idx_date     ON fact_sales(date_key);
CREATE INDEX idx_customer ON fact_sales(customer_key);
CREATE INDEX idx_product  ON fact_sales(product_key);
CREATE INDEX idx_city     ON fact_sales(city_key);
```

---

## 🛠 Tech Stack

| Tool | Purpose |
|------|---------|
| **MySQL 8.0** | Database engine |
| **SQL** | ETL, transformation, KPI queries |
| **Star Schema** | Dimensional modeling pattern |
| **CSV** | Source data format |

---

## 🚀 Getting Started

### Prerequisites
- MySQL 8.0+
- A MySQL client (MySQL Workbench, DBeaver, or CLI)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/danish0406/Retail-SalesDWH.git
   cd Retail-SalesDWH
   ```

2. **Create the database**
   ```sql
   CREATE DATABASE retail_sdw;
   USE retail_sdw;
   ```

3. **Run the schema scripts**
   ```bash
   mysql -u root -p sales_dw < sql/01_staging.sql
   mysql -u root -p sales_dw < sql/02_dimensions.sql
   mysql -u root -p sales_dw < sql/03_fact.sql
   ```

4. **Load your source data into staging tables and run the ETL**
   ```bash
   mysql -u root -p sales_dw < sql/04_etl.sql
   ```

5. **Run KPI queries**
   ```bash
   mysql -u root -p sales_dw < sql/05_kpis.sql
   ```

---

## 🧠 Skills Demonstrated

`Dimensional Modeling` · `Star Schema Design` · `Surrogate Key Implementation` · `ETL Workflow Design` · `SQL Joins & Aggregations` · `Data Transformation` · `KPI Development` · `Query Optimization`

---

## 🔮 Future Improvements

- [ ] Implement **Slowly Changing Dimensions (SCD Type 2)**
- [ ] Add **Incremental ETL** loading strategy
- [ ] Connect a **Power BI / Tableau** dashboard
- [ ] Automate pipeline with **Apache Airflow**
- [ ] Containerize the environment using **Docker**
- [ ] Add **data validation and error logging** layer

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

<p align="center">Built with 💡 as a demonstration of Data Warehousing & Dimensional Modeling best practices</p>
