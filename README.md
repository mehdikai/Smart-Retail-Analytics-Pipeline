# 🛒 Smart Retail Analytics Pipeline

A unified data engineering pipeline designed for retail analytics, integrating sales, marketing campaigns, web traffic, and IoT sensor data into a complete, automated ecosystem.

---

## 📚 Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Results](#results)
- [Technologies](#technologies)
- [Contributing](#contributing)
- [License](#license)
- [Author](#author)
- [Acknowledgments](#acknowledgments)

---

## 📌 Overview

**Smart Retail Analytics Pipeline** is a full data workflow that:

- Integrates heterogeneous data sources (SQLite, CSV, JSON)
- Performs cleaning, validation, and normalization
- Federates datasets through intelligent joins and aggregations
- Produces actionable visual insights
- Runs automatically through a daily orchestration system

The project was developed as part of a **Data Architecture course**, showcasing modern ETL and data engineering principles.

---

## 🚀 Features

### 🔹 Data Integration
- Multi-source ingestion (SQLite, CSV, JSON)
- Configurable paths via `config.yaml`
- Validation + error-handling for corrupted inputs

### 🔹 Data Normalization
- Standardized date parsing
- Type conversions (numbers, dates, strings)
- Missing-value handling
- Campaign validation (removes 488 invalid entries)

### 🔹 Data Federation
- Orders ↔ Campaigns join (product + date logic)
- Sales ↔ Web traffic merge
- IoT aggregation by timestamp
- 4 analytics views: daily, product, campaign, country

### 🔹 Visualizations (3 Charts)
1. **Daily Sales Evolution**
2. **Top Marketing Campaigns**
3. **Traffic vs Sales Correlation**

### 🔹 Orchestration
- Automatic execution every day at **19:45 GMT**
- Error retries + logging
- Report generation
- Optional email notifications

---

## 🏗️ Architecture

main.py → integration → normalisation → federation → visualisation → outputs → logs

---

## 🛠️ Installation

### **Prerequisites**
- Python 3.8+
- pip


git clone https://github.com/mehdikai/smart-retail-analytics.git
cd smart-retail-analytics
pip install -r requirements.txt

Edit in orchestration.py:
SENDER_EMAIL = "your_email@gmail.com"
SENDER_PASSWORD = "your_app_password"

Run full pipeline:
python main.py

Run modules individually:
python integration.py
python normalisation.py
python federation.py
python visualisation.py

Start orchestration:
python orchestration.py          # Daily scheduled run
python orchestration.py --test   # Immediate run

### Project Structure :

<img width="500" height="500" alt="image" src="https://github.com/user-attachments/assets/1279751a-870b-4e77-b9e8-61e314ac8032" />

### Technologies :
Core
Python 3.8+
Pandas, NumPy
Storage
SQLite
CSV, JSON
Visualization
Matplotlib
Seaborn
Config & Scheduling
PyYAML
schedule
pytz
Email
smtplib

---

⭐ If you found this project useful, consider giving it a star!
Last updated: November 16, 2024

