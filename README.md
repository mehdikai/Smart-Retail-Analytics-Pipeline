Smart Retail Analytics Pipeline
   
A comprehensive data integration and analytics system for retail intelligence, combining sales, marketing campaigns, web traffic, and IoT sensor data.
Table of Contents
•	Overview
•	Features
•	Architecture
•	Installation
•	Usage
•	Project Structure
•	Data Sources
•	Results
•	Technologies
•	Contributing
•	License
•	Author
Overview
Smart Retail Analytics Pipeline is an automated data processing system that:
•	Integrates multiple data sources (SQLite, CSV, JSON)
•	Normalizes and cleans raw data
•	Federates datasets through intelligent joins
•	Visualizes key business metrics
•	Orchestrates daily automated executions
Built as part of a Data Architecture course project, this pipeline demonstrates modern ETL practices and data engineering principles.
Features
Data Integration
•	Multi-source data loading (SQLite, CSV, JSON)
•	Robust error handling and validation
•	Configurable data paths via YAML
Data Normalization
•	Automatic date parsing and formatting
•	Type conversion (strings to numbers, dates)
•	Missing value handling
•	Data quality validation (488 invalid campaigns filtered)
Data Federation
•	Smart joins: Orders ↔ Marketing campaigns (by product_id + date)
•	Web traffic integration (by date)
•	IoT sensor data aggregation
•	4 aggregation views: daily sales, campaigns, products, countries
Visualizations
1.	Daily Sales Evolution - Time series with trend line
2.	Top Marketing Campaigns - Horizontal bar chart (Top 15)
3.	Traffic vs Sales Correlation - Dual-axis plot + scatter plot
Orchestration
•	Automated daily execution at 19:45 GMT
•	Email notifications with reports
•	Comprehensive logging system
•	Error handling and retries
Installation
Prerequisites
•	Python 3.8+
•	pip package manager
Setup
1.	Clone the repository
git clone https://github.com/mehdikai/smart-retail-analytics.git
cd smart-retail-analytics
2.	Install dependencies
pip install -r requirements.txt
3.	Create directory structure
mkdir -p data/processed outputs/figures outputs/report logs
4.	Configure orchestration (optional) Edit orchestration.py to set your email credentials:
SENDER_EMAIL = "your_email@gmail.com"
SENDER_PASSWORD = "your_app_password"
Usage
Run Complete Pipeline
python main.py
Test Individual Modules
# Test data integration
python integration.py

# Test normalization
python normalisation.py

# Test federation
python federation.py

# Test visualizations
python visualisation.py
Start Orchestration Daemon
# Continuous mode (runs daily at 19:45 GMT)
python orchestration.py

# Test mode (run immediately)
python orchestration.py --test

# Data Sources
1. Orders (SQLite)
•	Source: smartretail.db
•	Table: orders
•	Columns: order_date, country, product_id, quantity, total_amount
•	Records: 1,000 orders
2. Marketing Campaigns (CSV)
•	Source: marketing.csv
•	Columns: campaign_name, product_id, start_date, end_date
•	Records: 1,000 campaigns → 512 after validation
3. Web Traffic (JSON)
•	Source: web_traffic.json
•	Columns: date, pageviews, sessions, source
•	Records: 1,000 daily metrics
4. IoT Stream (CSV)
•	Source: iot_stream.csv
•	Columns: timestamp, footfall, temperature
•	Records: 1,000 sensor readings

# Results
Pipeline Execution Stats
•	Total Execution Time: 11.25 seconds
•	Final Dataset: 10,200 rows × 14 columns
•	Data Period: January 1, 2024 → December 31, 2024
•	Visualizations: 3 high-resolution charts (300 DPI)
Key Insights
Top 5 Marketing Campaigns
1.	Konklab: €44,004.73 (89 orders)
2.	Ventosanzap: €39,375.61 (74 orders)
3.	Y-Solowarm: €38,926.46 (79 orders)
4.	Cookley: €35,377.78 (76 orders)
5.	Zaam-Dox: €35,037.73 (74 orders)
Top 5 Products
1.	Product #49: €205,468.20 (1,650 units)
2.	Product #36: €191,650.72 (2,336 units)
3.	Product #6: €182,477.75 (1,950 units)
4.	Product #34: €169,038.24 (1,736 units)
5.	Product #21: €160,185.22 (1,768 units)
Top 5 Countries
1.	China: €897,217.27 (1,836 orders)
2.	Indonesia: €592,044.18 (1,156 orders)
3.	Russia: €357,962.90 (761 orders)
4.	Philippines: €280,209.40 (552 orders)
5.	France: €220,652.33 (425 orders)
Visualizations
1. Daily Sales Evolution
 <img width="4172" height="1770" alt="ventes_par_jour" src="https://github.com/user-attachments/assets/e310e1bf-f745-439e-bd82-413b72c8f822" />
•	Clear upward trend visible
•	Average daily revenue: ~€15,000
2. Top Marketing Campaigns
 <img width="3558" height="2369" alt="ventes_par_campagne" src="https://github.com/user-attachments/assets/d07012b9-9409-4a3e-bf1d-88179facbf08" />
•	3,359 orders linked to active campaigns
•	6,841 orders without campaigns
3. Traffic vs Sales Correlation
 <img width="4172" height="2963" alt="trafic_vs_ventes" src="https://github.com/user-attachments/assets/39b22673-2195-4d1b-b4f5-2c8e77c1d3e9" />
•	Correlation coefficient: r = 0.047 (weak correlation)
•	Indicates other factors drive sales beyond web traffic
Technologies
Core
•	Python 3.8+ - Programming language
•	Pandas - Data manipulation
•	NumPy - Numerical computing
Data Storage
•	SQLite3 - Relational database
•	JSON - Web traffic data format
•	CSV - Marketing & IoT data format
Visualization
•	Matplotlib - Chart generation
•	Seaborn - Statistical visualizations
Configuration & Scheduling
•	PyYAML - Configuration management
•	Schedule - Job scheduling
•	pytz - Timezone handling
Email
•	smtplib - Email sending (SMTP)
Contributing
Contributions are welcome! Please feel free to submit a Pull Request.
1.	Fork the project
2.	Create your feature branch (git checkout -b feature/AmazingFeature)
3.	Commit your changes (git commit -m 'Add some AmazingFeature')
4.	Push to the branch (git push origin feature/AmazingFeature)
5.	Open a Pull Request
License
This project is licensed under the MIT License - see the LICENSE file for details.

# Author
EL KAISSOUNI EL MEHDI
•	GitHub: @mehdikai
•	Email: kaissounim61@gmail.com
________________________________________
Acknowledgments
•	Mockaroo.com - Test data generation
•	Python Community - Amazing libraries and tools
________________________________________
If you found this project useful, please consider giving it a star!
Last updated: November 16, 2024

