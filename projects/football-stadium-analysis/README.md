# 🏟️ Football Stadium Analysis

This project extracts, transforms, analyzes, and visualizes global football stadium data using a modern data engineering and analytics workflow.

---

## 📊 Project Summary

- **Source**: Wikipedia (largest football stadiums)
- **ETL Tools**: Python, BeautifulSoup, Pandas, Geopy
- **Data Pipeline**: Apache Airflow
- **Storage**: Azure Data Lake Storage Gen2 (ADLS Gen2)
- **Visualization**: Power BI
- **Analytics**: DuckDB SQL on CSV

---

## 🧰 Technologies Used

- Python
- Apache Airflow
- DuckDB (for running SQL on CSVs)
- Azure Data Lake Gen2
- Power BI
- Git & GitHub

---

## 📁 Project Structure

```bash
.
├── pipelines/
│   └── wikipedia_pipeline.py       # ETL logic
├── Data/
│   └── stadium_cleaned....csv      # Cleaned dataset
├── query_stadiums.py              # SQL queries using DuckDB
├── Football_Stadiums-Dashboard.pbix                 # Power BI Dashboard (excluded in .gitignore)
├── README.md
└── .gitignore

⚙️ How It Works
1. Extract
Scrapes football stadium data from Wikipedia using requests + BeautifulSoup.

2. Transform
Cleans up text

Adds geolocation coordinates

Removes duplicates

Saves clean CSV file

3. Load
Saves cleaned data to Azure ADLS Gen2

4. Analyze (DuckDB)
Top 10 stadiums by capacity

Capacity by region

Regional ranking using SQL window functions

5. Visualize (Power BI)
🌍 Geographical distribution map

📋 Table: Top 10 by rank & capacity

📊 Bar chart: Stadiums by region

🥧 Pie chart: Capacity share


🚀 How to Run
Clone the repo:

bash
Copy
Edit
git clone https://github.com/Martin-7251/football-stadium-analysis.git
cd football-stadium-analysis
Install dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Run ETL:

bash
Copy
Edit
# or trigger Airflow DAG
python pipelines/wikipedia_pipeline.py
Query the CSV:

bash
Copy
Edit
python query_stadiums.py
Open dashboard.pbix in Power BI Desktop to view visuals.

🙈 Excluded from Git
.pbix files (too large)

Azure keys / secrets

Raw data files

✍️ Author
Martin Kamau
GitHub • LinkedIn