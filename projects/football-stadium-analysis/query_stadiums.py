import duckdb
import os
import pandas as pd

# Path to your CSV file
csv_file = r"c:\Users\Administrator\Documents\Football Project - DE\Data\stadium_cleaned2025-07-12_13,58,11.947843.csv"

# Create DuckDB connection
con = duckdb.connect()

# Output list to hold all DataFrames
output_sections = []

def append_section(title, query):
    df = con.execute(query).fetchdf()
    output_sections.append(pd.DataFrame([[title]], columns=[""]))   # Section title row
    output_sections.append(df)
    output_sections.append(pd.DataFrame())  # Blank line

# Queries

# Query 1: Top 10 stadiums with the highest capacity
append_section("Top 10 Stadiums by Capacity", f"""
SELECT stadium, country, capacity
FROM read_csv_auto('{csv_file}')
ORDER BY capacity DESC
LIMIT 10;
""")

# Query 2: Average capacity by region
append_section("Average Capacity by Region", f"""
SELECT region, AVG(capacity) as average_capacity
FROM read_csv_auto('{csv_file}')
GROUP BY region
ORDER BY average_capacity DESC;
""")

# Query 3: count the number of stadiums in each country
append_section("Number of Stadiums per Country", f"""
SELECT country, COUNT(*) AS stadium_count
FROM read_csv_auto('{csv_file}')
GROUP BY country
ORDER BY stadium_count DESC, country ASC;
""")

# Query 4: Stadium ranking by region using RANK()
append_section("Stadium Ranking by Region", f"""
SELECT *
FROM (
    SELECT 
        stadium,
        region,
        capacity,
        RANK() OVER (PARTITION BY region ORDER BY capacity DESC) AS regional_rank
    FROM read_csv_auto('{csv_file}')
)
WHERE regional_rank <= 3
ORDER BY region, regional_rank;
""")

# Query 5: stadiums with capacity above average
append_section("Stadiums Above Average Capacity in Region", f"""
SELECT *
FROM (
    SELECT
        stadium,
        region,
        country,
        capacity,
        AVG(capacity) OVER (PARTITION BY region) AS avg_capacity
    FROM read_csv_auto('{csv_file}')
)
WHERE capacity > avg_capacity 
ORDER BY capacity DESC;
""")

# Combine and write to CSV
final_report = pd.concat(output_sections, ignore_index=True)
output_path = r"c:\Users\Administrator\Documents\Football Project - DE\Data\stadium_analysis_output.csv"
final_report.to_csv(output_path, index=False)

print(f"\n✅ All analysis results saved to:\n{output_path}")
