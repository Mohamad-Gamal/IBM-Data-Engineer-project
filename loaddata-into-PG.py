import psycopg2
import pandas as pd

# --- Load CSV files ---
df_date = pd.read_csv("DimDate.csv")
df_category = pd.read_csv("DimCategory.csv")
df_country = pd.read_csv("DimCountry.csv")
df_sales = pd.read_csv("FactSales.csv")

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(
    host="172.21.216.25",
    dbname="softcart",
    user="postgres",
    password="HKTDzbsaDIM1rRnxHfhGEKgr"
)
cur = conn.cursor()

# --- Load DimDate ---
for _, row in df_date.iterrows():
    cur.execute("""
        INSERT INTO staging."softcartDimDate" 
        (dateid, date, year, quarter, quartername, month, monthname, weekday, weekdayname, day)
        OVERRIDING SYSTEM VALUE
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (dateid) DO NOTHING;
    """, (
        int(row['dateid']),
        row['date'],
        int(row['Year']),
        int(row['Quarter']),
        row['QuarterName'],
        int(row['Month']),
        row['Monthname'],
        int(row['Weekday']),
        row['WeekdayName'],
        int(row['Day'])
    ))

# --- Load DimCategory (no subcategory) ---
for _, row in df_category.iterrows():
    cur.execute("""
        INSERT INTO staging."softcartDimCategory" (categoryid, category)
        OVERRIDING SYSTEM VALUE
        VALUES (%s, %s)
        ON CONFLICT (categoryid) DO NOTHING;
    """, (
        int(row['categoryid']),
        row['category']
    ))

# --- Load DimCountry ---
for _, row in df_country.iterrows():
    cur.execute("""
        INSERT INTO staging."softcartDimCountry" (countryid, country)
        OVERRIDING SYSTEM VALUE
        VALUES (%s, %s)
        ON CONFLICT (countryid) DO NOTHING;
    """, (
        int(row['countryid']),
        row['country']
    ))



# --- Load FactSales ---
for _, row in df_sales.iterrows():
    cur.execute("""
        INSERT INTO staging."softcartFactSales" 
        (orderid, dateid, categoryid, itemid, countryid, price)
        OVERRIDING SYSTEM VALUE
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (orderid) DO NOTHING;
    """, (
        int(row['orderid']),
        int(row['dateid']),
        int(row['categoryid']),
        -1,                   # itemid missing in CSV
        int(row['countryid']),
        float(row['amount'])     # amount maps to price
    ))

# --- Commit and close ---
conn.commit()
cur.close()
conn.close()

print("✅ Data successfully loaded into all staging tables.")
