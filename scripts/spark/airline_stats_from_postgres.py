from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, broadcast
import pandas as pd                 #type: ignore
import matplotlib.pyplot as plt     #type: ignore
import seaborn as sns               #type: ignore

import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DB = os.environ.get("PG_DATA_DB")
DB_USER = os.environ.get("PG_DATA_USER")
PASSWORD = os.environ.get("PG_DATA_PASSWORD")
DATA_DIR = os.environ.get("DATA_DIR")

DB_URL = f"jdbc:postgresql://{os.environ.get("POSTGRES_HOST")}:{os.environ.get("POSTGRES_CONTAINER_PORT")}/{DB}"

spark = SparkSession.builder \
    .appName("postgres_connection") \
    .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

flights = spark.read \
    .format("jdbc") \
    .options(
        url=DB_URL,
        dbtable="""
            (
            WITH cte AS
            (
                SELECT airline, CASE WHEN departure_hour <= arrival_hour THEN arrival_hour - departure_hour ELSE arrival_hour - departure_hour + 24 END AS flight_time
                FROM flights
            )
            SELECT airline, SUM(flight_time) as summary_time
            FROM cte
            GROUP BY airline 
            ) AS groupped_companies
        """,
        user=DB_USER,
        password=PASSWORD,
    ) \
    .option("driver", "org.postgresql.Driver") \
    .load()

companies = spark.read \
    .format("jdbc") \
    .options(
        url=DB_URL,  
        dbtable="airlines",
        user=DB_USER,
        password=PASSWORD
    ) \
    .option("driver", "org.postgresql.Driver") \
    .load()

flights.show()
companies.show()



result = flights.join(broadcast(companies), companies["iata"] == flights["airline"]) \
           .select(companies["name"], flights["summary_time"]) \
           .orderBy(col("summary_time").desc())

result.show(50)
result_df = result.toPandas()

sns.set_theme(style="whitegrid")


plt.figure(figsize=(12, 8))

barplot = sns.barplot(
    data=result_df, 
    x='summary_time', 
    y='name', 
    palette='viridis'
)

plt.title('Суммарное время полетов по авиакомпаниям', fontsize=16, fontweight='bold', pad=20)
plt.xlabel('Общее время (в часах)', fontsize=12)
plt.ylabel('Авиакомпания', fontsize=12)

for p in barplot.patches:
    width = p.get_width()
    plt.text(width + (result_df['summary_time'].max() * 0.01),
             p.get_y() + p.get_height() / 2,
             f'{int(width)}', # Значение времени
             ha="left", va="center", fontsize=10)

plt.tight_layout()

plt.savefig(f'{DATA_DIR}/graphics/airline_statistics.png', dpi=300)

print(f'{DATA_DIR}/graphics/airline_statistics.png')