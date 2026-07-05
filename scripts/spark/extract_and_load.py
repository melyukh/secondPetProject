from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, count, round, to_date, avg, broadcast, lit, coalesce, length
import os
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.environ.get("DATA_DIR")

DB = os.environ.get("PG_DATA_DB")
password = os.environ.get("PG_DATA_PASSWORD")
user = os.environ.get("PG_DATA_USER")

jdbc_url = f"jdbc:postgresql://{os.environ.get("POSTGRES_HOST")}:{os.environ.get("POSTGRES_CONTAINER_PORT")}/{DB}"

properties ={
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}


spark = SparkSession.builder.appName("huita") \
    .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

flights_schema = StructType([
    StructField("flight_date", StringType(),nullable=True),
    StructField("day_of_week", StringType(),nullable=True),
    StructField("airline", StringType(),nullable=True),
    StructField("flight_number", StringType(),nullable=True),
    StructField("tail_number", StringType(),nullable=True),
    StructField("origin_airport", StringType(),nullable=True),
    StructField("destination_airport", StringType(),nullable=True),

    StructField("departure_delay", DoubleType(),nullable=True),
    StructField("distance", DoubleType(),nullable=True),
    StructField("arrival_delay", DoubleType(),nullable=True),

    StructField("diverted", IntegerType(),nullable=True),
    StructField("cancelled", IntegerType(),nullable=True),

    StructField("cancellation_reason", StringType(),nullable=True),

    StructField("air_system_delay", DoubleType(),nullable=True),
    StructField("security_delay", DoubleType(),nullable=True),
    StructField("airline_delay", DoubleType(),nullable=True),
    StructField("late_aircraft_delay", DoubleType(),nullable=True),
    StructField("weather_delay", DoubleType(),nullable=True),

    StructField("departure_hour", DoubleType(),nullable=True),
    StructField("arrival_hour", DoubleType(),nullable=True)
])

flights_df = spark.read.csv(f"{DATA_DIR}/aviadata/flights_pak.csv", header=True, schema=flights_schema) \
    .fillna({
        "air_system_delay": 0,
        "security_delay": 0,
        "airline_delay": 0,
        "late_aircraft_delay": 0,
        "weather_delay": 0,
        "diverted": False,
        "cancelled": False,
        "departure_delay": 0,
        "arrival_delay": 0
    }) \
    .dropna(subset=["flight_date", "day_of_week", "airline", "flight_number", "tail_number", "origin_airport", "destination_airport"]) \
    .withColumn("flight_number", col("flight_number").cast("int")) \
    .withColumn("departure_hour", col("departure_hour").cast("int")) \
    .withColumn("arrival_hour", col("arrival_hour").cast("int")) \
    .withColumn("departure_delay", col("departure_delay").cast("int")) \
    .withColumn("arrival_delay", col("arrival_delay").cast("int")) \
    .withColumn("air_system_delay", col("air_system_delay").cast("int")) \
    .withColumn("security_delay", col("security_delay").cast("int")) \
    .withColumn("airline_delay", col("airline_delay").cast("int")) \
    .withColumn("late_aircraft_delay", col("late_aircraft_delay").cast("int")) \
    .withColumn("weather_delay", col("weather_delay").cast("int")) \
    .withColumn("flight_date", to_date(col("flight_date"), 'yyyy-MM-dd')) \
    .withColumn("cancelled", col("cancelled").cast("boolean")) \
    .withColumn("diverted", col("diverted").cast("boolean")) \
    .filter(col("departure_hour").between(0, 24)) \
    .filter(length(col("origin_airport")) == 3) \
    .filter(length(col("destination_airport")) == 3) \
    .filter(length(col("airline")) == 2) \
    .withColumn("day_part", when((col("departure_hour").between(0, 5)) | (col("departure_hour") == 24),"Night")
                            .when(col("departure_hour").between(6, 11), "Morning")
                            .when(col("departure_hour").between(12, 17), "Afternoon")
                            .when(col("departure_hour").between(18, 23), "Evening")) \
    .withColumn("is_long_haul", when(col("distance") > 1000, True)
                            .otherwise(False)) \
    .withColumn("day_of_week",   when(col("day_of_week") == "Sunday", 0)
                                .when(col("day_of_week") == "Monday", 1)
                                .when(col("day_of_week") == "Tuesday", 2)
                                .when(col("day_of_week") == "Wednesday", 3)
                                .when(col("day_of_week") == "Thursday", 4)
                                .when(col("day_of_week") == "Friday", 5)
                                .when(col("day_of_week") == "Saturday", 6)
                                .otherwise(-1)) \
    .persist()

flights_df.count()
                                
airports_schema = StructType([
    StructField("iata", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False)
])

airports_df = spark.read.csv(f"{DATA_DIR}/aviadata/airports.csv", header=True, schema=airports_schema)

airlines_schema = StructType([
    StructField("iata", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False)
])

airlines_df = spark.read.csv(f"{DATA_DIR}/aviadata/airlines.csv", header=True, schema=airlines_schema)

#топ-5 авиалиний с наибольшей задержкой
average_of_delay_time = flights_df \
    .groupBy(col("airline")) \
    .agg(avg(col("departure_delay")).alias("average_delay")) \
    .select("airline", "average_delay")

top_5_by_delays = average_of_delay_time.join(broadcast(airlines_df), airlines_df["iata"] == average_of_delay_time["airline"]) \
    .select("iata", "name", "average_delay") \
    .orderBy(col("average_delay").desc())


#процент отмененных рейсов для каждого аэропорта
count_of_cancelled_and_total = flights_df \
    .groupBy(flights_df["origin_airport"]) \
    .agg(
            count(lit(1)).alias("total"), 
            count(when(col("cancelled") == True, 1)).alias("cancelled_total")
        ) \
    .select("origin_airport", "total", "cancelled_total")


percent_for_every_origin_airport = count_of_cancelled_and_total.join(broadcast(airports_df), airports_df["iata"] == count_of_cancelled_and_total["origin_airport"], "right") \
    .select(airports_df["iata"], airports_df["name"], round(coalesce(col("cancelled_total") / col("total") * lit(100), lit(0)), 2).alias("percent_of_cancelled")) \
    .orderBy(col("percent_of_cancelled").desc())

#какое время суток чаще всего связано с задержками и отменами
day_part_with_delays = flights_df.filter((col("departure_delay") > 0) | (col("cancelled") == True)) \
    .groupBy(col("day_part")) \
    .agg(count(lit(1)).alias("count_of_delays_and_cancellations")) \
    .select("day_part", "count_of_delays_and_cancellations") \
    .orderBy(col("count_of_delays_and_cancellations").desc()) 


print("топ-5 авиалиний с наибольшей задержкой")
top_5_by_delays.show()

print()
print("процент отмененных рейсов для каждого аэропорта")
percent_for_every_origin_airport.show(50)

print()
print("какое время суток чаще всего связано с задержками и отменами")
day_part_with_delays.show()
    
#запись в постгрес
airports_df.drop_duplicates(subset=["name"]) \
        .select("name") \
        .write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(
            url=jdbc_url,
            table="temp_cities",
            properties=properties 
        )

airports_df.select("iata", "name", "city", "latitude", "longitude") \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(
    url=jdbc_url,
    table="temp_airports",
    
    properties=properties
)

airlines_df\
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(
    url=jdbc_url,
    table="temp_airlines",
    mode="overwrite",
    properties=properties
)

flights_df.unpersist()

flights_df \
    .drop("day_part") \
    .repartition(10) \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .option("batchsize", "5000") \
    .jdbc(
        url=jdbc_url,
        table="temp_flights",
        properties=properties
    )