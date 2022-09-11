# %% [markdown]
# # Perth Weather Consumer - CSV

# %% [markdown]
# The following consumer will consumer the message and output two csvs: 
# 1. The average temperature in Perth over the past x hours
# 2. THe top 3 frequent weather text in Perth over the past x hours.
# 
# ### Table of Content: 
# * [Initialisation](#ONE)
# * [Extract data from the stream](#TWO)
# * [Transform the dataset](#THREE)
# * [Load the dataset for data analysis](#FOUR)
# * [Output result to CSV](#FIVE)
# 

# %% [markdown]
# ## Initialisation<a class="anchor" id="ONE"></a>
# Initialise the pyspark session and import libraries

# %%
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import sleep

spark = SparkSession \
    .builder \
    .appName("Weather Analysis") \
    .getOrCreate()

# %%
# Specify configuration and location
time_window = "12 seconds"
watermark = "6 seconds"
output_location = os.path.join(os.getcwd(), "result")

if not os.path.exists(output_location):
    os.makedirs(output_location)

# %% [markdown]
# ## Extract data from the stream<a class="anchor" id="TWO"></a>
# Subscribe to the topic and load the data from the stream 

# %%
topic = "Perth"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", topic) \
    .load()

# %%
df.printSchema()

# %% [markdown]
# ## Transform the dataset<a class="anchor" id="THREE"></a>
# Transform the dataset based on requirepments for analytical usage.  
# 
# Steps: 
# * [1 Convert the key/value from the kafka data stream to string](#1)
# * [2 Cast dataframe based on the schema](#2)
# * [3. Remove nested structure of the temperature](#3)
# * [4. Focuse on the temperature in Celsius](#4)
# * [5. Continue removing nested structure of the temperature](#5)
# * [6. Remove unwanted rows](#6)
# * [7. Rename and change the data type of the temperature column](#7)
# * [8. Data Wrangling](#8)

# %% [markdown]
# ### 1. Convert the key/value from the kafka data stream to string<a class="anchor" id="1"></a>

# %%
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# %%
df.printSchema()

# %% [markdown]
# ### 2. Cast dataframe based on the schema<a class="anchor" id="2"></a>

# %%
schema = StructType([
    StructField("Time", TimestampType(), True), 
    StructField("city", StringType(), True), 
    StructField("WeatherText", StringType(), True),
    StructField("Temperature", MapType(StringType(), MapType(StringType(), StringType())), True)        
])

df1=df.select(F.from_json(F.col("value").cast("string"), schema).alias('parsed_value'))

# %%
df1.printSchema()

# %% [markdown]
# ### 3. Remove nested structure of the temperature<a class="anchor" id="3"></a>

# %%
df2 = df1.select(
    F.col("parsed_value.Time").alias("Time"),
    F.col("parsed_value.city").alias("City"),
    F.col("parsed_value.WeatherText").alias("WeatherText"),
    explode("parsed_value.Temperature")
)

# %%
df2.printSchema()

# %% [markdown]
# ### 4. Focuse on the temperature in Celsius<a class="anchor" id="4"></a>
# Here, I only focus on the temperature in Celsius

# %%
df3 = df2.filter(df2.key == "Metric")

# %% [markdown]
# ### 5. Continue removing nested structure of the temperature<a class="anchor" id="5"></a>

# %%
df4 = df3.select("Time", 
                 "City",
                 "WeatherText",
                 explode("value")
)

# %%
df4.printSchema()

# %% [markdown]
# ### 6. Remove unwanted rows<a class="anchor" id="6"></a>
# We only want the value of the temperature, not the symbol C

# %%
df5 = df4.filter(df4.key == "Value")

# %% [markdown]
# ### 7. Rename and change the data type of the temperature column<a class="anchor" id="7"></a>

# %%
df6 = df5.select("Time", 
                 "City", 
                 "WeatherText", 
                 F.col("value").alias("TemperatureC"))

df_formatted = df6.withColumn("TemperatureC", df6.TemperatureC.cast(DoubleType()))

# %%
df_formatted.printSchema()

# %% [markdown]
# ### 8. Data Wrangling<a class="anchor" id="8"></a>
# Remove excessive spaces in some of the text columns

# %%
df_formatted2 = df_formatted.select("Time",
                                    F.trim(F.initcap(F.col("City"))).alias("City"), 
                                    F.trim(F.lower(F.col("WeatherText"))).alias("WeatherText"),
                                    F.col("TemperatureC"))

# %%
df_formatted2.printSchema()

# %% [markdown]
# ## Load the dataset for data analysis<a class="anchor" id="FOUR"></a>
# ### Average Temperature
# 
# Every second, the average temperature will be calculated.   
# Watermark has been set to allow data that is coming late. 

# %%
Avg_temp = df_formatted2\
    .withWatermark("Time", watermark)\
    .groupBy("City", 
             F.window(df_formatted2.Time, time_window, "1 second"))\
    .agg(F.avg("TemperatureC").alias("AvgTempC"))\
    .sort(F.col("window").desc())

# %%
Avg_temp.printSchema()

# %%
Avg_temp = Avg_temp.withColumn("AvgTempC", F.round(Avg_temp["AvgTempC"], 2))

# %%
# # Debug purpose
# query = Avg_temp \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='12 seconds') \
#     .option("truncate", False) \
#     .start()

# %%
# query.stop()

# %% [markdown]
# ### Frequent weather text
# 
# Every second, the frequency of the weather text has been generated.   
# Watermark has been set to allow data that is coming late. 

# %%
Weather_text = df_formatted2\
    .withWatermark("Time", watermark)\
    .groupBy("City", 
             "WeatherText", 
             F.window(df_formatted2.Time, time_window, "1 second")).agg(
    F.count("WeatherText").alias("Count"))

# %%
Weather_text = Weather_text.sort(
    F.col("window").desc(), 
    F.col("Count").desc())

# %%
# # Debug purpose
# query = Weather_text \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='12 seconds') \
#     .option("truncate", False) \
#     .start()

# %%
# query.stop()

# %%
Weather_text.printSchema()

# %% [markdown]
# ## Output result to CSV<a class="anchor" id="FIVE"></a>
# Once happy with the result, we can output to csv

# %% [markdown]
# ### Average Temperature

# %%
query1 = Avg_temp \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("temp_query") \
    .trigger(processingTime='12 seconds') \
    .start()

# To get enough data for our csv
print("Sleeping...")
sleep(60)
print("Waking Up...")

query1.stop()

# %%
spark.sql("SELECT * FROM temp_query limit 10").toPandas().head(10)

# %%
spark.sql("SELECT * FROM temp_query").toPandas().to_csv(
    os.path.join(output_location, "avg_temp.csv"), 
    index = False)

# %% [markdown]
# ### Top Frequent Weather Text

# %%
query2 = Weather_text \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("weather_query") \
    .trigger(processingTime='12 seconds') \
    .start()

# To get enough data for our csv
print("Sleeping...")
sleep(60)
print("Waking Up...")
    
query2.stop()

# %%
# Return the top 3 frequent weather text in each city and window frame. 
spark.sql('''
    SELECT City, 
        WeatherText, 
        window, 
        Count
    FROM (
        SELECT *, 
            row_number() OVER (PARTITION BY City, window order BY Count desc) AS rn
    FROM weather_query) temp
    WHERE rn <= 3
    ORDER BY window DESC, rn
    LIMIT 30
    ''').toPandas()

# %%
spark.sql('''
    SELECT City, 
        WeatherText, 
        window, 
        Count
    FROM (
        SELECT *, 
            row_number() OVER (PARTITION BY City, window order BY Count desc) AS rn
    FROM weather_query) temp
    WHERE rn <= 3
    ORDER BY window DESC, rn
    ''').toPandas().to_csv(os.path.join(output_location, "weather_text.csv"), index = False)

# %%



