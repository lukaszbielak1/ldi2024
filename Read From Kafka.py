# Databricks notebook source
# MAGIC %run ./SetupConnection 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DecimalType,IntegerType, LongType, DoubleType, ArrayType
from pyspark.sql.functions import unbase64, col, from_json, explode, to_timestamp

# COMMAND ----------

schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("supply", StringType(), True),
        StructField("maxSupply", StringType(), False),
        StructField("marketCapUsd", StringType(), True),
        StructField("volumeUsd24Hr", StringType(), True),
        StructField("priceUsd", StringType(), True),
        StructField("changePercent24Hr", StringType(), True),
        StructField("vwap24Hr", StringType(), True),
        StructField("explorer", StringType(), True)
    ])), True),
    StructField("timestamp", LongType(), True)
])

# COMMAND ----------

CLOUDKARAFKA_BROKERS = dbutils.secrets.get("key vault", "CLOUDKARAFKA-BROKERS")
CLOUDKARAFKA_USERNAME = dbutils.secrets.get("key vault", "CLOUDKARAFKA-USERNAME")
CLOUDKARAFKA_PASSWORD = dbutils.secrets.get("key vault", "CLOUDKARAFKA-PASSWORD")

# COMMAND ----------

df_bronze = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers",  CLOUDKARAFKA_BROKERS) \
      .option(f"kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{CLOUDKARAFKA_USERNAME}'password='{CLOUDKARAFKA_PASSWORD}';") \
      .option("subscribe", "lzwzwvyh-cogncap") \
      .option("group.id","username-consumer") \
      .option("startingOffsets", "earliest") \
      .option("kafka.ssl.endpoint.identification.algorithm", "https") \
      .option("kafka.security.protocol","SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
      .load() 
            



df_bronze.writeStream \
  .format("delta") \
  .outputMode("append") \
  .trigger(processingTime='30 seconds') \
  .option("checkpointLocation", f"""{adls_path}/checkpoints/bronze/crypto/""") \
  .start(f"""{adls_path}/bronze/crypto""")

# COMMAND ----------

df_silver =  spark.readStream.format("delta")\
  .load(f"""{adls_path}/bronze/crypto""")\
  .withColumn("str_value", col('value').cast('String')) \
  .withColumn("json", from_json(col("str_value"),schema)) \
  .withColumn("original_timestamp",to_timestamp(col("json.timestamp")/1000)) \
  .select("topic","partition","offset","timestamp",explode("json.data"),"original_timestamp")\
  .select("topic","partition","offset","timestamp","col.*","original_timestamp")\
  .withColumnRenamed("timestamp","kafka_timestamp")\
  .withColumn("supply", col("supply").cast(DecimalType(24,4)))\
  .withColumn("maxSupply", col("maxSupply").cast(DecimalType(24,4)))\
  .withColumn("marketCapUsd", col("marketCapUsd").cast(DecimalType(24,4)))\
  .withColumn("volumeUsd24Hr", col("volumeUsd24Hr").cast(DecimalType(24,4)))\
  .withColumn("priceUsd", col("priceUsd").cast(DecimalType(24,4)))\
  .withColumn("changePercent24Hr", col("changePercent24Hr").cast(DecimalType(24,4)))\
  .withColumn("vwap24Hr", col("vwap24Hr").cast(DecimalType(24,4)))

df_silver.writeStream \
  .format("delta") \
  .outputMode("append") \
  .trigger(processingTime='30 seconds') \
  .option("checkpointLocation", f"""{adls_path}/checkpoints/silver/crypto/""") \
  .start(f"""{adls_path}/silver/crypto""")

# COMMAND ----------

df_gold = spark.readStream.format("delta")\
          .load(f"""{adls_path}/silver/crypto""")\
          .withColumn("price_usd",col("priceUsd"))\
          .select("id","symbol","name","price_usd","original_timestamp")


df_gold.writeStream \
  .format("delta") \
  .outputMode("append") \
  .trigger(processingTime='30 seconds') \
  .option("checkpointLocation", f"""{adls_path}/checkpoints/gold/crypto/""") \
  .start(f"""{adls_path}/gold/crypto""")

# COMMAND ----------

# Create or replace bronze_crypto table
display(spark.sql(f"""
  CREATE OR REPLACE TABLE bronze_crypto (
    key BINARY,
    value BINARY,
    topic STRING,
    partition INT,
    offset LONG,
    timestamp TIMESTAMP,
    timestampType INT
  )
  USING DELTA
  LOCATION '{adls_path}/bronze/crypto'
"""))

# COMMAND ----------

# Create or replace silver_crypto table
display(spark.sql(f"""
  CREATE OR REPLACE TABLE silver_crypto (
    topic STRING,
    partition INT,
    offset LONG,
    kafka_timestamp TIMESTAMP,
    id STRING,
    rank INT,
    symbol STRING,
    name STRING,
    supply decimal(24,4),
    maxSupply decimal(24,4),
    marketCapUsd decimal(24,4),
    volumeUsd24Hr decimal(24,4),
    priceUsd decimal(24,4),
    changePercent24Hr decimal(24,4),
    vwap24Hr decimal(24,4),
    explorer STRING,
    original_timestamp TIMESTAMP
  )
  USING DELTA
  LOCATION '{adls_path}/silver/crypto'
"""))

# COMMAND ----------

# Create or replace gold_crypto table
display(spark.sql(f"""
  CREATE OR REPLACE TABLE gold_crypto (
    id STRING,
    symbol STRING,
    name STRING,
    price_usd decimal(24,4),
    original_timestamp TIMESTAMP
  )
  USING DELTA
  LOCATION '{adls_path}/gold/crypto'
"""))
