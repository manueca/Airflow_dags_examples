from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from textblob import TextBlob
import random
from pyspark.sql.functions import rand, expr
import socket
from delta.tables import *
import subprocess
# Initialize Spark with Delta Lake
from pyspark.sql import SparkSession
min_latitude = 24.396308  # Southernmost point (Key West, Florida)
max_latitude = 49.384358  # Northernmost point (Lake of the Woods, Minnesota)
min_longitude = -125.0    # Westernmost point (Cape Alava, Washington)
max_longitude = -66.93457 # Easternmost point (West Quoddy Head, Maine)

spark = SparkSession.builder \
    .appName("DeltaFix") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .config("spark.executorEnv.PYTHONIOENCODING", "UTF-8") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.executorEnv.PYTHONPATH", sys.executable)\
    .getOrCreate()



# Broadcast installation to all nodes
#spark.sparkContext.parallelize([1]).foreach(lambda _: install_textblob())

# =====================
# TWEET GENERATION MODULE
# =====================

def generate_jpmc_tweet():
    loan_products = {
        'auto': [
            "Chase auto loan rates looking competitive this quarter",
            "JPMorgan Chase denied my car loan application #frustrated",
            "Smooth process getting my auto loan with Chase",
            "Comparing Chase vs Bank of America auto loan rates"
        ],
        'home': [
            "Chase mortgage rates dropped to 4.25% this week",
            "Closing costs with JPMorgan Chase were higher than expected",
            "Refinancing my home loan with Chase saved me $200/month",
            "Chase home equity line of credit approved at 5.2% APR"
        ]
    }
    
    states = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    loan_type = random.choice(list(loan_products.keys()))
    template = random.choice(loan_products[loan_type])
    state = random.choice(states)
    
    return f"{template} #{loan_type}loan #{state}"

# =====================
# SENTIMENT ANALYSIS MODULE
# =====================
"""
def analyze_sentiment(text):
    analysis = TextBlob(str(text))
    polarity = analysis.sentiment.polarity
    if polarity > 0.2:
        return ("positive", polarity)
    elif polarity < -0.2:
        return ("negative", polarity)
    else:
        return ("neutral", polarity)

sentiment_udf = udf(analyze_sentiment, StructType([
    StructField("sentiment", StringType()),
    StructField("polarity", FloatType())
]))
"""
# =====================
# STREAMING PIPELINE
# =====================

# 1. Generate simulated tweet stream
tweets_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load() \
    .withColumn("tweet", udf(generate_jpmc_tweet, StringType())()) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("date", to_date(col("processing_time"))) \
    .withColumn("hour", hour(col("processing_time")))

# 2. Extract metadata and apply sentiment analysis
"""
processed_tweets = tweets_df \
    .withColumn("loan_type", regexp_extract(col("tweet"), "#(\\w+)loan", 1)) \
    .withColumn("state", regexp_extract(col("tweet"), "#([A-Z]{2})", 1)) \
    .withColumn("sentiment_data", sentiment_udf(col("tweet"))) \
    .select(
        col("tweet"),
        col("loan_type"),
        col("state"),
        col("sentiment_data.sentiment").alias("sentiment"),
        col("sentiment_data.polarity").alias("polarity"),
        col("processing_time"),
        col("date"),
        col("hour")
    )
"""
cities = [
    {"city": "New York City", "latitude": 40.670, "longitude": -73.940},
    {"city": "Los Angeles", "latitude": 34.110, "longitude": -118.410},
    {"city": "Chicago", "latitude": 41.840, "longitude": -87.680},
    {"city": "Houston", "latitude": 29.7407, "longitude": -95.4636},
    {"city": "Phoenix", "latitude": 33.540, "longitude": -112.070},
    {"city": "Philadelphia", "latitude": 40.010, "longitude": -75.130},
    {"city": "San Antonio", "latitude": 29.460, "longitude": -98.510},
    {"city": "Seattle", "latitude": 47.620, "longitude": -122.350},
    {"city": "Denver", "latitude": 39.770, "longitude": -104.870},
    {"city": "Boston", "latitude": 42.340, "longitude": -71.020}
]

# Broadcast the list of cities to all Spark nodes
broadcast_cities = spark.sparkContext.broadcast(cities)

# Define a UDF to randomly select a city and return its latitude and longitude
def random_city_coordinates():
    city = random.choice(broadcast_cities.value)
    return city["latitude"], city["longitude"]

# Register UDF
@udf(returnType=StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
]))
def get_random_coordinates():
    lat, lon = random_city_coordinates()
    return {"latitude": lat, "longitude": lon}

processed_tweets = tweets_df \
    .withColumn("loan_type", regexp_extract(col("tweet"), "#(\\w+)loan", 1)) \
    .withColumn("state", regexp_extract(col("tweet"), "#([A-Z]{2})", 1)) \
    .select(
        col("tweet"),
        col("loan_type"),
        col("state"),
        col("processing_time"),
        col("date"),
        col("hour")
    )
processed_tweets.registerTempTable("processed_tweets")
processed_tweets=spark.sql("""select * from processed_tweets""")
processed_tweets1=processed_tweets \
    .withColumn("Sentiment",when(rand() > 0.5, "Positive").otherwise("Negative")) \
    .withColumn("Coordinates", get_random_coordinates()) 
processed_tweets1.printSchema()
spark.read.format("delta").load("s3://j0c0vk5-streaming-demo/delta_tables/integrated_sentiment_2").printSchema()
processed_tweets=processed_tweets1.withColumn("City_latitude", processed_tweets1["Coordinates"]["latitude"]) \
    .withColumn("City_longitude", processed_tweets1["Coordinates"]["longitude"])  \
    .drop("Coordinates")
processed_tweets.printSchema()
# =====================
# DELTA TABLES OUTPUT
# =====================



# 1. Integrated Sentiment Table (Partitioned by date/hour)
integrated_sentiment_path = "s3://j0c0vk5-streaming-demo/delta_tables/integrated_sentiment_2"
checkpoint_path_integrated = "s3://j0c0vk5-streaming-demo/checkpoints/integrated_sentiment_2"
raw_tweets_path = "s3://j0c0vk5-streaming-demo/base/raw_tweets_2"
processed_tweets.writeStream \
    .format("delta") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path_integrated) \
    .partitionBy("date", "hour") \
    .start(integrated_sentiment_path)

# 2. State Metrics Table (Partitioned by date/state with UPSERT)
state_metrics_path = "s3://j0c0vk5-streaming-demo/delta_tables/state_metrics_2"
checkpoint_path_state = "s3://j0c0vk5-streaming-demo/checkpoints/state_metrics_2"

# Create Delta tables if they don't exist
if not DeltaTable.isDeltaTable(spark, raw_tweets_path):
    # Schema for raw tweets
    raw_schema = StructType([
        StructField("tweet", StringType()),
        StructField("loan_type", StringType()),
        StructField("state", StringType()),
        StructField("sentiment", StringType()),
        StructField("processing_time", TimestampType()),
        StructField("date", DateType()),
        StructField("hour", IntegerType())
    ])
    spark.createDataFrame([], raw_schema) \
        .write \
        .format("delta") \
        .partitionBy("date", "hour") \
        .save(raw_tweets_path)

if not DeltaTable.isDeltaTable(spark, state_metrics_path):
    # Schema for state metrics
    metrics_schema = StructType([
        StructField("time_window", StructType([
            StructField("start", TimestampType()),
            StructField("end", TimestampType())
        ])),
        StructField("date", DateType()),
        StructField("state", StringType()),
        StructField("distinct_tweets", LongType()),
        StructField("total_tweets", LongType()),
        StructField("positive_count", LongType()),
        StructField("negative_count", LongType()),
        StructField("sentiment_flag", StringType()),
        StructField("window_end", TimestampType())
    ])
    spark.createDataFrame([], metrics_schema) \
        .write \
        .format("delta") \
        .partitionBy("date", "state") \
        .save(state_metrics_path)

# Write raw tweets to Delta
raw_tweets_query = (processed_tweets.writeStream
    .format("delta")
    .option("mergeSchema", "true") \
    .outputMode("append")
    .option("checkpointLocation", f"{raw_tweets_path}/_checkpoints")
    .partitionBy("date", "hour")
    .start(raw_tweets_path))

# Calculate state metrics
state_metrics = processed_tweets \
    .withWatermark("processing_time", "1 hour") \
    .groupBy(
        window(col("processing_time"), "1 hour").alias("time_window"),
        col("date"),
        col("state")
    ) \
    .agg(
        approx_count_distinct("tweet").alias("distinct_tweets"),
        count(lit(1)).alias("total_tweets"),
        sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),
        sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count")
    ) \
    .withColumn("sentiment_flag", 
               when(col("positive_count") > col("negative_count"), "positive")
               .otherwise("negative")) \
    .withColumn("window_end", col("time_window.end"))

# Define upsert function for state metrics
def upsert_state_metrics(batch_df, batch_id):
    # Get Delta table reference
    delta_table = DeltaTable.forPath(spark, state_metrics_path)
    
    # Select only columns that exist in both DataFrames
    target_columns = delta_table.toDF().columns
    available_columns = [col for col in batch_df.columns if col in target_columns]
    batch_df = batch_df.select(*available_columns)
    
    # Drop duplicates on merge keys
    batch_df = batch_df.dropDuplicates(["date", "state", "window_end"])
    
    # Build merge condition
    merge_condition = "target.date = source.date AND target.state = source.state AND target.window_end = source.window_end"
    
    # Prepare column mappings
    update_mapping = {col: f"source.{col}" for col in available_columns if col not in ["date", "state", "window_end"]}
    insert_mapping = {col: f"source.{col}" for col in available_columns}
    
    # Execute merge
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set = update_mapping
    ).whenNotMatchedInsert(
        values = insert_mapping
    ).execute()

# Write state metrics with upsert
state_metrics_query = (state_metrics.writeStream
    .foreachBatch(upsert_state_metrics)
    .outputMode("update")
    .option("checkpointLocation", f"{state_metrics_path}/_checkpoints")
    .start())

# Wait for termination
spark.streams.awaitAnyTermination()
