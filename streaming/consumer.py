from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (SparkSession.builder
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
         .appName("KafkaSparkStreaming")
         .getOrCreate())

kafkaServer = "localhost:9092"
topicName = "first_kafka_topic"

# Define the schema for the incoming data
schema = StructType([
    StructField("key", StringType()),
    StructField("value", IntegerType())
])

# Create a DataFrame representing the stream of input from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topicName) \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Calculate the sum of the 'value' column
# sumDF = df.select(sum("value").alias("sum"))

# Start the streaming query and print the results to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
