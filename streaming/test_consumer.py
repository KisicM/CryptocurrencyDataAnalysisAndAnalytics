from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window, count, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

kafka_broker = "kafka:9092"
topic_name = "block_creation"

spark = (SparkSession.builder
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
         .appName("KafkaSparkStreaming")
         .getOrCreate())

block_schema = StructType([
    StructField("baseFeePerGas", StringType()),
    StructField("difficulty", StringType()),
    StructField("extraData", StringType()),
    StructField("gasLimit", StringType()),
    StructField("gasUsed", StringType()),
    StructField("hash", StringType()),
    StructField("logsBloom", StringType()),
    StructField("miner", StringType()),
    StructField("mixHash", StringType()),
    StructField("nonce", StringType()),
    StructField("number", StringType()),
    StructField("parentHash", StringType()),
    StructField("receiptsRoot", StringType()),
    StructField("sha3Uncles", StringType()),
    StructField("size", StringType()),
    StructField("stateRoot", StringType()),
    StructField("timestamp", StringType()),
    StructField("transactionsRoot", StringType())
])

# Define a function to convert hex to string
def hex_to_str(hex_string):
    if hex_string:
        return str(int(hex_string, 16))
    else:
        return ""

# Register the function as a UDF
hex_to_str_udf = udf(hex_to_str, StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .load() \

# filtered_df = df.filter("key IS NOT NULL and value IS NOT NULL")
df = df.select(from_json(col("value").cast("string"), block_schema).alias("data")) \
.select("data.*")

# Apply the UDF to all columns in the block DataFrame
df = df.select([hex_to_str_udf(col).alias(col) for col in df.columns])

query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

# # create Kafka consumer
# consumer = KafkaConsumer(
#     topic_name,  # replace with the topic you want to consume from
#     bootstrap_servers=[kafka_broker],
#     value_deserializer=lambda m: json.loads(m.decode('ascii'))
# )

# # loop over incoming messages and print them to console
# for message in consumer:
#     print(message.value)
