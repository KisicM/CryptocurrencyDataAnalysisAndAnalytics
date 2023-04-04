from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

kafka_broker = "172.18.0.3:9092"
topic_name = "block_creation"

# spark = (SparkSession.builder
#          .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
#          .appName("KafkaSparkStreaming")
#          .getOrCreate())

# schema = StructType([
#     StructField("key", StringType()),
#     StructField("value", IntegerType())
# ])

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", topic_name) \
#     .load() \
    
# filtered_df = df.filter("key IS NOT NULL and value IS NOT NULL")
# filtered_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
# .select("data.*")

# query = filtered_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

# create Kafka consumer
consumer = KafkaConsumer(
    topic_name,  # replace with the topic you want to consume from
    bootstrap_servers=[kafka_broker],
    value_deserializer=lambda m: json.loads(m.decode('ascii'))
)

# loop over incoming messages and print them to console
for message in consumer:
    print(message.value)
