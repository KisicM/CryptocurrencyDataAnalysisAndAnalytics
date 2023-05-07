from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json, col, sum, window, count, from_unixtime, expr, avg, row_number, desc
from pyspark.sql.types import StructType, StructField, StringType

kafka_broker = "kafka:9092"
topic_name = "block_creation"

host = "influxdb"
port = 8086
database = "eth_block"
username = "admin"
password = "password"

client = InfluxDBClient(url=f"http://{host}:{port}", token=f"lPhnJcc9XiYIaCGybs9opwY7i567FKRWS46FjE2nN7kkiLBMvlkD_s0yaz9slR0z_yleS_W8Z1RcQnlxHDj_pA==", org="bigdata")
write_api = client.write_api(write_options=SYNCHRONOUS)

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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .load() \

df = df.select(from_json(col("value").cast("string"), block_schema).alias("data")) \
    .select("data.*")

df = df.withColumn("timestamp", expr("int(conv(substring(timestamp, 3), 16, 10))")) \
    .withColumn("baseFeePerGasDecimal", expr("int(conv(substring(baseFeePerGas, 3), 16, 10))")) \
    .withColumn("blockNumber", expr("int(conv(substring(number, 3), 16, 10))")) \
    .withColumn("date", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumn("sizeInKb", expr("int(conv(substring(size, 3), 16, 10))") / 1024) \
    .withColumn("gasUsedDecimal", expr("int(conv(substring(gasUsed, 3), 16, 10))")) \
    .withColumn("gasLimitDecimal", expr("int(conv(substring(gasLimit, 3), 16, 10))")) \
    .withColumn("gasPercentage", col("gasUsedDecimal") / (col("gasLimitDecimal")))

def write_to_influxdb(df, epoch_id):
    for row in df.rdd.collect():
        point = Point("blocks").tag("eth", "eth") \
                                .field("date", str(row["date"])) \
                                .field("blockNumber", row["blockNumber"]) \
                                .field("baseFeePerGas", row["baseFeePerGasDecimal"]) \
                                .field("sizeInKb", row["sizeInKb"]) \
                                .field("gasUsed", row["gasUsedDecimal"]) \
                                .field("gasLimit", row["gasLimitDecimal"]) \
                                .field("gasPercentage", row["gasPercentage"])
        print(f"SAVING {point}")
        write_api.write(bucket=database, record=point)

df.writeStream.foreachBatch(write_to_influxdb).start()

# Process the blocks in batches
windowed = df \
    .withWatermark("date", "5 minutes") \
    .groupBy(window("date", "1 minute")) \
    .agg(
    count("*").alias("num_blocks"),
    sum("gasUsedDecimal").alias("total_gas_used"),
    avg("gasPercentage").alias("avg_gas_used_percentage"),
    avg("sizeInKb").alias("avg_block_size_kb")
)

# Output the windowed results to the console
query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
