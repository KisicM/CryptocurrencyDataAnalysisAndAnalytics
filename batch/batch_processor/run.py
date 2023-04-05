from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import from_unixtime, col

def main():
    spark = (
        SparkSession.builder \
        .appName("Batch processing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
        .config("spark.executor.extraClassPath", "/opt/workspace/postgresql-42.4.0.jar") \
        .master("spark://172.18.0.4:7077") \
        .getOrCreate()
    )
    #HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
    source_path = f'hdfs://namenode:9000/user/data/btcusd.csv'
    local_path = "../../btcusd.csv"
    print('reading source')
    df = spark.read.csv(
        source_path, inferSchema=True, header=True)

    # Show the resulting DataFrame
    df.show()

    df = (
        df.withColumn("timestamp", from_unixtime(col("time")/1000.0).cast("timestamp"))
        .withColumn('date', func.to_date('timestamp'))
        .withColumn('day', func.date_format('timestamp', 'dd').cast('int'))
        .withColumn('month', func.date_format('timestamp', 'M').cast('int'))
        .withColumn('year', func.date_format('timestamp', 'yyyy').cast('int'))
        .withColumn('hours', func.date_format('timestamp', 'HH').cast('int'))
        .withColumn('part_of_day', func.when(func.col('hours').between(6, 23), 'DURING_DAY').otherwise('DURING_NIGHT'))
        .withColumn('season', func.floor(func.col('month') % func.lit(12) / func.lit(3)) + func.lit(1))
        .withColumn('day_of_week', func.date_format('timestamp', 'E'))
        # highest difference between high and low price
        .withColumn('high_low_diff', func.col('high') - func.col('low'))        
        )
    df.show()
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://172.18.0.2:5432/btcusd") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "btcusd") \
        .option("user", "postgres").option("password", "admin").save()


if __name__ == '__main__':
    main()

#COMMAND ./spark-submit --packages org.postgresql:postgresql:42.4.0 /spark/batch_processor/run.py
