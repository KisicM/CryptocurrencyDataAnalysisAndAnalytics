from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.window import Window

def main():
    spark = (
        SparkSession.builder \
        .appName("Batch processing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
        .config("spark.executor.extraClassPath", "/opt/workspace/postgresql-42.4.0.jar") \
        .master("spark://spark-master:7077") \
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
        .option("url", "jdbc:postgresql://db:5432/btcusd") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "btcusd") \
        .option("user", "postgres").option("password", "admin").save()
    
    # Max price per year
    w = Window.partitionBy('year').orderBy('year')
    max_high_per_year = df.withColumn('max_high', func.max('high').over(w)) \
                        .withColumn('rank', func.row_number().over(w)) \
                        .filter('rank == 1').select('year', 'max_high').orderBy('year')
    max_high_per_year.show()

    max_high_per_year.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "yearly_max_price") \
            .option("user", "postgres").option("password", "admin").save()
    
    # Top 5 months per year by volume
    w = Window.partitionBy('year').orderBy(func.desc('total_volume'))
    ranked_by_volume = df.groupBy('year', 'month').agg(func.sum('volume').alias('total_volume')) \
                        .withColumn('rank', func.row_number().over(w))
    top_5_months_by_volume = ranked_by_volume.filter('rank <= 5')
    top_5_months_by_volume.show()

    top_5_months_by_volume.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "best_months_per_year_by_volume") \
            .option("user", "postgres").option("password", "admin").save()
    
    # Difference from max high per day ranked by volume
    max_high_per_day = df.groupBy('date').agg(func.max('high').alias('max_high'))
    difference_from_max = df.join(max_high_per_day, 'date') \
                        .withColumn('difference_from_max', col('high') - col('max_high')) \
                        .select('timestamp', 'date', 'high', 'max_high', 'difference_from_max', 'volume')
    w = Window.partitionBy('date').orderBy(func.desc('volume'))
    #df.select('timestamp', 'date', 'high', 'max_high', 'difference_from_max', 'volume', func.max('high').over(Window.partitionBy('date')).alias('max_high'), func.col('max_high')-func.col('high'))
    ranked_by_volume = difference_from_max.withColumn('rank', func.dense_rank().over(w)).filter('rank == 1').orderBy('difference_from_max')
    ranked_by_volume.show()

    ranked_by_volume.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "difference_from_max_high_per_day_ranked_by_volume") \
            .option("user", "postgres").option("password", "admin").save()

    # Top 5 high_low_diff per month
    w = Window.partitionBy('year', 'month').orderBy(func.desc('high'))
    ranked_by_high = df.withColumn('rank', func.row_number().over(w))
    top_5_high_low_diff_per_month = ranked_by_high.filter('rank <= 5').orderBy('year', 'month', 'rank')
    top_5_high_low_diff_per_month.show()

    top_5_high_low_diff_per_month.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "top_5_high_low_diff_per_month") \
            .option("user", "postgres").option("password", "admin").save()

    # Max difference from min closing price on 30 day rolling window
    w = Window.partitionBy('year').orderBy('timestamp').rowsBetween(-29, 0)
    rolling_min_close = df.withColumn('min_close', func.min('close').over(w)) \
                        .withColumn('difference_from_min', col('close') - col('min_close'))
    grouped_by_month = rolling_min_close.groupBy('year', 'month').agg(func.max('difference_from_min')).select('year', 'month', 'max(difference_from_min)').orderBy('year', 'month')
    grouped_by_month.show()

    grouped_by_month.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "max_difference_from_min_close_on_rolling_window") \
            .option("user", "postgres").option("password", "admin").save()

    # Daily max high low difference
    w = Window.partitionBy('date')
    daily_max_high_low_diff = df.withColumn('max_high_low_diff', func.max('high_low_diff').over(w)) \
                            .filter(col('high_low_diff') == col('max_high_low_diff')) \
                            .select('date', 'high_low_diff') \
                            .orderBy('date')
    daily_max_high_low_diff.show()

    daily_max_high_low_diff.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/btcusd") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "daily_max_high_low_diff") \
            .option("user", "postgres").option("password", "admin").save()

    # Monthly average high price
    w = Window.partitionBy('year', 'month')
    monthly_average_high = df.withColumn('avg_high', func.avg('high').over(w)) \
                        .select('year', 'month', 'avg_high') \
                        .distinct() \
                        .orderBy('year', 'month')
    monthly_average_high.show()

    monthly_average_high.write.format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/btcusd") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "monthly_average_high_price") \
        .option("user", "postgres").option("password", "admin").save()

if __name__ == '__main__':
    main()

#COMMAND ./spark-submit --packages org.postgresql:postgresql:42.4.0 /spark/batch_processor/run.py
