# Cryptocurrency-Data-Analysis-and-Analytics

Repository provides a comprehensive set of tools and resources for analyzing and visualizing cryptocurrency data. 

The repository leverages the power of Apache Spark for processing large historical datasets of Bitcoin prices and uses Kafka to collect real-time data on Ethereum network block creation.

## References

[Batch processing](https://www.kaggle.com/datasets/tencars/392-crypto-currency-pairs-at-minute-resolution)

[Stream processing](https://www.alchemy.com/)

## How to run

To run batch processing script:

```bash
docker exec -it spark-master /bin/bash
cd spark/bin
./spark-submit --packages org.postgresql:postgresql:42.4.0 /spark/batch_processor/run.py
```

To run kafka producer:

```bash
docker exec -it producer /bin/bash
python test_producer.py
```

To run kafka consumer:

```bash
docker exec -it consumer /bin/bash
python test_consumer.py
```
