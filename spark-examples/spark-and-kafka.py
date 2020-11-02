from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark-and-kafka.py
spark = (
    SparkSession.builder.config("spark.driver.memory", "400g")
    .config("spark.sql.streaming.checkpointLocation", "/workdir/boris/tmp")
    .getOrCreate()
)
main_topic = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "main_topic")
    .option("startingOffsets", "earliest")
    .load()
)
(
    main_topic.groupby()
    .count()
    .select(col("count").cast(StringType()).alias("value"))
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "result_topic")
    .outputMode("complete")
    .trigger(once=True)
    .start()
)
# kafka-console-consumer.sh --topic result_topic --from-beginning --bootstrap-server localhost:9092
sleep(10)
