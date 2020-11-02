from pyspark.sql import SparkSession
from time import sleep

spark = (
    SparkSession.builder
    .config("spark.driver.memory", "400g")
    .config("packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
    .config("spark.sql.streaming.checkpointLocation", "/workdir/boris/tmp")
    .getOrCreate()
)
main_topic = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "main_topic")
    .option("startingOffsets", "earliest")
    .load()
)
query = (
    main_topic
    .groupby().count().toDF("value").selectExpr("CAST(value AS STRING) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "result_topic")
    .outputMode("complete")
    .trigger(once=True)
    .start()
)
sleep(10)
