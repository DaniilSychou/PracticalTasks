# spark_session.py
from pyspark.sql import SparkSession

def get_spark(app_name="Task4_Spark_App"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
