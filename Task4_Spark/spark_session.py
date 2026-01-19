from pyspark.sql import SparkSession
import os

def get_spark(app_name="Task4_Spark_App"):
    # Путь к драйверу, который мы прописали в Dockerfile
    jdbc_jar_path = "/opt/spark/jars/postgresql-42.7.3.jar"
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.driver.extraClassPath", jdbc_jar_path) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    return spark