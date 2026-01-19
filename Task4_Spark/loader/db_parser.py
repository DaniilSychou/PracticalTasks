from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PyCharmSpark") \
    .getOrCreate()

# extract/load_tables.py
def load_table(url, table_name, props):
    df = spark.read.jdbc(
        url=url,
        table=table_name,
        properties=props
    )   
    return df
    