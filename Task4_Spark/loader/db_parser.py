from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PyCharmSpark") \
    .getOrCreate()

# extract/load_tables.py
def load_table(spark, table_name, url, props):
    return spark.read.jdbc(
        url=url,
        table=table_name,
        properties=props
    )

def load_all_tables(spark, url, props):
    tables = [
        "actor",
        "film",
        "film_actor",
        "category",
        "film_category"
    ]

    return {t: load_table(spark, t, url, props) for t in tables}
