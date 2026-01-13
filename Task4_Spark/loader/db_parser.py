from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PyCharmSpark") \
    .getOrCreate()

# extract/load_tables.py
def load_table(spark, table_name, url, props):
    df = spark.read.jdbc(
        url=url,
        table=table_name,
        properties=props
    )
    df.cache()   # кэшируем для повторного использования
    return df
    

def load_all_tables(spark, url, props):
    tables = [
        "actor",
        "address",
        "category",
        "city",
        "country",
        "customer",
        "film",
        "film_actor",
        "film_category",
        "inventory",
        "language",
        "payment",
        "rental",
        "staff",
        "store"
    ]

    return {t: load_table(spark, t, url, props) for t in tables}
