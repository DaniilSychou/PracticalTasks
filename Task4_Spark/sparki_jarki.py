from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("PyCharmSpark") \
    .getOrCreate()

spark.range(5).show()


df = spark.read.jdbc(
    url=pg_url,
    table="public.actor",   # имя таблицы
    properties=pg_properties
)

df.show(5)
df.printSchema()


