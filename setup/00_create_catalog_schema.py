from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create catalog, schema, and volume
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.finance")
spark.sql("CREATE VOLUME IF NOT EXISTS main.finance.lakehouse")

print("Setup completed successfully!")