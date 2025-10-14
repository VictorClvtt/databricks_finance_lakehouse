# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime
import os

today_str = datetime.now().strftime('%Y-%m-%d')

df_sales = spark.read.format('delta').load(f'/Volumes/main/financial/lakehouse/silver/sales/_ingestion_date={today_str}', header=True, inferSchema=True)
df_countries = spark.read.format('delta').load(f'/Volumes/main/financial/lakehouse/silver/countries/_ingestion_date={today_str}', header=True, inferSchema=True)

# COMMAND ----------

dim_date = ( 
    df_sales.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.dayofmonth("date").alias("day"),
        "month_number",
        "month_name",
        "year" )
    .distinct() 
)

display(dim_date)

# COMMAND ----------

dim_segment = (
    df_sales
    .select("segment")
    .distinct()
    .withColumn("segment_key", F.crc32(F.col("segment")))
)

display(dim_segment)

# COMMAND ----------

dim_discount_band = (
    df_sales
    .select("discount_band")
    .distinct()
    .withColumn("discount_band_key", F.crc32(F.col("discount_band")))
)

display(dim_discount_band)

# COMMAND ----------

dim_country = (
    df_countries.select("country", "region", "official_language", "currency", "population")
    .distinct()
    .withColumn("country_key", F.crc32(F.col("country")))
)

display(dim_country)

# COMMAND ----------

dim_product = (
    df_sales
    .select("product", "manufacturing_price", "sale_price")
    .distinct()
    .withColumn("product_key", F.crc32(F.col("product")))
)

display(dim_product)

# COMMAND ----------

fact_sales = (
    df_sales
    # Chaves estrangeiras (mesma lógica das dimensões)
    .withColumn("product_key", F.crc32(F.col("product")))
    .withColumn("country_key", F.crc32(F.col("country")))
    .withColumn("segment_key", F.crc32(F.col("segment")))
    .withColumn("discount_band_key", F.crc32(F.col("discount_band")))
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    # Seleciona as métricas e chaves
    .select(
        "country",
        "date_key",
        "country_key",
        "product_key",
        "segment_key",
        "discount_band_key",
        "units_sold",
        "gross_sales",
        "discounts",
        "sales",
        "cogs",
        "profit"
    )
)

display(fact_sales)

# COMMAND ----------

dim_date.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/dim_date")
dim_country.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/dim_country")
dim_product.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/dim_product")
dim_segment.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/dim_segment")
dim_discount_band.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/dim_discount_band")

fact_sales.write.format("delta").mode("append").save("/Volumes/main/financial/lakehouse/gold/fact_sales")
