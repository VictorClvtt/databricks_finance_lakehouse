# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

today_str = datetime.now().strftime("%Y-%m-%d")

# Diretório de saída
output_dir = f"/Volumes/main/financial/lakehouse/gold/reports/{today_str}"
dbutils.fs.mkdirs(output_dir)

dim_date = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/dim_date")
dim_country = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/dim_country")
dim_product = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/dim_product")
dim_segment = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/dim_segment")
dim_discount_band = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/dim_discount_band")
fact_sales = spark.read.format("delta").load("/Volumes/main/financial/lakehouse/gold/normalized_data/fact_sales")


# COMMAND ----------

df_joined = (
    fact_sales
    .join(dim_country, "country_key", "left")
    .join(dim_product, "product_key", "left")
    .join(dim_segment, "segment_key", "left")
    .join(dim_discount_band, "discount_band_key", "left")
    .join(dim_date, "date_key", "left")
)

# COMMAND ----------

report_country_product = (
    df_joined
    .groupBy("country", "region", "product")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.sum("profit").alias("total_profit"),
        F.sum("units_sold").alias("total_units")
    )
    .orderBy(F.desc("total_sales"))
)

report_country_product = report_country_product.toPandas()
report_country_product.to_csv(f"{output_dir}/report_country_product.csv", index=False)

display(report_country_product)

# COMMAND ----------

report_segment_discount = (
    df_joined
    .groupBy("segment", "discount_band")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.sum("profit").alias("total_profit"),
        F.avg("profit").alias("avg_profit")
    )
    .orderBy(F.desc("avg_profit"))
)

report_segment_discount = report_segment_discount.toPandas()
report_segment_discount.to_csv(f"{output_dir}/report_segment_discount.csv", index=False)

display(report_segment_discount)

# COMMAND ----------

report_daily_sales = (
    df_joined
    .groupBy("date", "year", "month_name", "day")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.sum("profit").alias("total_profit"),
        F.sum("units_sold").alias("total_units")
    )
)

report_daily_sales = report_daily_sales.toPandas()
report_daily_sales.to_csv(f"{output_dir}/report_daily_sales.csv", index=False)

display(report_daily_sales)

# COMMAND ----------

report_country_population_sales = (
    df_joined
    .groupBy("country", "population")
    .agg(
        F.sum("sales").alias("total_sales"),
        F.sum("profit").alias("total_profit")
    )
    .orderBy(F.desc("total_sales"))
)

report_country_population_sales = report_country_population_sales.toPandas()
report_country_population_sales.to_csv(f"{output_dir}/report_country_population_sales.csv", index=False)

display(report_country_population_sales)
