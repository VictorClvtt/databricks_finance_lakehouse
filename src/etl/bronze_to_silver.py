# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

today_str = datetime.now().strftime('%Y-%m-%d')

df_sales = spark.read.csv(f'/Volumes/main/financial/lakehouse/bronze/csv/sales/sales-data-{today_str}.csv', header=True, inferSchema=True)
df_countries = spark.read.csv(f'/Volumes/main/financial/lakehouse/bronze/csv/countries/countries-data-{today_str}.csv', header=True, inferSchema=True)


# MAGIC %md
# MAGIC #Fixing Sales Data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalizing Dates

# COMMAND ----------

df_sales.createOrReplaceTempView("sales")

df_sales = spark.sql("""
    SELECT
        *,
        COALESCE(
            try_to_date(Date, 'yyyy-MM-dd'),
            try_to_date(Date, 'dd/MM/yyyy')
        ) AS Date_parsed
    FROM sales
""")

df_sales = df_sales.drop("Date").withColumnRenamed("Date_parsed", "Date")

display(df_sales.select("Date").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fixing Country Name Typos

# COMMAND ----------

df_sales = df_sales.withColumn(
    "Country",
    F.when(F.col("Country") == "USA", "United States")
    .when(F.col("Country") == "U.S.A", "United States")
    .when(F.col("Country") == "UK", "United Kingdom")
    .when(F.col("Country") == "BRAZIL", "Brazil")
    .when(F.col("Country") == "Deutschland", "Germany")
    .otherwise(F.col("Country"))
)

df_sales.select("Country").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Replacing or Removing Null Values

# COMMAND ----------

# MAGIC %md
# MAGIC ###Removing the lines with Null Values on the 'Country' or the 'Product' columns

# COMMAND ----------

df_sales = df_sales.na.drop(subset=["Country", "Product"])

print(f'Country: {df_sales.where(F.col("Country").isNull()).count()} Null Values')
print(f'Product: {df_sales.where(F.col("Product").isNull()).count()} Null Values')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculating and replacing missing Null Values from the 'Units Sold' column

# COMMAND ----------

display(df_sales.select(F.col("Gross Sales"), F.col("Sale Price"), F.col("Units Sold")).where(F.col("Units Sold").isNull()))

# COMMAND ----------

df_sales = df_sales.withColumn(
    "Units Sold",
    F.col("Gross Sales") / F.col("Sale Price")
)

display(df_sales.select(F.col("Gross Sales"), F.col("Sale Price"), F.col("Units Sold")).where(F.col("Units Sold").isNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Redefining Column Names

# COMMAND ----------

def standardize_columns(df):
    import re
    new_cols = [re.sub(r'[^0-9a-zA-Z]+', '_', c.strip()).lower() for c in df.columns]
    return df.toDF(*new_cols)

df_sales = standardize_columns(df_sales)

df_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding Metadata to the Sales Data

# COMMAND ----------

df_sales = (
    df_sales
    # Data de processamento (timestamp atual)
    .withColumn("_ingestion_timestamp", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Fonte dos dados (ex: ERP, API, CSV etc.)
    .withColumn("_data_source", F.lit("ERP Global - Sales System"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Fixing Country Data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Redefining Column Names

# COMMAND ----------

def standardize_columns(df):
    import re
    new_cols = [re.sub(r'[^0-9a-zA-Z]+', '_', c.strip()).lower() for c in df.columns]
    return df.toDF(*new_cols)

df_countries = standardize_columns(df_countries)

df_countries.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding Metadata to the Country Data

# COMMAND ----------

df_countries = (
    df_countries
    # Data de processamento (timestamp atual)
    .withColumn("_ingestion_timestamp", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Fonte dos dados (ex: ERP, API, CSV etc.)
    .withColumn("_data_source", F.lit("ERP Global - Sales System"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving Data to the Silver Layer

# COMMAND ----------

# Exemplo de paths dentro do volume UC
sales_path = '/Volumes/main/financial/lakehouse/silver/sales/'
countries_path = '/Volumes/main/financial/lakehouse/silver/countries/'

# Salva Delta direto no volume e registra no UC
df_sales.write.format("delta").mode("overwrite").save(sales_path)
df_countries.write.format("delta").mode("overwrite").save(countries_path)