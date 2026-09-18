# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Population By Age data by performing the transformations below

# COMMAND ----------

# MAGIC %md
# MAGIC  ####-----------------------------------------------------------------------
# MAGIC ### 1. Split country code + age group
# MAGIC ### 2. Keep 2019 only
# MAGIC ### 3. Remove non-numeric percentage values
# MAGIC ### 4. Convert percentage to numeric
# MAGIC ### 5. Map age-group codes to readable names
# MAGIC ### 6. Pivot by age group
# MAGIC ### 7. Join dim_country
# MAGIC ### 8. Add country name, 3-digit country code, and total population
# MAGIC ### 9. Data-quality checks
# MAGIC ### 10. Write the result to the Silver ADLS layer as Delta

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

storage_account="covidreportingnr"


# COMMAND ----------

RAW_PATH = f"abfss://raw@{storage_account}.dfs.core.windows.net/Population/population_by_age.tsv.gz"
SILVER_PATH = f"abfss://silverlayer@{storage_account}.dfs.core.windows.net/population_by_age"

# COMMAND ----------

LOOKUP_DIM_COUNTRY_PATH = (
    "abfss://lookup@covidreportingnr.dfs.core.windows.net/dim_country"
)

# COMMAND ----------

dim_country = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(LOOKUP_DIM_COUNTRY_PATH)
)

display(dim_country)

# COMMAND ----------

df_raw = (
    spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "false")
        .csv(RAW_PATH)
)


display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the first column and split indicator + country code

# COMMAND ----------

# DBTITLE 1,Split indicator and country code
import pyspark.sql.functions as F

first_col = df_raw.columns[0]

df = (
    df_raw
        .withColumnRenamed(first_col, "indicator_country")
        .withColumn("indicator", F.split(F.col("indicator_country"), ",").getItem(0))
        .withColumn("country_code_2", F.split(F.col("indicator_country"), ",").getItem(1))
        .drop("indicator_country")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep only 2019 and clean the percentage

# COMMAND ----------

# DBTITLE 1,Display cleaned 2019 percentages
import pyspark.sql.functions as F

extracted_percentage = F.regexp_extract(
    F.trim(F.col("percentage_raw")),
    r"(\d+(?:\.\d+)?)",
    1
)

df_clean = (
    df.select(
        "indicator",
        "country_code_2",
        F.col("2019 ").alias("percentage_raw")
    )
    .withColumn(
        "percentage_2019",
        F.when(extracted_percentage != "", extracted_percentage.cast("double"))
    )
    .drop("percentage_raw")
)

df_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove non-numeric / invalid values

# COMMAND ----------

# DBTITLE 1,Filter non-null cleaned percentages
df_clean = df_clean.filter(
    F.col("percentage_2019").isNotNull()
)
df_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Map age-group codes to readable names

# COMMAND ----------

df_age = (
    df_clean
    .withColumn(
        "age_group",
        F.when(F.col("indicator") == "PC_Y0_14", "0-14")
         .when(F.col("indicator") == "PC_Y15_24", "15-24")
         .when(F.col("indicator") == "PC_Y25_49", "25-49")
         .when(F.col("indicator") == "PC_Y50_64", "50-64")
         .when(F.col("indicator") == "PC_Y65_79", "65-79")
         .when(F.col("indicator") == "PC_Y80_MAX", "80+")
    ) 
   .drop("indicator")
)

df_age.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Pivot by age group

# COMMAND ----------

df_pivot = (
    df_age
    .groupBy("country_code_2")
    .pivot(
        "age_group",
        ["0-14", "15-24", "25-49", "50-64", "65-79", "80+"]
    )
    .agg(F.first("percentage_2019"))
)

df_pivot.display()

# COMMAND ----------

df_pivot = df_pivot.select(
    "country_code_2",
    F.col("0-14").alias("age_0_14_pct"),
    F.col("15-24").alias("age_15_24_pct"),
    F.col("25-49").alias("age_25_49_pct"),
    F.col("50-64").alias("age_50_64_pct"),
    F.col("65-79").alias("age_65_79_pct"),
    F.col("80+").alias("age_80_plus_pct")
)

df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join dim_country

# COMMAND ----------

df_silver = (
    df_pivot.alias("p")
    .join(
        dim_country.alias("c"),
        F.col("p.country_code_2") == F.col("c.country_code_2_digit"),
        "left"
    )
    .select(
        F.col("c.country"),
        F.col("p.country_code_2").alias("country_code_2_digit"),
        F.col("c.country_code_3_digit"),
        F.col("c.population").alias("total_population"),
        F.col("p.age_0_14_pct").alias("age_group_0_14_"),
        F.col("p.age_15_24_pct").alias("age_group_15_24_"),
        F.col("p.age_25_49_pct").alias("age_group_25_49_"),
        F.col("p.age_50_64_pct").alias("age_group_50_64_"),
        F.col("p.age_65_79_pct").alias("age_group_65_79"),
        F.col("p.age_80_plus_pct").alias("age_group_80_max")
    )
    .orderBy(F.col("country"))
             
)

df_silver.display()

# COMMAND ----------

df_silver = df_silver.filter(F.col("country").isNotNull())

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output to an external datalake location

# COMMAND ----------


df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)


# COMMAND ----------

# MAGIC %md
# MAGIC