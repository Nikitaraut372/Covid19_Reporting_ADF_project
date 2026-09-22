# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Clean column names
# MAGIC ### 2. Remove duplicates
# MAGIC ### 3. Keep valid country codes
# MAGIC ### 4. Split year_week into year and week
# MAGIC ### 5. Convert numeric columns to proper types
# MAGIC ### 6. Recalculate testing_rate
# MAGIC ### 7. calculate positivity_rate
# MAGIC ### 8. Add data-quality flag
# MAGIC ### 9. Remove invalid records
# MAGIC ### 10. Write Silver data as Delta

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

storage_account = "covidreportingnr"

RAW_PATH = (
    f"abfss://raw@{storage_account}.dfs.core.windows.net/ECDC/testing/testing.csv"
)

SILVER_PATH = (
    f"abfss://silverlayer@{storage_account}.dfs.core.windows.net/ecdc/testing"
)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Read raw testing data

# COMMAND ----------

df_raw = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(RAW_PATH)
)

display(df_raw)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Remove duplicate records

# COMMAND ----------

df_clean=(
    df_raw.dropDuplicates()
)

df_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## remove null values

# COMMAND ----------

df_clean = df_clean.dropna()
df_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split year_week into year and week

# COMMAND ----------

df_clean=(
    df_clean
    .withColumn(
        "year",
        substring(col("year_week"),1,4).cast("int")
    )
    .withColumn(
        "week",
        substring(col("year_week"),7,2).cast("int"))
)
df_clean.display()

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn("new_cases", col("new_cases").cast("long"))
    .withColumn("tests_done", col("tests_done").cast("long"))
    .withColumn("population", col("population").cast("long"))
    .withColumn("testing_rate", col("testing_rate").cast("double"))
    .withColumn("positivity_rate", col("positivity_rate").cast("double"))
)

df_clean.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate country and country code

# COMMAND ----------

df_clean = df_clean.filter(
    (col("country").isNotNull()) &
    (trim(col("country")) != "") &
    (col("country_code").isNotNull()) &
    (length(trim(col("country_code"))) == 2)
)

df_clean.display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Recalculate testing rate per 100,000 population

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn(
        "testing_rate",
        when(
            col("population") > 0,
            round(
                (col("tests_done") / col("population")) * 100000,
                2
            )
        ).otherwise(None)
    )
)

df_clean.display()

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn(
        "positivity_rate",
        when(
            col("tests_done") > 0,
            round(
                (col("new_cases") / col("tests_done")) * 100,
                2
            )
        ).otherwise(None)
    )
)

df_clean.display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Add data-quality checks

# COMMAND ----------

df_quality = (
    df_clean
    .withColumn(
        "data_quality_flag",
        when(
            col("new_cases").isNull(),
            "INVALID_NEW_CASES"
        )
        .when(
            col("tests_done").isNull(),
            "INVALID_TESTS_DONE"
        )
        .when(
            col("new_cases") < 0,
            "INVALID_NEW_CASES"
        )
        .when(
            col("tests_done") < 0,
            "INVALID_TESTS_DONE"
        )
        .when(
            col("population") <= 0,
            "INVALID_POPULATION"
        )
        .when(
            col("week") < 1,
            "INVALID_WEEK"
        )
        .when(
            col("week") > 53,
            "INVALID_WEEK"
        )
        .otherwise("VALID")
    )
)

df_quality.display()

# COMMAND ----------

df_invalid = df_quality.filter(
    col("data_quality_flag") != "VALID"
)

display(df_invalid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep valid records

# COMMAND ----------

df_silver = (
    df_quality
    .filter(col("data_quality_flag") == "VALID")
)

df_silver.display()

# COMMAND ----------

df_silver = (
    df_silver
    .select(
        "country",
        "country_code",
        "year",
        "week",
        "new_cases",
        "tests_done",
        "population",
        "testing_rate",
        "positivity_rate",
        "testing_data_source"
    )
)

df_silver.display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Order Silver data by country and week

# COMMAND ----------

df_silver = (
    df_silver
    .orderBy(
        col("country"),
        col("year"),
        col("week")
    )
)

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write output to an external datalake location

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

# COMMAND ----------

df_verify = (
    spark.read
        .format("delta")
        .load(SILVER_PATH)
)

display(df_verify)

# COMMAND ----------

# MAGIC %md
# MAGIC