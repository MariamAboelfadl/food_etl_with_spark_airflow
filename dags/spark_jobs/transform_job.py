from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def main():
    spark = SparkSession.builder.master("local[*]").appName("ETL_DAG").getOrCreate()
    df0 = spark.read.parquet('/opt/airflow/dags/raw_data.parquet')

    required_columns = [
        "product_name", "brands", "categories", "ingredients_text",
        "fat_100g", "sugars_100g", "salt_100g", "nutrition_grade_fr", "countries"
    ]
    df = df0.select([f.col(c) for c in required_columns])

    df = df.filter(f.col("product_name").isNotNull())
    df = df.withColumn("brands", f.when(f.col("brands").isNotNull(),
                                        f.split(f.lower(f.col("brands")), ",").getItem(0)).otherwise(""))
    df = df.withColumn("categories", f.when(f.col("categories").isNotNull(),
                                            f.split(f.lower(f.col("categories")), ",").getItem(0)).otherwise(""))
    df = df.withColumn("ingredients_text", f.when(f.col("ingredients_text").isNotNull(),
                                                  f.regexp_replace(f.col("ingredients_text"), r"[^\w\s,.-]", "")
                                                  ).otherwise(""))

    for c in ["fat_100g", "sugars_100g", "salt_100g"]:
        df = df.withColumn(c, f.when(f.col(c).isNotNull(), f.col(c)).otherwise(f.lit(-1)))

    df = df.withColumn("countries", f.when(f.col("countries") == "United States", "US")
                        .when(f.col("countries") == "United Kingdom", "UK")
                        .otherwise(f.col("countries")))

    df.write.mode("overwrite").parquet("/opt/airflow/dags/df_trans.parquet")

if __name__ == "__main__":
    main()
