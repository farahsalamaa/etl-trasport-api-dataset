from pyspark.sql import functions as F


def build_quality_report(datasets_df, resources_df):
    print("[QUALITY] Building quality metrics")

    dataset_count = datasets_df.count()
    resource_count = resources_df.count()

    checks = []
    for column in datasets_df.columns:
        null_count = datasets_df.filter(
            F.col(column).isNull()
        ).count()
        checks.append(( "datasets", column, int(null_count), int(dataset_count) ))

    for column in resources_df.columns:
        null_count = resources_df.filter(
            F.col(column).isNull()
        ).count()
        checks.append(( "resources", column, int(null_count), int(resource_count) ))

    spark = datasets_df.sparkSession
    quality_schema = "table_name STRING, column_name STRING, null_count LONG, row_count LONG"
    quality_report = spark.createDataFrame(checks, schema=quality_schema).withColumn(
        "completeness_pct",
        F.when(F.col("row_count") == 0, F.lit(0.0)).otherwise(
            F.round((1 - F.col("null_count") / F.col("row_count")) * 100, 2)
        )
    )

    summary_by_format = (
        resources_df
        .groupBy("format")
        .agg(F.count("*").alias("resource_count"))
        .orderBy(F.desc("resource_count"))
    )

    print(f"[QUALITY] datasets={dataset_count}, resources={resource_count}")
    return quality_report, summary_by_format

#detecter les doublons 
def detect_dataset_duplicates(datasets_df):
    return (
        datasets_df
        .groupBy("dataset_id")
        .count()
        .filter(F.col("count") > 1)
    )

#lignes sans identifiant ou titre 
def detect_rejected_datasets(datasets_df):
    rejected_condition = (
        F.col("dataset_id").isNull()
        | (F.trim(F.col("dataset_id")) == "")
        | F.col("title").isNull()
        | (F.trim(F.col("title")) == "")
    )
    rejected_datasets = datasets_df.filter(rejected_condition)
    return rejected_datasets

#calcul des taux des doublons et des valeurs nulles  
def calculate_quality_rates(datasets_df):
    total_rows = datasets_df.count()
    total_columns = len(datasets_df.columns)

    if total_rows == 0:
        return {
            "duplicate_rate": 0.0,
            "null_rate": 0.0,
        }

    # Nombre de lignes supplémentaires dues aux doublons
    duplicate_rows = (
        datasets_df
        .filter(
            F.col("dataset_id").isNotNull()
            & (F.trim(F.col("dataset_id")) != "")
        )
        .groupBy("dataset_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1).alias("duplicates"))
        .first()["duplicates"]
        or 0
    )

    duplicate_rate = duplicate_rows / total_rows * 100

    # Nombre total de cellules NULL
    null_columns = [
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in datasets_df.columns
    ]

    null_values = (
        datasets_df
        .agg(*null_columns)
        .first()
    )

    null_count = sum(value or 0 for value in null_values)

    total_cells = total_rows * total_columns
    null_rate = null_count / total_cells * 100 if total_cells else 0.0

    return {
        "duplicate_rate": round(duplicate_rate, 2),
        "null_rate": round(null_rate, 2),
    }