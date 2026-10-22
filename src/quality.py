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
