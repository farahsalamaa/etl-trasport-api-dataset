from pyspark.sql import functions as F
from decimal import Decimal, ROUND_HALF_UP


def to_dec(x) -> Decimal:
    """Convertit n'importe quel nombre en Decimal(5,2), arrondi half-up."""
    if x is None:
        return Decimal("0.00")
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def taux_de_nan(v, total):
    if total == 0:
        return 0.0
    return round((v / total) * 100, 2)


def build_quality_report(datasets_df, resources_df):
    print("[QUALITY] Building quality metrics")

    dataset_count  = datasets_df.count()
    resource_count = resources_df.count()

    dub_count1 = datasets_df.count()  - datasets_df.dropDuplicates().count()
    dub_count2 = resources_df.count() - resources_df.dropDuplicates().count()

    checks = []
    for column in datasets_df.columns:
        null_count = datasets_df.filter(F.col(column).isNull()).count()
        checks.append((
            "datasets", column,
            int(null_count), int(dataset_count),
            to_dec(taux_de_nan(null_count, dataset_count)),
            to_dec(taux_de_nan(dub_count1, dataset_count)),
        ))

    for column in resources_df.columns:
        null_count = resources_df.filter(F.col(column).isNull()).count()
        checks.append((
            "resources", column,
            int(null_count), int(resource_count),
            to_dec(taux_de_nan(null_count, resource_count)),
            to_dec(taux_de_nan(dub_count2, resource_count)),
        ))

    spark = datasets_df.sparkSession
    quality_schema = (
        "table_name STRING, column_name STRING, "
        "null_count LONG, row_count LONG, "
        "tx_nan DECIMAL(5,2), tx_dub DECIMAL(5,2)"
    )

    quality_report = (
        spark.createDataFrame(checks, schema=quality_schema)
        .withColumn(
            "completeness_pct",
            F.when(
                F.col("row_count") == 0,
                F.lit(0.0)
            ).otherwise(
                F.round(
                    (1 - F.col("null_count").cast("double")
                       / F.col("row_count").cast("double")) * 100,
                    2
                )
            )
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