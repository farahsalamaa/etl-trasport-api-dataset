from config import PROCESSED_DIR, OUTPUT_DIR


def load_outputs(datasets_df, resources_df, quality_report, summary_by_format , rejected_df):
    print("[LOAD] Writing output files")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Spark écrit un dossier Parquet contenant un ou plusieurs fichiers part-*.parquet.
    datasets_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "datasets.parquet")) 
    rejected_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "rejected_data.parquet"))
    resources_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "resources.parquet"))
    print("=== SCHEMA AVANT WRITE ===")
    quality_report.printSchema()
    quality_report.show(truncate=False) 
    
    from pyspark.sql import functions as F
    quality_report = (
    quality_report
    .withColumn("tx_nan", F.col("tx_nan").cast("double"))
    .withColumn("tx_dub", F.col("tx_dub").cast("double"))
    .withColumn("completeness_pct", F.col("completeness_pct").cast("double"))
    ) 
    
    print("=== SCHEMA APRES CAST ===")
    quality_report.printSchema()
    # --------------------
    




    # Spark écrit aussi un dossier contenant un fichier CSV et son éventuel _SUCCESS.
    quality_report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT_DIR / "quality_report.csv")
    )
    summary_by_format.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT_DIR / "summary_by_format.csv")
    )

    print(f"[LOAD] Outputs written under {OUTPUT_DIR}")
