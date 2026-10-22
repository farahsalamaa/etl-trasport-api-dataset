from config import PROCESSED_DIR, OUTPUT_DIR


def load_outputs(datasets_df, resources_df, quality_report, summary_by_format):
    print("[LOAD] Writing output files")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Spark écrit un dossier Parquet contenant un ou plusieurs fichiers part-*.parquet.
    datasets_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "datasets.parquet"))
    resources_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "resources.parquet"))

    # Spark écrit aussi un dossier contenant un fichier CSV et son éventuel _SUCCESS.
    quality_report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT_DIR / "quality_report.csv")
    )
    summary_by_format.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT_DIR / "summary_by_format.csv")
    )

    print(f"[LOAD] Outputs written under {OUTPUT_DIR}")
