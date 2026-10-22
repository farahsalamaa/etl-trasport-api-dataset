import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.extract import extract_datasets
from src.transform import transform_datasets
from src.quality import build_quality_report, detect_dataset_duplicates, detect_rejected_datasets
from src.load import load_outputs


def main():
    print("[ETL] Starting transport datasets pipeline")

    raw_path, payload = extract_datasets()
    datasets, resources = transform_datasets(payload)

    duplicates_df = detect_dataset_duplicates(datasets)
    # Écriture du résultat des doublons en JSON 
    duplicates_path = os.path.join("output", "dataset_duplicates")

    (
        duplicates_df
        .write
        .mode("overwrite")
        .json(duplicates_path)
    )
    print(f"Duplicated dataset in : {duplicates_path}")

    # Détection des datasets rejetés
    rejected_datasets = detect_rejected_datasets(datasets)
    # Sauvegarde des datasets rejetés en JSON 
    rejected_path = os.path.join("output", "rejected_datasets")
    rejected_datasets.write.mode("overwrite").json(rejected_path)
    print(f"rejected datasets in : {rejected_path}")
    print(f"Number of datasets rejected : {rejected_datasets.count()}")

    quality_report, summary_by_format = build_quality_report(datasets, resources)
    load_outputs(datasets, resources, quality_report, summary_by_format)
    print(f"[ETL] Raw API response saved to: {raw_path}")
    print("[ETL] Pipeline completed successfully")


if __name__ == "__main__":
    main()
