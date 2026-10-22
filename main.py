from src.extract import extract_datasets
from src.transform import transform_datasets
from src.quality import build_quality_report
from src.load import load_outputs


def main():
    print("[ETL] Starting transport datasets pipeline")

    raw_path, payload = extract_datasets()
    datasets, resources = transform_datasets(payload)
    quality_report, summary_by_format = build_quality_report(datasets, resources)

    load_outputs(datasets, resources, quality_report, summary_by_format)
    print(f"[ETL] Raw API response saved to: {raw_path}")
    print("[ETL] Pipeline completed successfully")


if __name__ == "__main__":
    main()
