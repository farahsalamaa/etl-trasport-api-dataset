from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType , BooleanType
)


DATASET_SCHEMA = StructType([
    StructField("dataset_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("description", StringType(), True), 
    StructField("is_description", BooleanType(), True),
    StructField("license", StringType(), True),
    StructField("organization", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_update", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("resources_count", IntegerType(), True),
    StructField("Has_ressource",BooleanType() , True)
])

RESOURCE_SCHEMA = StructType([
    StructField("dataset_id", StringType(), True),
    StructField("dataset_title", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("resource_title", StringType(), True),
    StructField("format", StringType(), True),
    StructField("resource_type", StringType(), True),
    StructField("url", StringType(), True),
])


def _as_text(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return str(value)
    return str(value).strip() or None


def _is_decription(value):
    if value is None:
        return False
    else: 
        return True

def _organization_name(dataset):
    org = dataset.get("organization")
    if isinstance(org, dict):
        return _as_text(org.get("name") or org.get("acronym") or org.get("id"))
    return _as_text(org)


def _normalize_dataset(dataset):
    resources = dataset.get("resources") or []
    if not isinstance(resources, list):
        resources = []

    tags = dataset.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    dataset_id = _as_text(dataset.get("id") or dataset.get("slug"))
    title = _as_text(dataset.get("title"))
    row = {
        "dataset_id": dataset_id,
        "title": title,
        "slug": _as_text(dataset.get("slug")),
        "is_description": _as_text(dataset.get("description")) is not None ,
        "description": _as_text(dataset.get("description")),
        "license": _as_text(dataset.get("license")),
        "organization": _organization_name(dataset),
        "created_at": _as_text(dataset.get("created_at") or dataset.get("created")),
        "last_update": _as_text(dataset.get("last_update") or dataset.get("last_modified")),
        "tags": [str(tag).strip() for tag in tags if tag is not None],
        "resources_count": len(resources),
        "Has_ressource": len(resources) > 0
    }
    
    resource_rows = []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        resource_rows.append({
            "dataset_id": dataset_id,
            "dataset_title": title,
            "resource_id": _as_text(resource.get("id")),
            "resource_title": _as_text(resource.get("title")),
            "format": (_as_text(resource.get("format")) or "unknown").lower(),
            "resource_type": _as_text(resource.get("type")),
            "url": _as_text(resource.get("url") or resource.get("latest")),
        })
    return row, resource_rows


def transform_datasets(payload , spark):
    print("[TRANSFORM] Normalizing dataset and resource metadata")
    normalized_datasets = []
    normalized_resources = []

    for item in payload:
        if not isinstance(item, dict):
            continue
        dataset_row, resource_rows = _normalize_dataset(item)
        normalized_datasets.append(dataset_row)
        normalized_resources.extend(resource_rows)

    
    spark.sparkContext.setLogLevel("WARN")

    datasets_df = spark.createDataFrame(normalized_datasets, schema=DATASET_SCHEMA)
    resources_df = spark.createDataFrame(normalized_resources, schema=RESOURCE_SCHEMA)

    # Normalisation simple des champs texte et suppression des doublons.
    for col_name in ("title", "organization", "license"):
        datasets_df = datasets_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.trim(F.col(col_name)))
        )
    
    datasets_df = datasets_df
    resources_df = resources_df.dropDuplicates(["dataset_id", "resource_id", "url"])

    return datasets_df, resources_df
