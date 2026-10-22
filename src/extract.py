import json
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_URL, RAW_DIR, MAX_DATASETS, REQUEST_TIMEOUT_SECONDS


def _session_with_retries():
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _get_dataset_list(payload):
    """Supporte une réponse JSON en liste ou enveloppée dans une clé data/results."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "results", "datasets"):
            if isinstance(payload.get(key), list):
                return payload[key]
        raise ValueError(
            "Structure API non reconnue. Inspectez le JSON brut et adaptez "
            "_get_dataset_list() à la réponse réelle."
        )
    raise ValueError(f"Type de réponse API inattendu: {type(payload).__name__}")


def extract_datasets():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[EXTRACT] GET {API_URL}")

    with _session_with_retries() as session:
        response = session.get(
            API_URL,
            headers={"Accept": "application/json", "User-Agent": "transport-etl-exercise/1.0"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()

    datasets = _get_dataset_list(payload)
    if MAX_DATASETS is not None:
        datasets = datasets[:MAX_DATASETS]

    # Conserve la réponse complète pour audit / rejeu local.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = RAW_DIR / f"datasets_{timestamp}.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(datasets, f, ensure_ascii=False, indent=2)

    print(f"[EXTRACT] Retrieved {len(datasets)} datasets")
    return raw_path, datasets
