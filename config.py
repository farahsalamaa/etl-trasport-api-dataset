from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
API_URL = "https://transport.data.gouv.fr/api/datasets"

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "outputs"

# Limite de test : None pour traiter tous les datasets renvoyés par l'API.
MAX_DATASETS = 200

REQUEST_TIMEOUT_SECONDS = 30
