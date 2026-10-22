# Transport Data ETL — exercice PySpark

Pipeline ETL pédagogique utilisant l'API publique :
`https://transport.data.gouv.fr/api/datasets`

## Objectif

1. **Extract** : appeler l'API avec timeout et retries, sauvegarder le JSON brut.
2. **Transform** : normaliser les métadonnées des datasets et les ressources associées.
3. **Quality** : mesurer les valeurs nulles et calculer un résumé par format.
4. **Load** : écrire les tables en Parquet et les rapports en CSV.

## Prérequis

- Python 3.10 à 3.12 conseillé
- Java compatible avec votre version de PySpark
- macOS, Linux ou Windows

## Installation

```bash
python -m venv .venv
```

macOS/Linux :
```bash
source .venv/bin/activate
```

Windows PowerShell :
```powershell
.venv\Scripts\Activate.ps1
```

Puis :
```bash
pip install -r requirements.txt
python main.py
```

## Outputs attendus

- `data/raw/datasets_<timestamp>.json` : réponse brute sauvegardée
- `outputs/datasets.parquet/` : métadonnées des jeux de données
- `outputs/resources.parquet/` : métadonnées des ressources
- `outputs/quality_report.csv/` : valeurs nulles et complétude par colonne
- `outputs/summary_by_format.csv/` : nombre de ressources par format

**Note :** Spark écrit les fichiers dans des dossiers. Dans les dossiers `.csv`, le fichier de données s'appelle `part-*.csv`; le dossier peut aussi contenir `_SUCCESS`.

## Exercices à réaliser

### Niveau 1 — Fondamentaux
- [ ] Exécuter le pipeline et examiner le JSON brut.
- [ ] Ajouter une colonne `has_description`.
- [ ] Normaliser les formats (`CSV`, `csv`, `Csv` → `csv`).
- [ ] Identifier les datasets sans ressource.

### Niveau 2 — Qualité
- [ ] Détecter les doublons selon `dataset_id`.
- [ ] Créer `rejected_datasets.parquet` pour les lignes sans identifiant ou titre.
- [ ] Ajouter le taux de doublons et le taux de valeurs nulles au rapport qualité.

### Niveau 3 — Data Engineering
- [ ] Ajouter une configuration via variables d'environnement.
- [ ] Implémenter un mode incrémental basé sur `last_update`.
- [ ] Ajouter des logs structurés et des tests unitaires.
- [ ] Ajouter Docker et une CI GitHub Actions.
- [ ] Charger les tables dans PostgreSQL au lieu de fichiers uniquement.

## Points d'attention

- La forme exacte du JSON peut évoluer. Si l'API renvoie une enveloppe différente, adaptez `_get_dataset_list()` dans `src/extract.py`.
- `MAX_DATASETS = 200` limite les données pour un premier test. Passez à `None` pour traiter toute la liste.
- Le nombre de datasets retournés peut être important : ne téléchargez pas les fichiers de ressources tant que vous ne maîtrisez pas le pipeline de métadonnées.
- Pour inspecter le contrat de l'API, consultez https://transport.data.gouv.fr/swaggerui.
