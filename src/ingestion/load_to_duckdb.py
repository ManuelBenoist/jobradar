import logging
from pathlib import Path

import duckdb
from src.utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

DB_PATH = Path("jobradar.duckdb")
SOURCE_RESULT_FIELD = {
    "adzuna": "results",
    "france_travail": "resultats",
}


def load_source_to_duckdb(source_name: str, file_pattern: str):
    """
    Charge les fichiers JSON bruts d'une source dans sa propre table DuckDB (raw_source).
    """
    raw_dir = Path("data") / "raw"
    files = list(raw_dir.glob(file_pattern))

    if not files:
        logger.warning(
            "⚠️ Aucun fichier trouvé pour %s (%s).", source_name, file_pattern
        )
        return

    logger.info(
        "🔄 Chargement de %s fichiers pour %s dans DuckDB...", len(files), source_name
    )

    source_field = SOURCE_RESULT_FIELD.get(source_name, "results")
    table_name = f"raw_{source_name}"

    con = duckdb.connect(DB_PATH)
    try:
        # Idempotence : on supprime la table si elle existe pour la recréer proprement
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Création de la table dédiée à la source
        query = f"""
            CREATE TABLE {table_name} AS 
            SELECT unnest({source_field}) AS job_data
            FROM read_json_auto('data/raw/{file_pattern}', ignore_errors=true)
        """
        con.execute(query)

        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info("✅ %s offres chargées dans la table %s.", count, table_name)

    except Exception as exc:
        logger.error("❌ Erreur lors du chargement de %s : %s", source_name, exc)
    finally:
        con.close()


def main():
    logger.info("🚀 Début de la synchronisation vers DuckDB (Landing Zone)")

    load_source_to_duckdb("adzuna", "adzuna_*.json")
    load_source_to_duckdb("france_travail", "france_travail_*.json")

    logger.info("🏁 Synchronisation terminée.")


if __name__ == "__main__":
    main()
