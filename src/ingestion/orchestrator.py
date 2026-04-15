import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

# Ensure the top-level src package is importable when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.fetch_adzuna import fetch_adzuna_jobs
from ingestion.fetch_france_travail import fetch_france_travail_offers
from utils.common import get_batch_id, slugify_query
from utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

DEFAULT_SEARCH_KEYWORDS = [
    "Data Engineer",
    "Data Analyst",
    "DevOps",
    "Ingénieur DevOps",
    "Machine Learning",
]
DEFAULT_ADZUNA_WHERE = "Nantes"
DEFAULT_ADZUNA_DISTANCE = 20
DEFAULT_FRANCE_TRAVAIL_DEPARTEMENT = 44
DEFAULT_THROTTLE_SECONDS = 0.2


def run_daily_ingestion(
    keywords: Optional[Iterable[str]] = None,
    france_travail_departement: int = DEFAULT_FRANCE_TRAVAIL_DEPARTEMENT,
    france_travail_per_page: int = 150,
    france_travail_fetch_all: bool = True,
    france_travail_max_pages: Optional[int] = None,
    adzuna_where: str = DEFAULT_ADZUNA_WHERE,
    adzuna_distance: int = DEFAULT_ADZUNA_DISTANCE,
    adzuna_results_per_page: int = 50,
    adzuna_fetch_all: bool = False,
    adzuna_max_pages: Optional[int] = 3,
    batch_id: Optional[str] = None,
    throttle_seconds: float = DEFAULT_THROTTLE_SECONDS,
) -> dict[str, dict]:
    """Collecte quotidienne des offres depuis France Travail et Adzuna."""
    keywords = list(keywords or DEFAULT_SEARCH_KEYWORDS)
    batch_id = get_batch_id(batch_id)
    results: dict[str, dict] = {}

    logger.info(
        "🚀 Starting daily ingestion batch=%s with %s keyword(s)",
        batch_id,
        len(keywords),
    )

    for keyword in keywords:
        query_label = slugify_query(keyword)
        logger.info(
            "🚀 Début fetch France Travail keyword=%s batch=%s",
            keyword,
            batch_id,
        )

        try:
            results[f"france_travail/{query_label}"] = fetch_france_travail_offers(
                filter_departement=france_travail_departement,
                keywords=keyword,
                per_page=france_travail_per_page,
                fetch_all=france_travail_fetch_all,
                max_pages=france_travail_max_pages,
                batch_id=batch_id,
                query_name=query_label,
            )
        except EnvironmentError as exc:
            logger.warning("France Travail ingestion skipped for %s: %s", keyword, exc)
        except Exception as exc:
            logger.exception("France Travail ingestion failed for %s", keyword)

        time.sleep(throttle_seconds)

        logger.info(
            "🚀 Début fetch Adzuna keyword=%s batch=%s",
            keyword,
            batch_id,
        )

        try:
            results[f"adzuna/{query_label}"] = fetch_adzuna_jobs(
                what=keyword,
                where=adzuna_where,
                distance=adzuna_distance,
                results_per_page=adzuna_results_per_page,
                fetch_all=adzuna_fetch_all,
                max_pages=adzuna_max_pages,
                batch_id=batch_id,
                query_name=query_label,
            )
        except EnvironmentError as exc:
            logger.warning("Adzuna ingestion skipped for %s: %s", keyword, exc)
        except Exception:
            logger.exception("Adzuna ingestion failed for %s", keyword)

        time.sleep(throttle_seconds)

    logger.info("✅ Daily ingestion batch=%s completed", batch_id)
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run daily raw ingestion for France Travail and Adzuna.")
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=DEFAULT_SEARCH_KEYWORDS,
        help="Liste de mots-clés à rechercher.",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Identifiant de batch pour regrouper les raw files.",
    )
    parser.add_argument(
        "--throttle-seconds",
        type=float,
        default=DEFAULT_THROTTLE_SECONDS,
        help="Temps d'attente entre deux appels API.",
    )
    parser.add_argument(
        "--france-travail-per-page",
        type=int,
        default=150,
        help="Nombre de résultats par page France Travail (max 150).",
    )
    parser.add_argument(
        "--france-travail-max-pages",
        type=int,
        default=None,
        help="Nombre max de pages France Travail lors de fetch_all.",
    )
    parser.add_argument(
        "--no-france-travail-fetch-all",
        action="store_true",
        help="Ne pas paginer entièrement France Travail.",
    )
    parser.add_argument(
        "--adzuna-results-per-page",
        type=int,
        default=50,
        help="Nombre de résultats Adzuna par page.",
    )
    parser.add_argument(
        "--adzuna-max-pages",
        type=int,
        default=3,
        help="Nombre max de pages Adzuna à récupérer.",
    )
    parser.add_argument(
        "--adzuna-fetch-all",
        action="store_true",
        help="Paginer entièrement Adzuna.",
    )
    args = parser.parse_args()

    run_daily_ingestion(
        keywords=args.keywords,
        batch_id=args.batch_id,
        throttle_seconds=args.throttle_seconds,
        france_travail_fetch_all=not args.no_france_travail_fetch_all,
        france_travail_max_pages=args.france_travail_max_pages,
        adzuna_results_per_page=args.adzuna_results_per_page,
        adzuna_fetch_all=args.adzuna_fetch_all,
        adzuna_max_pages=args.adzuna_max_pages,
    )
