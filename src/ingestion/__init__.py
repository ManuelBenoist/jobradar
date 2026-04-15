"""Ingestion multi-sources des offres d'emploi (France Travail, Adzuna, etc.)."""

from .fetch_adzuna import fetch_adzuna_jobs
from .fetch_france_travail import fetch_france_travail_offers
from .orchestrator import DEFAULT_SEARCH_KEYWORDS, run_daily_ingestion

__all__ = [
    "fetch_adzuna_jobs",
    "fetch_france_travail_offers",
    "run_daily_ingestion",
    "DEFAULT_SEARCH_KEYWORDS",
]
