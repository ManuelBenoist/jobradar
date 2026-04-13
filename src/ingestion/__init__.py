"""Ingestion multi-sources des offres d'emploi (France Travail, Adzuna, etc.)."""

from .fetch_adzuna import fetch_adzuna_jobs

__all__ = ["fetch_adzuna_jobs"]
