import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Ensure the top-level src package is importable when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

_TOKEN_CACHE: dict[str, datetime] = {
    "access_token": None,
    "expires_at": None,
}


def _require_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable '{name}'. "
            "Set FRANCE_TRAVAIL_CLIENT_ID and FRANCE_TRAVAIL_CLIENT_SECRET in your environment."
        )
    return value


def _get_oauth_token_url() -> str:
    return os.getenv(
        "FRANCE_TRAVAIL_OAUTH_TOKEN_URL",
        "https://auth.francetravail.io/oauth2/token",
    )


def _get_api_url() -> str:
    return os.getenv(
        "FRANCE_TRAVAIL_API_URL",
        "https://api.francetravail.io/offres/v2/search",
    )


def _get_access_token(force_refresh: bool = False) -> str:
    now = datetime.now(timezone.utc)
    cached_token = _TOKEN_CACHE.get("access_token")
    expires_at = _TOKEN_CACHE.get("expires_at")
    
    if cached_token and expires_at and not force_refresh:
        if now < expires_at - timedelta(seconds=30):
            return cached_token

    client_id = _require_env_var("FRANCE_TRAVAIL_CLIENT_ID")
    client_secret = _require_env_var("FRANCE_TRAVAIL_CLIENT_SECRET")
    token_url = _get_oauth_token_url()

    logger.info("Requesting France Travail OAuth2 token...")

    # France Travail préfère souvent les credentials dans le corps de la requête
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api_offresdemploiv2 o2dso"
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    response = requests.post(
        token_url,
        data=payload,  # On envoie tout ici
        headers=headers,
        timeout=30
    )

    if response.status_code != 200:
        logger.error("OAuth2 Error Details: %s", response.text) # Pour voir le vrai message d'erreur du serveur
        response.raise_for_status()

    token_payload = response.json()
    access_token = token_payload.get("access_token")
    
    expires_in = int(token_payload.get("expires_in", 3600))
    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = now + timedelta(seconds=expires_in)

    logger.info("France Travail OAuth2 token acquired.")
    return access_token


def _get_output_path(base_dir: Path, batch_id: str, page: int) -> Path:
    filename = f"france_travail_{batch_id}.json" if page == 1 else f"france_travail_{batch_id}_page{page}.json"
    return base_dir / filename


def _get_batch_id(batch_id: Optional[str]) -> str:
    return batch_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _get_range(start: int, per_page: int) -> str:
    return f"{start}-{start + per_page - 1}"


def _validate_per_page(per_page: int) -> int:
    if per_page < 1 or per_page > 150:
        raise ValueError("per_page must be between 1 and 150 for France Travail API range requests.")
    return per_page


def fetch_france_travail_offers(
    filter_departement: int = 44,
    keywords: str = "Data Engineer DevOps Cloud",
    page: int = 1,
    per_page: int = 50,
    fetch_all: bool = False,
    max_pages: Optional[int] = None,
    batch_id: Optional[str] = None,
) -> dict:
    """Fetch France Travail offers and save raw pages to data/raw.

    Args:
        filter_departement: Department code to filter results (44 for Loire Atlantique).
        keywords: Business keywords used by France Travail to narrow the search.
        page: Starting page number for the range-based pagination.
        per_page: Number of records per range request (must be between 1 and 150).
        fetch_all: If True, continue fetching until the API indicates no more results.
        max_pages: Optional cap on the number of range requests.
        batch_id: Optional batch identifier to unify raw files across sources.

    Returns:
        Combined JSON payload containing the fetched offers and optional page count.
    """
    batch_id = _get_batch_id(batch_id)
    per_page = _validate_per_page(per_page)

    raw_dir = Path("data") / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    start = (page - 1) * per_page
    combined_results: list[dict] = []
    total_count = None
    fetched_pages = 0
    current_page = page

    while True:
        token = _get_access_token()
        endpoint = _get_api_url()
        params = {
            "range": _get_range(start, per_page),
            "departement": filter_departement,
            "motsCles": keywords,
        }

        logger.info(
            "Requesting France Travail offers range=%s departement=%s",
            params["range"],
            filter_departement,
        )
        response = requests.get(
            endpoint,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        logger.info("France Travail response status=%s", response.status_code)

        if response.status_code == 429:
            raise RuntimeError("France Travail API rate limited (HTTP 429). Please retry later.")

        if response.status_code in {401, 403}:
            logger.warning(
                "France Travail API returned %s. Refreshing token and retrying once.",
                response.status_code,
            )
            token = _get_access_token(force_refresh=True)
            response = requests.get(
                endpoint,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if response.status_code in {401, 403, 429}:
                response.raise_for_status()

        response.raise_for_status()
        if response.status_code == 204:
            logger.info("No content returned (204) for this range.")
            if total_count is None:
                total_count = 0
            break

        payload = response.json()

        if total_count is None:
            total_count = payload.get("count")

        page_results = payload.get("results") or []
        combined_results.extend(page_results)
        fetched_pages += 1

        output_path = _get_output_path(raw_dir, batch_id, current_page)
        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)

        logger.info("Saved raw France Travail payload to %s", output_path)
        logger.info("Range %s returned %s offer(s)", params["range"], len(page_results))

        if not fetch_all:
            break

        if max_pages is not None and fetched_pages >= max_pages:
            logger.info("Reached max_pages=%s, stopping pagination.", max_pages)
            break

        if isinstance(total_count, int) and len(combined_results) >= total_count:
            logger.info(
                "Fetched all %s offers according to count, stopping pagination.",
                total_count,
            )
            break

        if len(page_results) == 0:
            logger.info("No more results returned at range=%s, stopping pagination.", params["range"])
            break

        start += per_page
        current_page += 1

    combined_payload = {
        "count": total_count if isinstance(total_count, int) else len(combined_results),
        "results": combined_results,
    }
    if fetched_pages > 1:
        combined_payload["pages"] = fetched_pages

    return combined_payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch France Travail job offers and save raw JSON.")
    parser.add_argument("--filter-departement", type=int, default=44, help="Département de recherche")
    parser.add_argument(
        "--keywords",
        default="Data Engineer DevOps Cloud",
        help="Mots clés métiers pour filtrer les offres",
    )
    parser.add_argument("--page", type=int, default=1, help="Page de départ pour la pagination range")
    parser.add_argument("--per-page", type=int, default=50, help="Nombre d'offres par plage (max 150)")
    parser.add_argument("--fetch-all", action="store_true", help="Récupérer toutes les pages disponibles")
    parser.add_argument("--max-pages", type=int, default=None, help="Nombre maximal de pages à récupérer")
    parser.add_argument("--batch-id", type=str, default=None, help="Identifiant de batch pour nommer les fichiers bruts")
    args = parser.parse_args()

    try:
        result = fetch_france_travail_offers(
            filter_departement=args.filter_departement,
            keywords=args.keywords,
            page=args.page,
            per_page=args.per_page,
            fetch_all=args.fetch_all,
            max_pages=args.max_pages,
            batch_id=args.batch_id,
        )

        print(
            f"Fetched {result.get('count', len(result.get('results', [])))} offers "
            f"across {result.get('pages', 1)} page(s)"
        )
    except EnvironmentError as exc:
        logger.warning(str(exc))
        logger.warning(
            "France Travail skipped. Set FRANCE_TRAVAIL_CLIENT_ID and FRANCE_TRAVAIL_CLIENT_SECRET in .env to enable this source."
        )
        sys.exit(0)
