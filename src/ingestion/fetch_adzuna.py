import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Ensure the top-level src package is importable when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
from dotenv import load_dotenv

from utils.logging_utils import mask_url

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)


def _require_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable '{name}'. "
            "Set ADZUNA_APP_ID and ADZUNA_APP_KEY in your environment."
        )
    return value


def _get_output_path(base_dir: Path, batch_id: str, page: int) -> Path:
    return base_dir / f"adzuna_{batch_id}_page{page}.json"


def fetch_adzuna_jobs(
    what: str = "Data Engineer",
    where: str = "Nantes",
    distance: int = 20,
    page: int = 1,
    results_per_page: int = 50,
    fetch_all: bool = False,
    max_pages: Optional[int] = None,
) -> dict:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    """Fetch Adzuna job offers and store the raw payload.

    Args:
        what: Job title or keywords to search.
        where: Location to search.
        distance: Distance in kilometers around the location.
        page: Page number to start fetching from.
        results_per_page: Number of offers to request per page.
        fetch_all: If True, fetch all available pages starting from `page`.
        max_pages: Optional upper bound on the number of pages to fetch.

    Returns:
        The combined JSON payload of the fetched page(s).
    """
    app_id = _require_env_var("ADZUNA_APP_ID")
    app_key = _require_env_var("ADZUNA_APP_KEY")

    raw_dir = Path("data") / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    current_page = page
    combined_results: list[dict] = []
    total_count = None
    fetched_pages = 0

    while True:
        endpoint = f"https://api.adzuna.com/v1/api/jobs/fr/search/{current_page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "what": what,
            "where": where,
            "distance": distance,
            "results_per_page": results_per_page,
        }

        logger.info(
            "Requesting Adzuna jobs page=%s what=%s where=%s distance=%s",
            current_page,
            what,
            where,
            distance,
        )
        response = requests.get(endpoint, params=params, timeout=30)
        logger.info(
            "Adzuna response status=%s url=%s",
            response.status_code,
            mask_url(response.url),
        )
        response.raise_for_status()

        payload = response.json()
        if total_count is None:
            total_count = payload.get("count")

        page_results = payload.get("results") or []
        combined_results.extend(page_results)
        fetched_pages += 1

        output_path = _get_output_path(raw_dir, batch_id, current_page)
        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)

        logger.info("Saved raw Adzuna payload to %s", output_path)
        logger.info("Page %s returned %s offer(s)", current_page, len(page_results))

        if not fetch_all:
            break

        if max_pages is not None and fetched_pages >= max_pages:
            logger.info("Reached max_pages=%s, stopping pagination.", max_pages)
            break

        if isinstance(total_count, int) and len(combined_results) >= total_count:
            logger.info("Fetched all %s offers according to count, stopping pagination.", total_count)
            break

        if len(page_results) == 0:
            logger.info("No more results returned at page=%s, stopping pagination.", current_page)
            break

        current_page += 1

    offers_count = len(combined_results)
    logger.info(
        "Adzuna fetched %s total offer(s) across %s page(s)",
        offers_count,
        fetched_pages,
    )

    combined_payload = {"count": total_count if isinstance(total_count, int) else offers_count, "results": combined_results}
    if fetched_pages > 1:
        combined_payload["pages"] = fetched_pages

    return combined_payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Adzuna job offers and save raw JSON.")
    parser.add_argument("--what", default="Data Engineer", help="Job title or keywords")
    parser.add_argument("--where", default="Nantes", help="Location")
    parser.add_argument("--distance", type=int, default=20, help="Distance in km")
    parser.add_argument("--page", type=int, default=1, help="Page number to start fetching from")
    parser.add_argument(
        "--results-per-page",
        type=int,
        default=50,
        help="Number of results per page",
    )
    parser.add_argument(
        "--fetch-all",
        action="store_true",
        help="Fetch all available result pages starting from the requested page",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to fetch when --fetch-all is enabled",
    )
    args = parser.parse_args()

    result = fetch_adzuna_jobs(
        what=args.what,
        where=args.where,
        distance=args.distance,
        page=args.page,
        results_per_page=args.results_per_page,
        fetch_all=args.fetch_all,
        max_pages=args.max_pages,
    )

    print(
        f"Fetched {result.get('count', len(result.get('results', [])))} offers "
        f"across {result.get('pages', 1)} page(s)"
    )
