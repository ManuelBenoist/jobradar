import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

import boto3
import requests

# Configuration du logging pour AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation globale du client S3 pour optimiser les performances (Reuse execution context)
s3_client = boto3.client("s3")


def fetch_adzuna_jobs(
    what: str, where: str, app_id: str, app_key: str
) -> Dict[str, Any]:
    """
    Récupère les offres d'emploi via l'API Adzuna avec gestion de la pagination.

    Args:
        what (str): Mot-clé de recherche (ex: Data Engineer).
        where (str): Localisation géographique.
        app_id (str): Identifiant de l'application Adzuna.
        app_key (str): Clé secrète de l'application Adzuna.

    Returns:
        Dict[str, Any]: Dictionnaire contenant le compte total et la liste des résultats.
    """
    current_page = 1
    combined_results = []
    total_count = None

    # Limitation préventive pour respecter les quotas d'exécution Lambda
    MAX_PAGES = 3

    while current_page <= MAX_PAGES:
        endpoint = f"https://api.adzuna.com/v1/api/jobs/fr/search/{current_page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "what": what,
            "where": where,
            "distance": 20,
            "results_per_page": 50,
        }

        logger.info(f"Requête Adzuna - Page {current_page} - Keyword: {what}")

        try:
            response = requests.get(endpoint, params=params, timeout=15)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Échec de l'appel API Adzuna à la page {current_page}: {e}")
            break

        if total_count is None:
            total_count = payload.get("count", 0)

        page_results = payload.get("results", [])
        combined_results.extend(page_results)

        # Arrêt si la page est vide ou si on a atteint la fin des résultats
        if not page_results:
            break

        current_page += 1

    if not isinstance(combined_results, list):
        raise ValueError(
            f"Schéma invalide : 'results' devrait être une liste, reçu {type(combined_results).__name__}"
        )

    return {
        "count": total_count,
        "results": combined_results,
        "keyword": what,
        "ingested_at": datetime.now().isoformat(),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Point d'entrée de la fonction Lambda déclenchée par EventBridge.
    Gère l'extraction Adzuna et le stockage sur S3 (Couche Bronze).
    """
    try:
        # 1. Chargement de la configuration via les variables d'environnement
        bucket_name = os.environ["BUCKET_NAME"]
        app_id = os.environ["ADZUNA_APP_ID"]
        app_key = os.environ["ADZUNA_APP_KEY"]

        # 2. Paramétrage de la recherche via l'événement déclencheur
        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Lancement de l'ingestion Adzuna | Keyword: {keyword}")

        # 3. Extraction des données
        data = fetch_adzuna_jobs(keyword, where, app_id, app_key)

        # Contrôle de schéma : validation du payload retourné
        if "results" not in data:
            raise ValueError("Schéma invalide : clé 'results' absente du payload")
        if not isinstance(data.get("results"), list):
            raise ValueError(
                f"Schéma invalide : 'results' devrait être une liste, reçu {type(data.get('results')).__name__}"
            )

        # 4. Génération du chemin de stockage (Architecture Médaillon - Bronze)
        now = datetime.now()
        safe_keyword = keyword.replace(" ", "_").lower()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%H%M%S")

        # Format : bronze/adzuna/YYYY/MM/DD/keyword_HHMMSS.json
        filename = f"adzuna/{date_path}/{safe_keyword}_{timestamp}.json"

        # 5. Persistance des données brutes sur S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json",
        )

        logger.info(
            f"✅ Ingestion réussie : {filename} ({len(data['results'])} offres)"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Ingestion completed",
                    "file": filename,
                    "count": len(data["results"]),
                }
            ),
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique lors de l'exécution : {str(e)}")
        # On relance l'exception pour permettre les mécanismes de "Retry" d'AWS
        raise e
