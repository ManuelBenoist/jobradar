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

# Initialisation du client S3 (optimisation du Cold Start)
s3_client = boto3.client("s3")


def fetch_jooble_jobs(keyword: str, where: str, api_key: str) -> Dict[str, Any]:
    """
    Récupère les offres d'emploi depuis l'API Jooble via une requête POST.

    Args:
        keyword (str): Mots-clés de recherche.
        where (str): Localisation (ex: Nantes).
        api_key (str): Clé API fournie par Jooble.

    Returns:
        Dict[str, Any]: Résultats structurés incluant la liste des offres.
    """
    # L'API Jooble intègre la clé directement dans l'URL
    endpoint = f"https://fr.jooble.org/api/{api_key}"
    headers = {"Content-Type": "application/json"}

    # Corps de la requête
    payload = {"keywords": keyword, "location": where}

    logger.info(f"Requête Jooble | Keyword: {keyword} | Location: {where}")

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()

        # Jooble renvoie les résultats dans la clé 'jobs'
        results = data.get("jobs", [])

        return {
            "count": data.get("totalCount", len(results)),
            "results": results,
            "keyword": keyword,
            "source": "jooble",
            "ingested_at": datetime.now().isoformat(),
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de l'appel API Jooble : {str(e)}")
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Point d'entrée AWS Lambda pour l'ingestion Jooble.
    Sauvegarde les données brutes dans la couche Bronze de S3.
    """
    try:
        # 1. Récupération de la configuration
        bucket_name = os.environ["BUCKET_NAME"]
        api_key = os.environ["JOOBLE_API_KEY"]

        # 2. Paramètres de l'événement (EventBridge ou manuel)
        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Démarrage Ingestion Jooble pour : {keyword}")

        # 3. Extraction des données
        data = fetch_jooble_jobs(keyword, where, api_key)

        # 4. Organisation du stockage S3 (Architecture Médaillon)
        now = datetime.now()
        safe_keyword = keyword.replace(" ", "_").lower()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%H%M%S")

        # Chemin partitionné pour optimiser les requêtes Athena futures
        filename = f"jooble/{date_path}/{safe_keyword}_{timestamp}.json"

        # 5. Écriture sur S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json",
        )

        logger.info(
            f"✅ {len(data['results'])} offres Jooble sauvegardées dans {filename}"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Jooble ingestion successful",
                    "count": len(data["results"]),
                    "file": filename,
                }
            ),
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique lors de l'ingestion Jooble : {str(e)}")
        raise e
