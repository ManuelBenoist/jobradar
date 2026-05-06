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

# Constants
JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HOST = "jsearch.p.rapidapi.com"


def fetch_jsearch_jobs(keyword: str, where: str, api_key: str) -> Dict[str, Any]:
    """
    Récupère les offres d'emploi via l'API JSearch (RapidAPI).

    Args:
        keyword (str): Mots-clés de recherche.
        where (str): Localisation géographique.
        api_key (str): Clé RapidAPI.

    Returns:
        Dict[str, Any]: Résultats structurés incluant les offres brutes.
    """
    query = f"{keyword} in {where}"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": JSEARCH_HOST}

    # Configuration optimisée : on cible les 3 derniers jours pour l'ingestion quotidienne
    params = {
        "query": query,
        "country": "fr",
        "page": "1",
        "num_pages": "1",
        "date_posted": "3days",
    }

    logger.info(f"Appel API JSearch | Query: '{query}' | Region: fr")

    try:
        # Timeout étendu à 60s car JSearch est un agrégateur temps réel
        response = requests.get(JSEARCH_URL, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()

        results = payload.get("data", [])
        if not isinstance(results, list):
            raise ValueError(
                f"Schéma invalide : 'data' devrait être une liste, reçu {type(results).__name__}"
            )

        return {
            "count": len(results),
            "results": results,
            "keyword": keyword,
            "source": "jsearch",
            "ingested_at": datetime.now().isoformat(),
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Échec de l'appel JSearch : {str(e)}")
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal pour l'ingestion JSearch vers S3 Bronze.
    """
    try:
        # 1. Chargement de la configuration
        bucket_name = os.environ["BUCKET_NAME"]
        api_key = os.environ["JSEARCH_API_KEY"]

        # 2. Paramétrage via l'event
        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Démarrage Ingestion JSearch | {keyword} | {where}")

        # 3. Extraction
        data = fetch_jsearch_jobs(keyword, where, api_key)

        # Contrôle de schéma : validation du payload retourné
        if "results" not in data:
            raise ValueError("Schéma invalide : clé 'results' absente du payload")
        if not isinstance(data.get("results"), list):
            raise ValueError(
                f"Schéma invalide : 'results' devrait être une liste, reçu {type(data.get('results')).__name__}"
            )

        # 4. Partitionnement temporel pour la couche Bronze
        now = datetime.now()
        safe_keyword = keyword.replace(" ", "_").lower()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%H%M%S")

        filename = f"jsearch/{date_path}/{safe_keyword}_{timestamp}.json"

        # 5. Persistance S3 avec métadonnées
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json",
        )

        logger.info(
            f"✅ Ingestion terminée : {filename} ({len(data['results'])} offres)"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"source": "jsearch", "count": len(data["results"]), "file": filename}
            ),
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique JSearch : {str(e)}")
        raise e
