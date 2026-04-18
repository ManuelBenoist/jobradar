import json
import logging
import os
import requests
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# On initialise le client S3 en dehors du handler pour l'optimisation (Cold Start)
s3_client = boto3.client("s3")


def fetch_adzuna_jobs(what: str, where: str, app_id: str, app_key: str) -> dict:
    """Fetch les offres d'emploi depuis l'API Adzuna en paginant les résultats."""

    current_page = 1
    combined_results = []
    total_count = None

    # On limite à 3 pages max dans le cloud pour éviter de surcharger la Lambda
    max_pages = 3

    while current_page <= max_pages:
        endpoint = f"https://api.adzuna.com/v1/api/jobs/fr/search/{current_page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "what": what,
            "where": where,
            "distance": 20,
            "results_per_page": 50,
        }

        logger.info(f"Appel Adzuna: Page {current_page} - Mot-clé: {what}")
        response = requests.get(endpoint, params=params, timeout=15)
        response.raise_for_status()

        payload = response.json()
        if total_count is None:
            total_count = payload.get("count", 0)

        page_results = payload.get("results", [])
        combined_results.extend(page_results)

        if len(page_results) == 0:
            break

        current_page += 1

    return {"count": total_count, "results": combined_results, "keyword": what}


# ==========================================
# LE POINT D'ENTRÉE DE LA LAMBDA
# ==========================================
def lambda_handler(event, context):
    """
    event: C'est ce que EventBridge va envoyer (ex: {"keyword": "Data Engineer"})
    context: Infos sur la Lambda par AWS
    """
    try:
        # 1. Récupération des variables d'environnement (Sécurité)
        bucket_name = os.environ["BUCKET_NAME"]
        app_id = os.environ["ADZUNA_APP_ID"]
        app_key = os.environ["ADZUNA_APP_KEY"]

        # 2. Récupération du mot-clé depuis l'événement déclencheur
        # Si aucun mot-clé n'est envoyé, on met "Data Engineer" par défaut
        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Démarrage Ingestion Adzuna pour : {keyword}")

        # 3. Appel de ta fonction métier
        data = fetch_adzuna_jobs(keyword, where, app_id, app_key)

        # 4. Nettoyage du nom pour le fichier
        safe_keyword = keyword.replace(" ", "_").lower()
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # L'architecture Médaillon dans S3 : raw/adzuna/2026/04/16/data_engineer_1200.json
        date_path = datetime.now().strftime("%Y/%m/%d")
        filename = f"adzuna/{date_path}/{safe_keyword}_{date_str}.json"

        # 5. Envoi vers AWS S3
        s3_client.put_object(
            Bucket=bucket_name, Key=filename, Body=json.dumps(data, ensure_ascii=False)
        )

        logger.info(f"✅ Fichier sauvegardé dans S3 : {filename}")
        return {
            "statusCode": 200,
            "body": f"Ingestion réussie pour {keyword}. {len(data['results'])} offres.",
        }

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'ingestion: {str(e)}")
        # On remonte l'erreur à AWS pour qu'il la marque en rouge dans la console
        raise e
