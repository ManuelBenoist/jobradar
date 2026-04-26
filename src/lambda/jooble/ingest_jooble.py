import json
import logging
import os
import requests
import boto3
from datetime import datetime

# Configuration du logger pour AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

def fetch_jooble_jobs(keyword: str, where: str, api_key: str) -> dict:
    """Fetch les offres d'emploi depuis Jooble via POST request."""
    # URL de l'API standard Jooble
    endpoint = f"https://fr.jooble.org/api/{api_key}"
    
    headers = {"Content-Type": "application/json"}
    
    # Payload minimaliste
    payload = {
        "keywords": keyword,
        "location": where
    }

    logger.info(f"Appel Jooble pour : {keyword} à {where}")
    
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # Extraction des résultats (clé 'jobs')
        results = data.get("jobs", [])
        
        return {
            "count": data.get("totalCount", len(results)),
            "results": results,
            "keyword": keyword,
            "source": "jooble"
        }
    except Exception as e:
        logger.error(f"Erreur API Jooble : {str(e)}")
        raise e

def lambda_handler(event, context):
    """Point d'entrée AWS Lambda"""
    try:
        # 1. Récupération des paramètres
        bucket_name = os.environ["BUCKET_NAME"]
        api_key = os.environ["JOOBLE_API_KEY"]

        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Démarrage Ingestion Jooble : {keyword}")

        # 2. Ingestion
        data = fetch_jooble_jobs(keyword, where, api_key)

        # 3. Partitionnement S3 (Bronze Layer)
        safe_keyword = keyword.replace(" ", "_").lower()
        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        filename = f"jooble/{date_path}/{safe_keyword}_{now.strftime('%H%M%S')}.json"

        # 4. Écriture S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False)
        )

        logger.info(f"✅ {len(data['results'])} offres Jooble stockées dans {filename}")
        
        return {
            "statusCode": 200,
            "body": f"Succès Jooble : {len(data['results'])} offres."
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique Jooble : {str(e)}")
        raise e