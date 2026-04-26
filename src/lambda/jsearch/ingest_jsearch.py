import json
import logging
import os
import requests
import boto3
from datetime import datetime

# Configuration du logger pour CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

def fetch_jsearch_jobs(keyword: str, where: str, api_key: str) -> dict:
    """Fetch les offres d'emploi depuis JSearch via RapidAPI."""
    url = "https://jsearch.p.rapidapi.com/search"
    query = f"{keyword} in {where}"
    
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    
    params = {
        "query": query,
        "country": "fr",
        "page": "1",
        "num_pages": "1",
        "date_posted": "3days" # Optimisé pour la prod : on ne prend que les nouveautés
    }

    logger.info(f"Appel JSearch pour : {query} (Country: fr)")
    
    try:
        # Timeout long car JSearch agrège plusieurs sources en temps réel
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        
        results = payload.get("data", [])
        return {
            "count": len(results),
            "results": results,
            "keyword": keyword,
            "source": "jsearch"
        }
    except Exception as e:
        logger.error(f"Erreur API JSearch : {str(e)}")
        raise e

def lambda_handler(event, context):
    """Point d'entrée AWS Lambda"""
    try:
        bucket_name = os.environ["BUCKET_NAME"]
        api_key = os.environ["JSEARCH_API_KEY"]

        keyword = event.get("keyword", "Data Engineer")
        where = event.get("where", "Nantes")

        logger.info(f"Démarrage Ingestion JSearch : {keyword} à {where}")

        data = fetch_jsearch_jobs(keyword, where, api_key)

        # Partitionnement S3 (Architecture Médaillon - Bronze)
        safe_keyword = keyword.replace(" ", "_").lower()
        now = datetime.now()
        filename = f"jsearch/{now.strftime('%Y/%m/%d')}/{safe_keyword}_{now.strftime('%H%M%S')}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False)
        )

        logger.info(f"✅ {len(data['results'])} offres JSearch stockées : {filename}")
        
        return {
            "statusCode": 200,
            "body": f"Succès JSearch : {len(data['results'])} offres."
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique JSearch : {str(e)}")
        raise e