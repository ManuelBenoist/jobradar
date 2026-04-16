import json
import logging
import os
import requests
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')

def get_ft_access_token(client_id: str, client_secret: str) -> str:
    token_url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
    
    # France Travail a besoin du paramètre realm=/partenaire
    params = {"realm": "/partenaire"}
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    logger.info("Demande de Token France Travail (Partenaire)...")
    response = requests.post(token_url, params=params, data=payload, headers=headers, timeout=15)
    
    if response.status_code != 200:
        logger.error(f"Erreur OAuth: {response.text}")
        response.raise_for_status()
        
    return response.json().get("access_token")

def fetch_france_travail_offers(keywords: str, departement: int, client_id: str, client_secret: str) -> dict:
    token = get_ft_access_token(client_id, client_secret)
    
    # URL EXACTE POUR L'API
    endpoint = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
    
    params = {
        "range": "0-49",
        "departement": departement,
        "motsCles": keywords,
    }

    logger.info(f"Appel API France Travail pour : {keywords}")
    response = requests.get(
        endpoint,
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )

    if response.status_code == 204:
        return {"count": 0, "results": [], "keyword": keywords}

    response.raise_for_status()
    payload = response.json()
    
    return {
        "count": payload.get("nbResultats", 0),
        "results": payload.get("resultats", []),
        "keyword": keywords
    }

def lambda_handler(event, context):
    try:
        bucket_name = os.environ['BUCKET_NAME']
        client_id = os.environ['FT_CLIENT_ID']
        client_secret = os.environ['FT_CLIENT_SECRET']
        
        keyword = event.get('keyword', "Data Engineer")
        departement = event.get('departement', 44)
        
        data = fetch_france_travail_offers(keyword, departement, client_id, client_secret)
        
        safe_keyword = keyword.replace(" ", "_").lower()
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        date_path = datetime.now().strftime('%Y/%m/%d')
        
        filename = f"france_travail/{date_path}/{safe_keyword}_{date_str}.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False)
        )
        
        return {'statusCode': 200, 'body': f"Succès FT: {filename}"}
    except Exception as e:
        logger.error(f"❌ Erreur: {str(e)}")
        raise e
