import json
import logging
import os
import requests
import boto3
from datetime import datetime
from typing import Dict, Any

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation du client S3 (optimisation du contexte d'exécution)
s3_client = boto3.client("s3")

# Constants
FT_TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
FT_API_ENDPOINT = (
    "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
)


def get_ft_access_token(client_id: str, client_secret: str) -> str:
    """
    Récupère un jeton d'accès OAuth2 via le protocole Client Credentials de France Travail.

    Args:
        client_id (str): ID client partenaire.
        client_secret (str): Secret client partenaire.

    Returns:
        str: Access Token valide pour les requêtes API.
    """
    params = {"realm": "/partenaire"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    logger.info("Authentification OAuth2 auprès de France Travail...")

    try:
        response = requests.post(
            FT_TOKEN_URL, params=params, data=payload, headers=headers, timeout=15
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        logger.error(f"Échec de l'authentification OAuth2: {e}")
        raise


def fetch_france_travail_offers(
    keywords: str, departement: int, client_id: str, client_secret: str
) -> Dict[str, Any]:
    """
    Interroge l'API des offres d'emploi v2 de France Travail.

    Args:
        keywords (str): Mots-clés de recherche.
        departement (int): Code départemental (ex: 44).
        client_id/secret: Identifiants OAuth2.

    Returns:
        Dict[str, Any]: Payload structuré avec les résultats de l'API.
    """
    token = get_ft_access_token(client_id, client_secret)

    # Paramètres de recherche (limité aux 50 premiers résultats pour l'ingestion quotidienne)
    params = {
        "range": "0-49",
        "departement": departement,
        "motsCles": keywords,
    }

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    logger.info(
        f"Appel API France Travail | Keywords: {keywords} | Dept: {departement}"
    )

    response = requests.get(FT_API_ENDPOINT, params=params, headers=headers, timeout=15)

    # Gestion spécifique du code 204 (No Content) fréquent sur cette API
    if response.status_code == 204:
        logger.info("Aucune nouvelle offre trouvée pour ces critères.")
        return {"count": 0, "results": [], "keyword": keywords}

    response.raise_for_status()
    payload = response.json()

    return {
        "count": payload.get("nbResultats", 0),
        "results": payload.get("resultats", []),
        "keyword": keywords,
        "ingested_at": datetime.now().isoformat(),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal pour l'ingestion France Travail vers S3 (Couche Bronze).
    """
    try:
        # 1. Configuration et Variables d'environnement
        bucket_name = os.environ["BUCKET_NAME"]
        client_id = os.environ["FT_CLIENT_ID"]
        client_secret = os.environ["FT_CLIENT_SECRET"]

        # 2. Parsing de l'event (Trigger EventBridge ou manuel)
        keyword = event.get("keyword", "Data Engineer")
        departement = event.get("departement", 44)

        # 3. Extraction
        data = fetch_france_travail_offers(
            keyword, departement, client_id, client_secret
        )

        # 4. Organisation du stockage (Medallion Architecture)
        now = datetime.now()
        safe_keyword = keyword.replace(" ", "_").lower()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%H%M%S")

        # Chemin structuré pour le partitionnement futur dans Glue/Athena
        filename = f"france_travail/{date_path}/{safe_keyword}_{timestamp}.json"

        # 5. Envoi vers S3 Bronze
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json",
        )

        logger.info(f"✅ Ingestion France Travail terminée: {filename}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "source": "france_travail",
                    "file": filename,
                    "count": len(data["results"]),
                }
            ),
        }

    except Exception as e:
        logger.error(f"❌ Erreur critique lors de l'ingestion FT: {str(e)}")
        raise e
