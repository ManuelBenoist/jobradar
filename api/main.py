import logging
import os
import time

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pyathena import connect
from pyathena.cursor import DictCursor

# Chargement des variables d'environnement (Utile pour le développement local)
load_dotenv()

# Configuration du logging pour CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = FastAPI(
    title="JobRadar API",
    description="Interface d'accès aux données transformées (Couche Gold) via AWS Athena.",
    version="1.1.0",
)

# --- CONFIGURATION CORS ---
# Liste des origines autorisées à requêter l'API
ALLOWED_ORIGINS = [
    "https://jobradar-nantes.streamlit.app/",  # Production (Streamlit Cloud)
    "http://localhost:8501",  # Développement local (Streamlit par défaut)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],  # Autorise tous les verbes HTTP (GET, POST, etc.)
    allow_headers=["*"],  # Autorise tous les headers (dont X-API-KEY)
)

# Configuration des paramètres AWS Athena
REGION = os.getenv("AWS_REGION", "eu-west-3")
DB = os.getenv("ATHENA_DATABASE", "jobradar_db")
S3_STAGING = os.getenv("ATHENA_S3_STAGING_DIR")


@app.get("/", tags=["Health"])
def read_root():
    """Point d'entrée principal fournissant les liens vers la documentation."""
    return {
        "message": "API JobRadar - Opérationnelle (AWS Lambda)",
        "endpoints": {"health": "/health", "jobs": "/jobs", "documentation": "/docs"},
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Vérification de l'état de santé du service."""
    return {"status": "healthy", "service": "jobradar-api-serverless"}


@app.get("/health/pipeline", tags=["Health"])
def get_pipeline_health():
    """Récupère le dernier statut du pipeline depuis le catalogue Glue/Athena."""
    try:
        athena_client = boto3.client(
            "athena", region_name="eu-west-3"
        )  # Remplace par ta région

        # 1. Lancer la requête
        query = "SELECT status, run_at, records_count FROM jobradar_db.pipeline_logs ORDER BY run_at DESC LIMIT 1"
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "jobradar_db"},
            ResultConfiguration={
                "OutputLocation": "s3://jobradar-athena-results-manuel-cloud/health-checks/"
            },
        )

        query_execution_id = response["QueryExecutionId"]

        # 2. Attendre le résultat (Polling)
        state = "RUNNING"
        while state in ["QUEUED", "RUNNING"]:
            response_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = response_status["QueryExecution"]["Status"]["State"]
            if state in ["FAILED", "CANCELLED"]:
                raise Exception(
                    f"Athena query failed: {response_status['QueryExecution']['Status'].get('StateChangeReason')}"
                )
            time.sleep(1)  # Attendre 1 seconde avant de revérifier

        # 3. Récupérer les résultats
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        # 4. Formater la réponse
        rows = results["ResultSet"]["Rows"]
        if (
            len(rows) > 1
        ):  # S'il y a des résultats (la première ligne contient les noms de colonnes)
            data_row = rows[1]["Data"]
            return {
                "status": data_row[0]["VarCharValue"],
                "last_run": data_row[1][
                    "VarCharValue"
                ],  # Tu devras peut-être formater cette date
                "count": int(data_row[2]["VarCharValue"]),
            }
        else:
            return {"status": "UNKNOWN", "last_run": "N/A", "count": 0}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/jobs", tags=["Data"])
def get_jobs(limit: int = 200, x_api_key: str = Header(None)):
    """
    Récupère les offres d'emploi scorées depuis la table de sortie dbt (Gold).
    Requiert une clé API interne pour l'authentification.
    """
    # Sécurité : Vérification de la clé API
    expected_key = os.getenv("INTERNAL_API_KEY")
    if not expected_key or x_api_key != expected_key:
        raise HTTPException(
            status_code=403, detail="Accès refusé : Clé API invalide ou manquante."
        )

    if not S3_STAGING:
        raise HTTPException(
            status_code=500, detail="Configuration S3 Staging manquante."
        )

    # Limitation préventive pour éviter les coûts de requête excessifs
    query_limit = min(limit, 1000)

    try:
        # Connexion au moteur de requête Athena
        conn = connect(
            s3_staging_dir=S3_STAGING,
            region_name=REGION,
            schema_name=DB,
            cursor_class=DictCursor,
        )
        cursor = conn.cursor()

        # Requête sur la vue finale générée par dbt
        query = f"""
            SELECT * FROM api_jobs_ranking 
            ORDER BY matching_score DESC, published_at DESC 
            LIMIT {query_limit}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        return {"total_count": len(results), "database": DB, "jobs": results}

    except Exception as e:
        logger.error(f"Erreur lors de la requête Athena : {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des données.",
        ) from e


# --- ADAPTATEUR MANGUM ---
# Transforme l'application FastAPI en un handler compatible avec AWS Lambda
handler = Mangum(app)
