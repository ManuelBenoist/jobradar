import os
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware  # Pour le dashboard
from pyathena import connect
from pyathena.cursor import DictCursor
from dotenv import load_dotenv
from mangum import Mangum

load_dotenv()

app = FastAPI(
    title="JobRadar API",
    description="API pour interagir avec les données de JobRadar via AWS Athena (Serverless Edition)",
    version="1.1.0",
)

# --- CONFIGURATION CORS (Indispensable pour Streamlit) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],  # On autorisera tout le monde au début pour faciliter les tests
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration Athena
REGION = os.getenv("AWS_REGION", "eu-west-3")
DB = os.getenv("ATHENA_DATABASE", "jobradar_db")
S3_STAGING = os.getenv("ATHENA_S3_STAGING_DIR")


@app.get("/")
def read_root():
    return {
        "message": "Bienvenue sur l'API JobRadar (Running on AWS Lambda)",
        "endpoints": {"health": "/health", "jobs": "/jobs", "documentation": "/docs"},
    }


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "jobradar-api-serverless"}


@app.get("/jobs")
def get_jobs(limit: int = 200, x_api_key: str = Header(None)):
    if limit > 1000:
        limit = 1000
    # Récupération de la clé attendue (configurée dans tes variables Lambda)
    expected_key = os.getenv("INTERNAL_API_KEY")

    if not expected_key or x_api_key != expected_key:
        raise HTTPException(
            status_code=403, detail="Accès refusé : Clé API invalide ou manquante."
        )
    if not S3_STAGING:
        raise HTTPException(status_code=500, detail="S3 Staging Dir non configuré")

    try:
        conn = connect(
            s3_staging_dir=S3_STAGING,
            region_name=REGION,
            schema_name=DB,
            cursor_class=DictCursor,
        )
        cursor = conn.cursor()

        query = f"""
            SELECT * FROM api_jobs_ranking 
            ORDER BY matching_score DESC, published_at DESC 
            LIMIT {limit}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        return {"total_count": len(results), "database": DB, "jobs": results}

    except Exception as e:
        print(f"❌ Erreur Athena : {e}")
        raise HTTPException(
            status_code=500, detail=f"Erreur lors de la requête Athena : {str(e)}"
        )


# --- 2. LE HANDLER (Le pont entre Lambda et FastAPI) ---
handler = Mangum(app)
