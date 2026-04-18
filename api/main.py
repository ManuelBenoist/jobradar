import os
from fastapi import FastAPI, HTTPException
from pyathena import connect
from pyathena.cursor import DictCursor
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="JobRadar API",
    description="API pour interagir avec les données de JobRadar via AWS Athena",
    version="1.0.0",
)

# Configuration Athena
REGION = os.getenv("AWS_REGION", "eu-west-3")
DB = os.getenv("ATHENA_DATABASE", "jobradar_db")
S3_STAGING = os.getenv("ATHENA_S3_STAGING_DIR")

# --- 1. Route d'Accueil (pour éviter le Not Found) ---
@app.get("/")
def read_root():
    return {
        "message": "Bienvenue sur l'API JobRadar",
        "endpoints": {
            "health": "/health",
            "jobs": "/jobs",
            "documentation": "/docs"
        }
    }

# --- 2. Route de Santé ---
@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "jobradar-api"}

# --- 3. Route Principale : Récupération des jobs ---
@app.get("/jobs")
def get_jobs(limit: int = 10): # Ajout d'une limite par défaut
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
        
        # On utilise une F-string pour la limite 
        query = f"SELECT * FROM api_jobs_ranking LIMIT {limit}"
        cursor.execute(query)
        results = cursor.fetchall()

        return {
            "total_count": len(results),
            "database": DB,
            "jobs": results
        }

    except Exception as e:
        print(f"❌ Erreur Athena : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur lors de la requête Athena : {str(e)}")