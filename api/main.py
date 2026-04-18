import os
from fastapi import FastAPI, HTTPException
from pyathena import connect #pyathena est construit sur boto3 
from pyathena.cursor import DictCursor
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="JobRadar API",
    description="API pour interagir avec les données de JobRadar via AWS Athena",
    version="1.0.0"
)

# Configuration Athena
REGION = os.getenv("AWS_REGION","eu-west-3")
DB = os.getenv("ATHENA_DATABASE","jobradar_db")
S3_STAGING = os.getenv("ATHENA_S3_STAGING_DIR")

# 1. Route de Santé (Healthcheck)
@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "jobradar-api"}

# 2. Route Principale : Récupération des jobs
@app.get("/jobs")
def get_jobs():
    if not S3_STAGING:
        raise HTTPException(status_code=500, detail="S3 Staging Dir non configuré")
    
    try:
        # Connexion à Athena avec un DictCursor pour avoir des JSON propres
        conn = connect(
            s3_staging_dir=S3_STAGING,
            region_name=REGION,
            schema_name=DB,
            cursor_class=DictCursor
        )
        
        cursor = conn.cursor()
        
        # On interroge la VUE finale créée par dbt
        query = "SELECT * FROM api_jobs_ranking"
        cursor.execute(query)
        
        results = cursor.fetchall()
        
        return {
            "total_count": len(results),
            "jobs": results
        }
        
    except Exception as e:
        print(f"❌ Erreur Athena : {e}")
        raise HTTPException(status_code=500, detail=str(e))