import os
import logging
import pandas as pd
from sentence_transformers import SentenceTransformer
from typing import List

# Configuration du logging pour une visibilité claire en console
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATION DU PROFIL CIBLE ---
# Cette description sert de référence pour le calcul de similarité cosinus.
# On y inclut : Stack technique, localisation, valeurs d'entreprise et soft skills.
MY_PROFILE_DESC = """
Nous recherchons un Data Engineer / Devops Junior passionné pour rejoindre notre équipe technique basée à Nantes, Pays de la Loire. 
Dans un environnement moderne de type Data Lakehouse, vous participerez à la construction de pipelines ETL/ELT robustes en utilisant Python, SQL, dbt, Spark (PySpark et ScalaSpark) et Pandas. Vous interviendrez sur notre infrastructure Cloud AWS (S3, Lambda, Glue, Athena) automatisée avec Terraform et Docker ou encore Gitlab CI/CD ou Github Actions.
En tant qu'entreprise engagée et certifiée B Corp et ESS, nous plaçons l'impact positif et l'éthique au cœur de nos projets Data for Good. Nous offrons un cadre de travail stimulant avec du mentorat pour les profils juniors et une forte culture de contribution à l'open source, l'environnement, le développement durable et le social. 
Le poste est ouvert en CDI avec un rythme de télétravail flexible (2 à 3 jours par semaine). Si vous aimez l'architecture Medallion et le Clean Code, rejoignez notre aventure à impact !
"""

# Chemin de sortie pour dbt seed (permet d'injecter le vecteur dans l'entrepôt de données)
OUTPUT_PATH = "transform/seeds/ideal_profile_vector.csv"

def generate_ideal_profile_seed() -> None:
    """
    Génère un embedding vectoriel à partir de la description du profil idéal 
    et le sauvegarde sous forme de fichier CSV pour dbt.
    """
    try:
        logger.info("🧠 Chargement du modèle de NLP (all-MiniLM-L6-v2)...")
        # all-MiniLM-L6-v2 est un modèle léger et performant pour les calculs de similarité de phrases
        model = SentenceTransformer("all-MiniLM-L6-v2")

        logger.info("⚡ Génération de l'embedding pour le profil idéal...")
        # Transformation du texte en liste de flottants
        vector: List[float] = model.encode(MY_PROFILE_DESC).tolist()

        # Construction du DataFrame
        # Le vecteur est stocké en String pour assurer la compatibilité lors du chargement dans Athena/S3
        df = pd.DataFrame(
            [
                {
                    "profile_id": "manuel_ideal_profile",
                    "description": MY_PROFILE_DESC.replace("\n", " ").strip(),
                    "ideal_vector": str(vector),
                }
            ]
        )

        # Création récursive du dossier de destination (seeds/)
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

        # Export en CSV sans l'index Pandas
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"✅ Fichier seed généré avec succès : {OUTPUT_PATH}")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération du profil : {str(e)}")
        raise

if __name__ == "__main__":
    generate_ideal_profile_seed()