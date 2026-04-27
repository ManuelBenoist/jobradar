import pandas as pd
from sentence_transformers import SentenceTransformer
import os

# --- CONFIGURATION DU PROFIL ---
# Description détaillée du profil idéal pour générer un vecteur d'embedding
MY_PROFILE_DESC = """
Data Engineer et DevOps Junior passionné par l'écosystème Data.
Compétences : 
- Python, SQL, DBT, Spark (PySpark ou ScalaSpark).
- Infrastructure et Cloud : AWS (S3, Lambda, Glue, Athena), Terraform, Docker, CI/CD, GitHub Actions. 
- Méthodologies : Architecture Medallion, ETL, ELT, Data Lakehouse, Analyse de données avec Pandas.
Sensible aux projets à impact positif (Data for Good), aux enjeux environnementaux, à l'éthique et à l'économie sociale et solidaire (ESS). 
Recherche un poste de cadre en CDI, télétravail partiel accepté (par exemple 2 jours par semaine).
Localisation : Nantes ou Pays de la Loire.
"""

OUTPUT_PATH = "transform/seeds/ideal_profile_vector.csv"

def generate():
    print("🧠 Génération du vecteur profil idéal...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Génération de l'embedding
    vector = model.encode(MY_PROFILE_DESC).tolist()
    
    # Création d'un DataFrame pour dbt seed
    # On stocke le vecteur sous forme de chaîne JSON pour qu'Athena puisse le lire
    df = pd.DataFrame([{
        "profile_id": "manuel_ideal_profile",
        "description": MY_PROFILE_DESC.replace("\n", " ").strip(),
        "ideal_vector": str(vector)
    }])
    
    # Création du dossier seeds s'il n'existe pas
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Sauvegarde en CSV
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Fichier seed généré avec succès : {OUTPUT_PATH}")

if __name__ == "__main__":
    generate()