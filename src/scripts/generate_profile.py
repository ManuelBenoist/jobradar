import pandas as pd
from sentence_transformers import SentenceTransformer
import os

# --- CONFIGURATION DU PROFIL ---
# Description détaillée du profil idéal pour générer un vecteur d'embedding
MY_PROFILE_DESC = """
Nous recherchons un Data Engineer / Devops Junior passionné pour rejoindre notre équipe technique basée à Nantes, Pays de la Loire. 
Dans un environnement moderne de type Data Lakehouse, vous participerez à la construction de pipelines ETL/ELT robustes en utilisant Python, SQL, dbt, Spark (PySpark et ScalaSpark) et Pandas. Vous interviendrez sur notre infrastructure Cloud AWS (S3, Lambda, Glue, Athena) automatisée avec Terraform et Docker ou encore Gitlab CI/CD ou Github Actions.
En tant qu'entreprise engagée et certifiée B Corp et ESS, nous plaçons l'impact positif et l'éthique au cœur de nos projets Data for Good. Nous offrons un cadre de travail stimulant avec du mentorat pour les profils juniors et une forte culture de contribution à l'open source, l'environnement, le développement durable et le social. 
Le poste est ouvert en CDI avec un rythme de télétravail flexible (2 à 3 jours par semaine). Si vous aimez l'architecture Medallion et le Clean Code, rejoignez notre aventure à impact !
"""

OUTPUT_PATH = "transform/seeds/ideal_profile_vector.csv"


def generate():
    print("🧠 Génération du vecteur profil idéal...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Génération de l'embedding
    vector = model.encode(MY_PROFILE_DESC).tolist()

    # Création d'un DataFrame pour dbt seed
    # On stocke le vecteur sous forme de chaîne JSON pour qu'Athena puisse le lire
    df = pd.DataFrame(
        [
            {
                "profile_id": "manuel_ideal_profile",
                "description": MY_PROFILE_DESC.replace("\n", " ").strip(),
                "ideal_vector": str(vector),
            }
        ]
    )

    # Création du dossier seeds s'il n'existe pas
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Sauvegarde en CSV
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Fichier seed généré avec succès : {OUTPUT_PATH}")


if __name__ == "__main__":
    generate()
