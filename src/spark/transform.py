from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- CONFIGURATION ---
# On utilise les variables d'environnement (GitHub Secrets)
RAW_PATH = "s3a://jobradar-raw-manuel-cloud"
SILVER_PATH = "s3a://jobradar-processed-manuel-cloud/silver_jobs"

def create_spark_session():
    # Configuration allégée : GitHub Actions fournira les clés via l'environnement
    return SparkSession.builder \
        .appName("JobRadar_Silver_Layer") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com") \
        .getOrCreate()

def stage_adzuna(spark):
    print("📥 Extraction Adzuna...")
    return spark.read.option("multiLine", "true").option("recursiveFileLookup", "true") \
        .json(f"{RAW_PATH}/adzuna/") \
        .select(F.explode("results").alias("job")).select("job.*") \
        .select(
            F.col("id").cast("string").alias("job_id"),
            F.col("title"),
            F.col("company.display_name").alias("company_name"),
            F.col("description"),
            F.col("location.display_name").alias("location"),
            F.col("created").alias("created_at"),
            F.lit("Adzuna").alias("source_name")
        )

def stage_france_travail(spark):
    print("📥 Extraction France Travail...")
    return spark.read.option("multiLine", "true").option("recursiveFileLookup", "true") \
        .json(f"{RAW_PATH}/france_travail/") \
        .select(F.explode("results").alias("job")).select("job.*") \
        .select(
            F.col("id").cast("string").alias("job_id"),
            F.col("intitule").alias("title"),
            F.coalesce(F.col("entreprise.nom"), F.lit("Non renseigné")).alias("company_name"),
            F.col("description"),
            F.col("lieuTravail.libelle").alias("location"),
            F.col("dateCreation").alias("created_at"),
            F.lit("France Travail").alias("source_name")
        )

def apply_silver_logic(df):
    print("🧹 Nettoyage Silver & Extraction des Techs...")
    
    # 1. Normalisation de base
    df = df.withColumn("title_clean", F.lower(F.col("title"))) \
           .withColumn("company_clean", F.lower(F.col("company_name"))) \
           .withColumn("location_clean", F.trim(F.regexp_replace(F.col("location"), r"\(?[0-9]{2,5}\)?|-", "")))

    # 2. Extraction des Skills (Regex) - Le résultat est un ARRAY
    tech_map = {
        "python": r"\bpython\b",
        "sql": r"\bsql\b",
        "dbt": r"\bdbt\b",
        "aws": r"\baws\b",
        "spark": r"\bspark\b",
        "docker": r"\bdocker\b",
        "kubernetes": r"\b(kubernetes|k8s)\b"
    }
    
    # On crée une colonne par tech, puis on les regroupe dans une liste
    tag_exprs = [F.when(F.lower(F.col("description")).rlike(regex) | F.col("title_clean").rlike(regex), F.lit(tech)).otherwise(None) 
                 for tech, regex in tech_map.items()]
    
    df = df.withColumn("extracted_skills", F.array_remove(F.array(*tag_exprs), None))

    # 3. Déduplication par Hashing
    # Formule : $$ID_{unique} = SHA256(Titre + Entreprise + Ville)$$
    df = df.withColumn("dedup_id", F.sha2(F.concat_ws("||", F.col("title_clean"), F.col("company_clean"), F.col("location_clean")), 256))
    
    return df.dropDuplicates(["dedup_id"]).withColumn("ingestion_date", F.current_date())

def run_pipeline():
    spark = create_spark_session()
    
    # Colonnes finales pour la Silver Layer
    silver_columns = [
        "job_id", "title", "company_name", "location_clean", "description",
        "created_at", "source_name", "extracted_skills", "ingestion_date"
    ]
    
    try:
        df_adz = stage_adzuna(spark)
        df_ft = stage_france_travail(spark)
        
        df_silver = apply_silver_logic(df_adz.unionByName(df_ft))
        
        print(f"🚀 Écriture de {df_silver.count()} offres dans la couche Silver...")
        
        df_silver.select(silver_columns).write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet(SILVER_PATH)
            
        print("✅ Silver Transformation terminée !")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    run_pipeline()