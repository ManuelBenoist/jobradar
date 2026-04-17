import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- CONFIGURATION ---
load_dotenv()
RAW_PATH = "s3a://jobradar-raw-manuel-cloud/"
SILVER_PATH = "s3a://jobradar-processed-manuel-cloud/silver_jobs/"

def create_spark_session():
    return SparkSession.builder \
        .appName("JobRadar_Final_Pipeline") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def stage_adzuna(spark):
    print("📥 Stage Adzuna...")
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
    print("📥 Stage France Travail...")
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

def apply_logic(df):
    print("🧹 Nettoyage et Scoring...")
    df = df.withColumn("location_clean", F.trim(F.regexp_replace(F.col("location"), r"\(?[0-9]{2,5}\)?|-", "")))
    df = df.withColumn("t_lower", F.lower(F.col("title")))
    df = df.withColumn("is_python", F.when(F.col("t_lower").rlike("python"), 1).otherwise(0))
    df = df.withColumn("is_data", F.when(F.col("t_lower").rlike("data|ingénieur"), 1).otherwise(0))
    df = df.withColumn("matching_score", F.lit(40) + (F.col("is_python") * 20) + (F.col("is_data") * 20))
    return df.dropDuplicates(["title", "company_name"]).withColumn("ingestion_date", F.current_date())

def run_pipeline():
    spark = create_spark_session()
    
    final_columns = [
        "job_id", "title", "company_name", "location_clean", 
        "created_at", "source_name", "matching_score", "ingestion_date"
    ]
    
    try:
        df_adz = stage_adzuna(spark)
        df_ft = stage_france_travail(spark)
        
        df_unified = df_adz.unionByName(df_ft)
        df_final = apply_logic(df_unified)
        
        print(f"🔥 Écriture de {df_final.count()} offres vers {SILVER_PATH}...")
        
        # L'écriture standard, sans complexité
        df_final.select(final_columns).write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet(SILVER_PATH)
            
        print("🚀 Pipeline JobRadar terminé avec succès !")
        
    except Exception as e:
        print(f"❌ Erreur dans le pipeline : {e}")

if __name__ == "__main__":
    run_pipeline()