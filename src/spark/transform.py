import logging
import os

import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

# --- CONFIGURATION INITIALE ---
load_dotenv(override=True)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("JobRadar_Spark")

# Paramètres S3 (Silver Layer)
RAW_PATH = "s3a://jobradar-raw-manuel-cloud"
SILVER_PATH = "s3a://jobradar-processed-manuel-cloud/silver_jobs"

# Cache pour le modèle NLP (Singleton pattern pour limiter l'empreinte mémoire)
model_cache = None


def create_spark_session() -> SparkSession:
    """
    Initialise une session Spark optimisée pour les environnements à ressources limitées.
    Incorpore les configurations S3A pour l'interaction avec AWS S3.
    """
    logger.info("🚀 Initialisation de la Spark Session (Cluster Local)...")
    return (
        SparkSession.builder.appName("JobRadar_Silver_Layer")
        .master("local[1]")  # Optimisé pour GitHub Runners (7GB RAM)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "16")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .getOrCreate()
    )


# --- ÉTAPE 1 : EXTRACTION & STAGING (BRONZE) ---


def stage_adzuna(spark: SparkSession) -> DataFrame:
    """Extraction et normalisation des données sources Adzuna."""
    logger.info("📥 Staging : Adzuna")
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{RAW_PATH}/adzuna/")
        .select(F.explode("results").alias("job"))
        .select("job.*")
        .select(
            F.col("id").cast("string").alias("job_id"),
            F.col("title"),
            F.col("company.display_name").alias("company_name"),
            F.col("description"),
            F.col("location.display_name").alias("location"),
            F.col("salary_min").cast("string").alias("salary_info"),
            F.col("created").alias("created_at"),
            F.col("redirect_url").alias("url"),
            F.lit("Adzuna").alias("source_name"),
        )
    )


def stage_france_travail(spark: SparkSession) -> DataFrame:
    """Extraction et normalisation des données sources France Travail."""
    logger.info("📥 Staging : France Travail")
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{RAW_PATH}/france_travail/")
        .select(F.explode("results").alias("job"))
        .select("job.*")
        .select(
            F.col("id").cast("string").alias("job_id"),
            F.col("intitule").alias("title"),
            F.coalesce(F.col("entreprise.nom"), F.lit("Non renseigné")).alias(
                "company_name"
            ),
            F.col("description"),
            F.col("lieuTravail.libelle").alias("location"),
            F.to_json(F.col("salaire")).alias("salary_info"),
            F.col("dateCreation").alias("created_at"),
            F.col("origineOffre.urlOrigine").alias("url"),
            F.lit("France Travail").alias("source_name"),
        )
    )


def stage_jsearch(spark: SparkSession) -> DataFrame:
    """Extraction et normalisation des données sources JSearch."""
    logger.info("📥 Staging : JSearch")
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{RAW_PATH}/jsearch/")
        .select(F.explode("results").alias("job"))
        .select("job.*")
        .select(
            F.col("job_id").cast("string"),
            F.col("job_title").alias("title"),
            F.col("employer_name").alias("company_name"),
            F.col("job_description").alias("description"),
            F.concat_ws(", ", F.col("job_city"), F.col("job_country")).alias(
                "location"
            ),
            F.lit(None).cast("string").alias("salary_info"),
            F.col("job_posted_at_datetime_utc").alias("created_at"),
            F.col("job_apply_link").alias("url"),
            F.lit("JSearch").alias("source_name"),
        )
    )


def stage_jooble(spark: SparkSession) -> DataFrame:
    """Extraction et normalisation des données sources Jooble."""
    logger.info("📥 Staging : Jooble")
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{RAW_PATH}/jooble/")
        .select(F.explode("results").alias("job"))
        .select("job.*")
        .select(
            F.col("id").cast("string").alias("job_id"),
            F.col("title"),
            F.col("company").alias("company_name"),
            F.col("snippet").alias("description"),
            F.col("location"),
            F.col("salary").alias("salary_info"),
            F.col("updated").alias("created_at"),
            F.col("link").alias("url"),
            F.lit("Jooble").alias("source_name"),
        )
    )


# --- ÉTAPE 2 : LOGIQUE MÉTIER & ENRICHISSEMENT (SILVER) ---


def apply_silver_logic(df: DataFrame) -> DataFrame:
    """
    Applique les transformations Silver : Nettoyage, Extraction de Features (NLP) et Déduplication.
    """

    @pandas_udf("array<float>")
    def vectorize_text_udf(texts: pd.Series) -> pd.Series:
        """Inférence NLP via SentenceTransformers pour le calcul d'embeddings sémantiques."""
        global model_cache
        import torch
        from sentence_transformers import SentenceTransformer

        torch.set_num_threads(1)
        if model_cache is None:
            model_cache = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        embeddings = model_cache.encode(texts.tolist(), show_progress_bar=False)
        return pd.Series(embeddings.tolist())

    logger.info(
        "🧹 Exécution de la logique Silver (Nettoyage & Feature Engineering)..."
    )

    # Nettoyage spécial Adzuna et Jooble
    logger.info("🧹 Nettoyage spécifique et Data Quality Scoring (Adzuna & Jooble)...")
    df = df.withColumn(
        "description", F.regexp_replace(F.col("description"), r"&nbsp;", " ")
    )
    # Retrait des points de suspension de fin pour Adzuna et Jooble
    # On cible les "..." qui traînent en fin de chaîne
    df = df.withColumn(
        "description",
        F.when(
            F.col("source_name").isin("Adzuna", "Jooble"),
            F.regexp_replace(F.col("description"), r"\.\.\.\s*$", ""),
        ).otherwise(F.col("description")),
    )

    # 1. Normalisation temporelle et nettoyage HTML
    df = df.withColumn("published_at", F.to_timestamp(F.col("created_at")))
    df = df.withColumn(
        "published_at", F.coalesce(F.col("published_at"), F.current_timestamp())
    )

    df = df.withColumn(
        "description_clean", F.regexp_replace(F.col("description"), "<[^>]*>", " ")
    )
    df = df.withColumn(
        "description_clean",
        F.regexp_replace(F.col("description_clean"), r"[\n\r\t\xa0]", " "),
    )

    # 2. Préparation du texte pour la recherche et vectorisation
    df = df.withColumn(
        "search_text",
        F.lower(F.concat_ws(" ", F.col("title"), F.col("description_clean"))),
    )
    df = df.withColumn(
        "location_clean",
        F.trim(F.regexp_replace(F.col("location"), r"\(?[0-9]{2,5}\)?|-", "")),
    )

    # 3. Extraction de l'expérience (Regex multiniveaux)
    years_regex = r"(\d+)\s*(?:ans?|ann[ée]es?)(?:\s*d['\s]exp[ée]rience|\s*de\s*pratique|\s*minimum)?"
    months_regex = r"(\d+)\s*mois"

    df = df.withColumn(
        "ext_years",
        F.regexp_extract(F.lower(F.col("description_clean")), years_regex, 1).cast(
            "float"
        ),
    )
    df = df.withColumn(
        "ext_months",
        F.regexp_extract(F.lower(F.col("description_clean")), months_regex, 1).cast(
            "float"
        )
        / 12.0,
    )
    df = df.withColumn(
        "exp_min_required", F.coalesce(F.col("ext_years"), F.col("ext_months"))
    )

    # Validation du range d'expérience
    df = df.withColumn(
        "exp_min_required",
        F.when(F.col("exp_min_required") <= 15, F.col("exp_min_required")).otherwise(
            None
        ),
    )
    df = df.drop("ext_years", "ext_months")

    # 4. Extraction des Hard Skills (Mapping Dictionnaire)
    tech_map = {
        "python": r"\bpython\b",
        "sql": r"\bsql\b",
        "pyspark": r"\bpyspark\b",
        "scala": r"\bscala\b",
        "spark": r"\bspark\b",
        "aws": r"\baws\b|amazon\s?web\s?services",
        "S3": r"\bs3\b|amazon\s?s3",
        "Lambda": r"\blambda\b",
        "ECR": r"\becr\b",
        "Athena": r"\bathena\b",
        "dbt": r"\bdbt\b",
        "snowflake": r"\bsnowflake\b",
        "airflow": r"\bairflow\b",
        "pandas": r"\bpandas\b",
        "streamlit": r"\bstreamlit\b",
        "fastapi": r"\bfastapi|fast\s?api\b",
        "git": r"\bgit\b",
        "github_actions": r"github\s?actions",
        "cicd": r"ci/cd|continuous\s?integration|continuous\s?delivery",
        "docker": r"\bdocker\b",
        "kubernetes": r"kubernetes|\bk8s\b",
        "terraform": r"\bterraform\b",
        "architecture_medaillon": r"architecture\s?m[ée]daillon|medallion\s?architecture",
        "etl": r"\betl\b|extract\s?transform\s?load",
        "elt": r"\belt\b|extract\s?load\s?transform",
    }

    tag_exprs = [
        F.when(F.col("search_text").rlike(regex), F.lit(tech)).otherwise(F.lit(None))
        for tech, regex in tech_map.items()
    ]
    df = df.withColumn("raw_skills_array", F.array(*tag_exprs))
    df = df.withColumn(
        "extracted_skills",
        F.array_distinct(F.filter(F.col("raw_skills_array"), lambda x: x.isNotNull())),
    )
    df = df.drop("raw_skills_array")

    # 5. Extraction du salaire (Optimisation numérique)
    salary_regex = r"(\d{2}\s?\d{3}|\d{2}k|\d{5})"
    df = df.withColumn(
        "salary_search",
        F.concat_ws(" | ", F.col("salary_info"), F.col("description_clean")),
    )
    df = df.withColumn(
        "salary_raw", F.regexp_extract(F.col("salary_search"), salary_regex, 1)
    )
    df = df.withColumn("salary_tmp", F.regexp_replace(F.col("salary_raw"), r"\s", ""))
    df = df.withColumn(
        "salary_min_numeric",
        F.when(
            F.col("salary_tmp").contains("k"),
            F.regexp_replace(F.col("salary_tmp"), "k", "").cast("int") * 1000,
        ).otherwise(F.col("salary_tmp").cast("int")),
    )

    # Filtre sur les salaires réalistes (Data)
    df = df.withColumn(
        "salary_min_numeric",
        F.when(
            (F.col("salary_min_numeric") >= 25000)
            & (F.col("salary_min_numeric") <= 180000),
            F.col("salary_min_numeric"),
        ).otherwise(None),
    )

    # 6. Classification métier & Ethique
    df = df.withColumn(
        "is_junior", F.col("search_text").rlike(r"junior|débutant|jeune diplômé")
    )
    df = df.withColumn(
        "is_senior", F.col("search_text").rlike(r"senior|expert|lead|confirmé")
    )
    df = df.withColumn(
        "is_red_flag",
        F.col("search_text").rlike(r"alternance|stage|support|technicien|helpdesk"),
    )

    ethical_regex = r"impact|green|environnement|transition|coopérative|scic|scop|ess"
    df = df.withColumn(
        "is_ethical",
        F.col("search_text").rlike(ethical_regex)
        | F.lower(F.col("company_name")).rlike(ethical_regex),
    )
    df = df.withColumn(
        "is_remote",
        F.col("search_text").rlike(
            r"télétravail|remote|home\s?office|full\s?remote|distanciel"
        ),
    )

    # Ajout du Data Quality Score (DQS)
    df = df.withColumn(
        "data_quality_score",
        F.when(F.col("source_name") == "France Travail", F.lit(1.0))
        .when(
            F.col("source_name").isin("Adzuna", "Jooble"), F.lit(0.6)
        )  # Les deux sont pénalisés à 60%
        .when(F.col("source_name") == "JSearch", F.lit(1.0))
        .otherwise(F.lit(0.8)),
    )

    # 7. Déduplication par empreinte (Window Function)
    df = df.withColumn(
        "dedup_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.lower(F.col("title")),
                F.lower(F.col("company_name")),
                F.col("location_clean"),
            ),
            256,
        ),
    )
    window_spec = Window.partitionBy("dedup_id").orderBy(F.col("published_at").desc())

    # 8. Vectorisation NLP finale
    df = df.withColumn(
        "combined_text_for_vector",
        F.substring(F.concat_ws(" ", F.col("title"), F.col("description")), 1, 1000),
    )
    logger.info("🧠 Inférence NLP : Calcul des embeddings sémantiques...")
    df = df.withColumn(
        "description_vector", vectorize_text_udf(F.col("combined_text_for_vector"))
    )
    df = df.drop("combined_text_for_vector")

    return (
        df.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .withColumn("ingestion_date", F.current_date())
        .drop(
            "rn",
            "search_text",
            "salary_search",
            "salary_raw",
            "salary_tmp",
            "description_clean",
            "dedup_id",
        )
    )


# --- ÉTAPE 3 : MAIN ORCHESTRATION ---


def run_pipeline() -> None:
    """Exécution complète du pipeline de transformation Silver."""
    spark = create_spark_session()

    silver_columns = [
        "job_id",
        "title",
        "company_name",
        "location_clean",
        "description",
        "url",
        "published_at",
        "source_name",
        "data_quality_score",
        "extracted_skills",
        "salary_min_numeric",
        "exp_min_required",
        "is_junior",
        "is_senior",
        "is_red_flag",
        "is_ethical",
        "is_remote",
        "description_vector",
        "ingestion_date",
    ]

    try:
        # Ingestion par source
        df_adz = stage_adzuna(spark)
        df_ft = stage_france_travail(spark)
        df_js = stage_jsearch(spark)
        df_jb = stage_jooble(spark)

        logger.info(
            f"✅ Staging terminé : {df_adz.count()} Adzuna, {df_ft.count()} FT, {df_js.count()} JSearch, {df_jb.count()} Jooble."
        )

        # Union et Transformation globale
        raw_df = df_adz.unionByName(df_ft).unionByName(df_js).unionByName(df_jb)
        df_silver = apply_silver_logic(raw_df)

        # Écriture partitionnée au format Parquet
        logger.info(
            f"🚀 Écriture de {df_silver.count()} offres uniques vers la couche Silver..."
        )
        df_silver.select(silver_columns).write.mode("overwrite").partitionBy(
            "ingestion_date"
        ).parquet(SILVER_PATH)

        # Mise à jour du catalogue Athena
        logger.info("🔧 Synchronisation du catalogue Athena (MSCK REPAIR)...")
        athena = boto3.client("athena", region_name="eu-west-3")
        athena.start_query_execution(
            QueryString="MSCK REPAIR TABLE jobradar_db.silver_jobs",
            QueryExecutionContext={"Database": "jobradar_db"},
            ResultConfiguration={
                "OutputLocation": "s3://jobradar-athena-results-manuel-cloud/athena_temp/"
            },
        )
        logger.info("✅ Pipeline Silver terminé avec succès !")

    except Exception as e:
        logger.error(f"❌ Échec critique du pipeline : {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline()
