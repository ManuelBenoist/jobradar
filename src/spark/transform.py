import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# import os
# from dotenv import load_dotenv
# load_dotenv()
# --- CONFIGURATION ---
RAW_PATH = "s3a://jobradar-raw-manuel-cloud"
SILVER_PATH = "s3a://jobradar-processed-manuel-cloud/silver_jobs"


def create_spark_session():
    return (
        SparkSession.builder.appName("JobRadar_Silver_Layer")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
        .getOrCreate()
    )


# --- ETAPE 1 : BRONZE / STAGING ---
def stage_adzuna(spark):
    print("📥 Extraction Adzuna...")
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


def stage_france_travail(spark):
    print("📥 Extraction France Travail...")
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

def stage_jsearch(spark):
    print("📥 Extraction JSearch (Score: 1.0)...")
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
            F.col("job_description").alias("description"), # Full text !
            F.concat_ws(", ", F.col("job_city"), F.col("job_country")).alias("location"),
            F.lit(None).cast("string").alias("salary_info"), # JSearch a peu de salaires exploitables en FR
            F.col("job_posted_at_datetime_utc").alias("created_at"),
            F.col("job_apply_link").alias("url"),
            F.lit("JSearch").alias("source_name"),
        )
    )

def stage_jooble(spark):
    print("📥 Extraction Jooble (Score: 1.0)...")
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
            F.col("snippet").alias("description"), # Jooble renvoie un snippet long ou HTML
            F.col("location"),
            F.col("salary").alias("salary_info"),
            F.col("updated").alias("created_at"),
            F.col("link").alias("url"),
            F.lit("Jooble").alias("source_name"),
        )
    )

def apply_silver_logic(df):
    print("🧹 Nettoyage Silver Expert & Feature Engineering...")
    # --- Normalisation des dates de publication ---
    # On transforme le string ISO en Timestamp Spark
    df = df.withColumn("published_at", F.to_timestamp(F.col("created_at")))

    # Sécurité : Si published_at est nul (erreur API), on met la date du jour
    df = df.withColumn(
        "published_at", F.coalesce(F.col("published_at"), F.current_timestamp())
    )
    # -------------------------------------------------------
    # 1. Nettoyage HTML + Suppression des caractères invisibles (\xa0, retours à la ligne, etc.)
    df = df.withColumn(
        "description_clean", F.regexp_replace(F.col("description"), "<[^>]*>", " ")
    )
    df = df.withColumn(
        "description_clean",
        F.regexp_replace(F.col("description_clean"), r"[\n\r\t\xa0]", " "),
    )

    # 2. Création des colonnes de recherche
    df = df.withColumn(
        "search_text",
        F.lower(F.concat_ws(" ", F.col("title"), F.col("description_clean"))),
    )
    df = df.withColumn(
        "location_clean",
        F.trim(F.regexp_replace(F.col("location"), r"\(?[0-9]{2,5}\)?|-", "")),
    )

    # 3. EXTRACTION DES SKILLS
    tech_map = {
        # Langages
        "python": r"\bpython\b",
        "sql": r"\bsql\b",
        "pyspark": r"\bpyspark\b",
        "scala": r"\bscala\b",
        "spark": r"\bspark\b",
        # Cloud AWS
        "aws": r"\baws\b|amazon\s?web\s?services",
        "S3": r"\bs3\b|amazon\s?s3",
        "Lambda": r"\blambda\b",
        "ECR": r"\becr\b",
        "Athena": r"\bathena\b",
        # Data Stack & Tools
        "dbt": r"\bdbt\b",
        "snowflake": r"\bsnowflake\b",
        "airflow": r"\bairflow\b",
        "pandas": r"\bpandas\b",
        "streamlit": r"\bstreamlit\b",
        "fastapi": r"\bfastapi|fast\s?api\b",
        # DevOps & CI/CD
        "git": r"\bgit\b",
        "github_actions": r"github\s?actions",
        "cicd": r"ci/cd|continuous\s?integration|continuous\s?delivery",
        "docker": r"\bdocker\b",
        "kubernetes": r"kubernetes|\bk8s\b",
        "terraform": r"\bterraform\b",
        # Concepts (Bonus culture)
        "architecture_medaillon": r"architecture\s?m[ée]daillon|medallion\s?architecture",
        "etl": r"\betl\b|extract\s?transform\s?load",
        "elt": r"\belt\b|extract\s?load\s?transform",
    }

    tag_exprs = [
        F.when(F.col("search_text").rlike(regex), F.lit(tech)).otherwise(F.lit(None))
        for tech, regex in tech_map.items()
    ]

    # On crée le tableau avec les nulls
    df = df.withColumn("raw_skills_array", F.array(*tag_exprs))

    # On filtre les nulls, puis on déduplique
    df = df.withColumn(
        "extracted_skills",
        F.array_distinct(F.filter(F.col("raw_skills_array"), lambda x: x.isNotNull())),
    )

    # On nettoie la colonne temporaire
    df = df.drop("raw_skills_array")

    # 4. EXTRACTION DU SALAIRE
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

    df = df.withColumn(
        "salary_min_numeric",
        F.when(
            (F.col("salary_min_numeric") >= 25000)
            & (F.col("salary_min_numeric") <= 180000),
            F.col("salary_min_numeric"),
        ).otherwise(None),
    )

    # 5. FLAGS (Junior, Senior, etc.)
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
    # Détection du Télétravail (Regex robuste)
    remote_regex = r"télétravail|remote|home\s?office|full\s?remote|distanciel"
    df = df.withColumn("is_remote", F.col("search_text").rlike(remote_regex))

    # 6. DÉDUPLICATION (Window Function)
    # On prépare le hash d'ID unique pour la partition
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

    # 7. NETTOYAGE FINAL ET RETOUR
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
            "salary_clean",
            "dedup_id",
        )
    )


# --- ETAPE 3 : PIPELINE ---
def run_pipeline():
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
        "extracted_skills",
        "salary_min_numeric",
        "is_junior",
        "is_senior",
        "is_red_flag",
        "is_ethical",
        "is_remote",
        "ingestion_date",
    ]

    try:
        df_adz = stage_adzuna(spark)
        df_ft = stage_france_travail(spark)
        df_js = stage_jsearch(spark)
        df_jb = stage_jooble(spark)

        print(
            f"✅ Staging terminé : {df_adz.count()} Adzuna, {df_ft.count()} France Travail, {df_js.count()} JSearch, {df_jb.count()} Jooble."
        )

        df_silver = apply_silver_logic(df_adz.unionByName(df_ft).unionByName(df_js).unionByName(df_jb))

        print(f"🚀 Écriture de {df_silver.count()} offres uniques vers S3...")

        df_silver.select(silver_columns).write.mode("overwrite").partitionBy(
            "ingestion_date"
        ).parquet(SILVER_PATH)

        print("🔧 Mise à jour du catalogue Athena (MSCK REPAIR)...")
        athena = boto3.client("athena", region_name="eu-west-3")

        # force Athena à scanner S3 pour trouver les nouvelles partitions
        athena.start_query_execution(
            QueryString="MSCK REPAIR TABLE jobradar_db.silver_jobs",
            QueryExecutionContext={"Database": "jobradar_db"},
            ResultConfiguration={
                "OutputLocation": "s3://jobradar-athena-results-manuel-cloud/athena_temp/"
            },
        )
        print("✅ Silver Transformation terminée avec succès !")

    except Exception as e:
        print(f"❌ Erreur : {e}")


if __name__ == "__main__":
    run_pipeline()
