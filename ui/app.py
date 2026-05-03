from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import requests
import streamlit as st

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="JobRadar Live | Data & DevOps",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def fetch_pipeline_status():
    """Appelle l'API pour connaître l'état de santé du système."""
    API_URL = st.secrets["API_URL"]
    try:
        response = requests.get(f"{API_URL}/health/pipeline", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# --- STYLE CSS PERSONNALISÉ ---
st.markdown(
    """
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px; border-radius: 10px; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { font-weight: 600; color: #4b5563; }
    .stTabs [aria-selected="true"] { color: #2563eb !important; border-color: #2563eb !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


# --- CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=3600)
def fetch_job_data(limit: int) -> Optional[Dict[str, Any]]:
    """Récupère les données depuis l'API JobRadar avec gestion d'erreurs."""
    try:
        API_URL = st.secrets["API_URL"]
        API_KEY = st.secrets["INTERNAL_API_KEY"]
        headers = {"X-API-Key": API_KEY}
        params = {"limit": limit}

        response = requests.get(API_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"📡 Erreur de connexion à l'API : {e}")
        return None


# --- INITIALISATION SESSION STATE ---
if "max_limit_fetched" not in st.session_state:
    st.session_state.max_limit_fetched = 0

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Configuration")
    job_limit = st.slider("Nombre d'offres à charger", 50, 1000, 200, step=50)

    if st.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()
        st.session_state.max_limit_fetched = 0
        st.rerun()

# --- LOGIQUE DE CHARGEMENT INTELLIGENTE ---
# On ne refait un scan que si on demande plus que ce qu'on a déjà en cache
needs_new_scan = job_limit > st.session_state.max_limit_fetched

if needs_new_scan:
    with st.spinner(f"📡 Scan profond en cours ({job_limit} offres)..."):
        data_raw = fetch_job_data(limit=job_limit)
        if data_raw:
            st.session_state.max_limit_fetched = job_limit
            st.toast(f"✅ {len(data_raw['jobs'])} offres récupérées !")
else:
    data_raw = fetch_job_data(limit=st.session_state.max_limit_fetched)

# --- TRONCATURE LOCALE (Slicing) ---
data = None
if data_raw and "jobs" in data_raw:
    data = data_raw.copy()
    data["jobs"] = data_raw["jobs"][:job_limit]

# --- HEADER ---
st.title("📡 JobRadar Live")
st.caption(
    "Veille automatisée de recherche d'emplois Data & DevOps, personnalisé pour Manuel B.| Pipeline ELT Serverless sur AWS"
)

tab_radar, tab_tech = st.tabs(["🎯 Radar des offres", "🏗️ Architecture du projet"])

# --- ONGLET 1 : LE RADAR ---
with tab_radar:
    if data and "jobs" in data and len(data["jobs"]) > 0:
        df = pd.DataFrame(data["jobs"])

        # Nettoyage et conversion numérique
        for col in ["matching_score", "semantic_score", "rules_score", "salary_min"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # Conversion des dates (ISO8601)
        df["published_at"] = pd.to_datetime(
            df["published_at"], format="ISO8601", errors="coerce"
        )
        df["ingestion_date"] = pd.to_datetime(
            df["ingestion_date"], format="ISO8601", errors="coerce"
        )
        df["original_url"] = df["original_url"].fillna("")

        # Nettoyage des compétences
        if "skills" in df.columns:
            df["skills"] = df["skills"].str.replace(r"[\[\]']", "", regex=True)

        # LOGIQUE VISUELLE : Pastilles de couleur
        def get_score_visual(score):
            if score >= 80:
                return f"🟢 {score}%"
            if score >= 60:
                return f"🟡 {score}%"
            return f"🔴 {score}%"

        # Application des visuels sur les 3 types de scores (Ta demande)
        df["matching_visual"] = df["matching_score"].apply(get_score_visual)
        df["semantic_visual"] = df["semantic_score"].apply(get_score_visual)
        df["rules_visual"] = df["rules_score"].apply(get_score_visual)

        df["salary_visual"] = df["salary_min"].apply(
            lambda x: f"{int(x):,} €".replace(",", " ") if x > 0 else "N/A"
        )

        # Calcul de fraîcheur
        limit_date = datetime.now() - timedelta(hours=48)
        new_jobs_count = len(df[df["published_at"] >= limit_date])

        # 1. KPI CARDS
        health = fetch_pipeline_status()
        # Appel du statut de la pipeline et affichage du bandeau juste sous le titre
        if health:
            if health.get("status") == "SUCCESS":
                st.caption(
                    f"🟢 Pipeline saine : dernière synchronisation avec AWS Athena le {health.get('last_run')}, {health.get('count')} offres ingérées au total."
                )
            else:
                st.caption(
                    "🔴 **Alerte Pipeline** | Échec détecté sur le dernier run. Les données peuvent être obsolètes."
                )

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Offres analysées", len(df))
        k2.metric("Nouveautés (<48h)", f"{new_jobs_count}")
        k3.metric("Matching Moyen", f"{int(df['matching_score'].mean())}%")

        valid_salaries = df[df["salary_min"] > 0]["salary_min"]
        avg_salary_text = (
            f"{int(valid_salaries.mean()):,} €".replace(",", " ")
            if not valid_salaries.empty
            else "N/A"
        )
        k4.metric("Salaire Moyen", avg_salary_text)

        st.divider()

        # 2. FILTRES
        col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
        search_query = col_f1.text_input(
            "🔍 Recherche rapide", placeholder="ex: AWS, Nantes, Analyste..."
        )
        min_score = col_f2.slider("Score matching min.", 0, 100, 30)
        show_only_fresh = col_f3.toggle("✨ Uniquement les nouveautés")

        filtered_df = df[df["matching_score"] >= min_score]
        if show_only_fresh:
            filtered_df = filtered_df[filtered_df["published_at"] >= limit_date]

        if search_query:
            mask = filtered_df.apply(
                lambda row: (
                    row.astype(str).str.contains(search_query, case=False).any()
                ),
                axis=1,
            )
            filtered_df = filtered_df[mask]

        # 3. AFFICHAGE DU TABLEAU (Config complète conservée)
        st.dataframe(
            filtered_df,
            column_config={
                "matching_visual": st.column_config.TextColumn(
                    "Global", help="Moyenne pondérée", width="small"
                ),
                "semantic_visual": st.column_config.TextColumn(
                    "🤖 IA", help="Similarité sémantique (NLP)", width="small"
                ),
                "rules_visual": st.column_config.TextColumn(
                    "📋 Règles", help="Critères métier", width="small"
                ),
                "published_at": st.column_config.DatetimeColumn(
                    "Publié", format="D MMM, HH:mm"
                ),
                "title": st.column_config.TextColumn("Poste", width="large"),
                "company_name": "Entreprise",
                "positive_labels": st.column_config.TextColumn(
                    "Points Positifs", width="medium"
                ),
                "negative_labels": st.column_config.TextColumn(
                    "Points Négatifs", width="medium"
                ),
                "description": st.column_config.TextColumn(
                    "Description", width="large"
                ),
                "salary_visual": st.column_config.TextColumn(
                    "Salaire Min", width="small"
                ),
                "skills": st.column_config.TextColumn("Compétences", width="medium"),
                "city": "Ville",
                "platform": "Source",
                "original_url": st.column_config.LinkColumn("Lien", display_text="🔗"),
            },
            column_order=(
                "published_at",
                "matching_visual",
                "semantic_visual",
                "rules_visual",
                "title",
                "positive_labels",
                "negative_labels",
                "company_name",
                "description",
                "salary_visual",
                "skills",
                "city",
                "platform",
                "original_url",
            ),
            width="stretch",
            hide_index=True,
        )

    else:
        st.warning(
            "📡 Aucune donnée disponible. La Lambda est peut-être en cours de réveil."
        )

# --- ONGLET 2 : ARCHITECTURE & TECH ---
with tab_tech:
    st.header("Spécifications Techniques")
    st.markdown(
        """**État de la pipeline :** [![JobRadar CI](https://github.com/ManuelBenoist/jobradar/actions/workflows/data_pipeline.yml/badge.svg)](https://github.com/ManuelBenoist/jobradar/actions/workflows/data_pipeline.yml)"""
    )

    col_a, col_b = st.columns(2)
    with col_a:
        with st.expander("🏗️ Pipeline Data (ELT)", expanded=True):
            st.markdown("""
            - **Ingestion :** Lambdas Python multi-sources (Adzuna, FT, JSearch, Jooble).
            - **Storage :** S3 (Bronze/Silver/Gold) en format Parquet.
            - **Transformation :** PySpark (Déduplication & NLP) + dbt Core (Scoring SQL).
            """)

    with col_b:
        with st.expander("☁️ Cloud & DevOps", expanded=True):
            st.markdown("""
            - **API :** FastAPI sur AWS Lambda (Serverless via Mangum).
            - **IaC :** Terraform pour le déploiement reproductible.
            - **Sécurité :** IAM Roles, Secrets Manager & CORS restrictif.
            """)

    st.divider()
    st.subheader("Extrait Logique : Scoring dbt (Gold Layer)")
    st.code(
        """
-- Calcul du score final pondéré (extrait de fct_jobs.sql)
SELECT 
    *,
    -- Pondération 50% Règles / 50% NLP
    ROUND((
        (CASE WHEN rules_score > 100 THEN 100 ELSE rules_score END) * 0.5 + 
        (CASE WHEN semantic_score > 100 THEN 100 ELSE semantic_score END) * 0.5
    )) AS matching_score
FROM {{ ref('final_calculation') }}
        """,
        language="sql",
    )
