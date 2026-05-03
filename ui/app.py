import streamlit as st
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="JobRadar Live | Data & DevOps",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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


# --- CHARGEMENT DES DONNÉES (API) ---
@st.cache_data(ttl=3600)
def fetch_job_data(limit: int) -> Optional[Dict[str, Any]]:
    """
    Interroge l'API JobRadar pour récupérer les offres scorées.
    Utilise les secrets Streamlit pour l'authentification.
    """
    API_URL = st.secrets["API_URL"]
    API_KEY = st.secrets["INTERNAL_API_KEY"]

    headers = {"X-API-Key": API_KEY}
    params = {"limit": limit}

    try:
        response = requests.get(API_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"⚠️ Erreur de connexion à l'API : {str(e)}")
        return None


# --- GESTION DE LA SESSION & CACHE INTELLIGENT ---
if "max_limit_fetched" not in st.session_state:
    st.session_state.max_limit_fetched = 0

# --- SIDEBAR & CONTRÔLES ---
with st.sidebar:
    st.header("⚙️ Configuration")
    job_limit = st.slider("Portée du scan (offres)", 50, 1000, 200, step=50)

    if st.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()
        st.session_state.max_limit_fetched = 0
        st.rerun()

# Logique de chargement optimisée (évite les appels API inutiles)
needs_new_scan = job_limit > st.session_state.max_limit_fetched

if needs_new_scan:
    with st.spinner(f"📡 Scan profond en cours ({job_limit} offres)..."):
        data_raw = fetch_job_data(limit=job_limit)
        if data_raw:
            st.session_state.max_limit_fetched = job_limit
            st.toast(f"✅ {len(data_raw['jobs'])} offres récupérées !")
else:
    data_raw = fetch_job_data(limit=st.session_state.max_limit_fetched)

# Slicing local pour respecter le slider sans re-requêter
data = None
if data_raw and "jobs" in data_raw:
    data = data_raw.copy()
    data["jobs"] = data_raw["jobs"][:job_limit]

# --- HEADER PRINCIPAL ---
st.title("📡 JobRadar Live")
st.caption(
    "Système autonome de veille Data & DevOps | Architecture ELT Serverless sur AWS"
)

tab_radar, tab_tech = st.tabs(["🎯 Radar des offres", "🏗️ Architecture Projet"])

# --- ONGLET 🎯 : LE RADAR ---
with tab_radar:
    if data and "jobs" in data and len(data["jobs"]) > 0:
        df = pd.DataFrame(data["jobs"])

        # Conversion et nettoyage des types
        cols_to_fix = ["matching_score", "semantic_score", "rules_score", "salary_min"]
        for col in cols_to_fix:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        df["published_at"] = pd.to_datetime(
            df["published_at"], format="ISO8601", errors="coerce"
        )
        df["original_url"] = df["original_url"].fillna("")

        if "skills" in df.columns:
            df["skills"] = df["skills"].str.replace(r"[\[\]']", "", regex=True)

        # Helpers visuels
        def get_score_visual(score):
            if score >= 80:
                return f"🟢 {score}%"
            if score >= 60:
                return f"🟡 {score}%"
            return f"🔴 {score}%"

        df["matching_visual"] = df["matching_score"].apply(get_score_visual)
        df["salary_visual"] = df["salary_min"].apply(
            lambda x: f"{int(x):,} €".replace(",", " ") if x > 0 else "N/A"
        )

        # Statistiques Marché
        limit_date = datetime.now() - timedelta(hours=48)
        new_jobs_count = len(df[df["published_at"] >= limit_date])

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Offres analysées", len(df))
        k2.metric("Nouveautés (<48h)", f"{new_jobs_count}")
        k3.metric("Matching Moyen", f"{int(df['matching_score'].mean())}%")

        valid_salaries = df[df["salary_min"] > 0]["salary_min"]
        avg_salary = (
            f"{int(valid_salaries.mean()):,} €".replace(",", " ")
            if not valid_salaries.empty
            else "N/A"
        )
        k4.metric("Salaire Moyen", avg_salary)

        st.divider()

        # Filtres interactifs
        col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
        search_query = col_f1.text_input(
            "🔍 Recherche rapide", placeholder="ex: AWS, Spark, Nantes..."
        )
        min_score = col_f2.slider("Score min.", 0, 100, 30)
        show_only_fresh = col_f3.toggle("✨ Nouveautés uniquement")

        # Application des filtres
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

        # Affichage du tableau principal
        st.dataframe(
            filtered_df,
            column_config={
                "matching_visual": st.column_config.TextColumn(
                    "Matching", help="Score hybride (IA + Métier)", width="small"
                ),
                "published_at": st.column_config.DatetimeColumn(
                    "Publié", format="D MMM, HH:mm"
                ),
                "title": st.column_config.TextColumn("Poste", width="large"),
                "positive_labels": st.column_config.TextColumn(
                    "Points Positifs", width="medium"
                ),
                "negative_labels": st.column_config.TextColumn(
                    "Points Négatifs", width="medium"
                ),
                "salary_visual": st.column_config.TextColumn(
                    "Salaire Min", width="small"
                ),
                "platform": "Source",
                "original_url": st.column_config.LinkColumn("Lien", display_text="🔗"),
            },
            column_order=(
                "published_at",
                "matching_visual",
                "title",
                "positive_labels",
                "negative_labels",
                "company_name",
                "city",
                "salary_visual",
                "skills",
                "platform",
                "original_url",
            ),
            width="stretch",
            hide_index=True,
        )
        st.caption(
            f"Dernière synchronisation Cloud : {datetime.now().strftime('%d/%m/%Y à %H:%M')}"
        )

    else:
        st.warning(
            "📡 En attente de données... L'API est peut-être en phase de démarrage."
        )

# --- ONGLET 🏗️ : ARCHITECTURE & TECH ---
with tab_tech:
    st.header("Spécifications Techniques")

    st.markdown(
        """
        **Pipeline Status :** [![JobRadar CI](https://github.com/ManuelBenoist/jobradar/actions/workflows/main.yml/badge.svg)](https://github.com/ManuelBenoist/jobradar/actions/workflows/main.yml)
        """
    )

    col_a, col_b = st.columns(2)
    with col_a:
        with st.expander("🏗️ Data Engineering (ELT)", expanded=True):
            st.markdown("""
            - **Ingestion :** Lambdas Python multi-sources (Triggers EventBridge).
            - **Processing :** PySpark sur AWS (Déduplication, NLP, Hashing).
            - **Entrepôt :** S3 (Architecture Médaillon) + AWS Athena.
            - **Modélisation :** dbt Cloud (Scoring métier & sémantique).
            """)

    with col_b:
        with st.expander("☁️ Cloud & DevOps", expanded=True):
            st.markdown("""
            - **IaC :** Terraform (S3, ECR, Lambda, IAM, Glue).
            - **CI/CD :** GitHub Actions (Automated QA & Deployment).
            - **API :** FastAPI containerisée sur AWS Lambda.
            - **Observabilité :** CloudWatch Logs & GitHub Status Badges.
            """)

    st.divider()
    st.subheader("Extrait Logique : Scoring Hybride (dbt)")
    st.code(
        """
-- Exemple de pondération des bonus (extrait de fct_jobs.sql)
SELECT 
    *,
    -- Synergie technique (Bonus stack cible)
    (CASE WHEN contains(extracted_skills, 'python') THEN 15 ELSE 0 END +
     CASE WHEN contains(extracted_skills, 'dbt') AND contains(extracted_skills, 'spark') THEN 15 ELSE 0 END) as p_tech_skills,
    
    -- Bonus fraîcheur (Time Decay)
    (CASE WHEN date_diff('day', published_at, current_timestamp) <= 2 THEN 10 ELSE 0 END) as p_freshness
    
FROM {{ ref('stg_silver_jobs') }}
        """,
        language="sql",
    )
