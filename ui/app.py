import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="JobRadar Live | Data & DevOps",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- STYLE CSS ---
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
def fetch_job_data():
    API_URL = st.secrets["API_URL"]
    API_KEY = st.secrets["INTERNAL_API_KEY"] # On récupère la clé dans les secrets
    headers = {"X-API-Key": API_KEY}
    try:
        response = requests.get(API_URL, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erreur de connexion à l'API : {e}")
        return None


data = fetch_job_data()

# --- HEADER ---
st.title("📡 JobRadar Live")
st.caption(
    "Veille automatisée de recherche d'emplois Data & DevOps | Pipeline ELT Serverless sur AWS"
)

tab_radar, tab_tech = st.tabs(["Radar des offres", "Architecture du projet"])

# --- ONGLET 1 : LE RADAR ---
with tab_radar:
    if data and "jobs" in data and len(data["jobs"]) > 0:
        df = pd.DataFrame(data["jobs"])

        df["matching_score"] = (
            pd.to_numeric(df["matching_score"], errors="coerce").fillna(0).astype(int)
        )
        df["salary_min"] = (
            pd.to_numeric(df["salary_min"], errors="coerce").fillna(0).astype(int)
        )
        # Conversion des dates pour les calculs
        # On ajoute format='ISO8601' pour gérer les millisecondes variables
        df["published_at"] = pd.to_datetime(df["published_at"], format='ISO8601', errors='coerce')
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], format='ISO8601', errors='coerce')
        
        df["original_url"] = df["original_url"].fillna("")

        # Nettoyage des compétences
        if "skills" in df.columns:
            df["skills"] = df["skills"].str.replace(r"[\[\]']", "", regex=True)

        # LOGIQUE VISUELLE : Matching Score
        def get_score_visual(score):
            if score >= 85:
                return f"🟢 {score}%"
            if score >= 60:
                return f"🟡 {score}%"
            return f"🔴 {score}%"

        df["matching_visual"] = df["matching_score"].apply(get_score_visual)

        # --- CALCUL DES OFFRES FRAÎCHES (Real Market Date) ---
        # On calcule les offres publiées il y a moins de 48h
        limit_date = datetime.now() - timedelta(hours=48)
        new_jobs_count = len(df[df["published_at"] >= limit_date])

        # 1. KPI CARDS
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Nombre total d'offres", len(df))
        k2.metric("Nouveautés (<48h)", f"{new_jobs_count}")
        k3.metric("Matching Moyen", f"{int(df['matching_score'].mean())}%")
        k4.metric(
            "Salaire Moyen", f"{int(df['salary_min'].mean()):,} €".replace(",", " ")
        )

        st.divider()

        # 2. FILTRES
        col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
        search_query = col_f1.text_input(
            "🔍 Rechercher (Poste, Entreprise, Ville, Techno...)",
            placeholder="ex: AWS, Nantes, Analyste...",
        )
        min_score = col_f2.slider("Score matching min.", 0, 100, 30)
        show_only_fresh = col_f3.toggle("✨ Uniquement les nouveautés")

        filtered_df = df[df["matching_score"] >= min_score]
        
        if show_only_fresh:
            filtered_df = filtered_df[filtered_df["published_at"] >= limit_date]

        if search_query:
            mask = (
                filtered_df["title"].str.contains(search_query, case=False, na=False)
                | filtered_df["company_name"].str.contains(
                    search_query, case=False, na=False
                )
                | filtered_df["city"].str.contains(search_query, case=False, na=False)
                | filtered_df["skills"].str.contains(search_query, case=False, na=False)
                | filtered_df["description"].str.contains(search_query, case=False, na=False)
            )
            filtered_df = filtered_df[mask]

        # 3. AFFICHAGE DU TABLEAU AVEC ORDRE ET TRONCATURE
        st.dataframe(
            filtered_df,
            column_config={
                "matching_visual": st.column_config.TextColumn(
                    "Matching Score",
                    help="Vert: >85% | Jaune: >60% | Rouge: <60%",
                    width="small",
                ),
                "published_at": st.column_config.DatetimeColumn(
                    "Publié le", 
                    format="D MMM, HH:mm"
                ),
                "title": st.column_config.TextColumn("Poste", width="large"),
                "company_name": "Entreprise",
                "description": st.column_config.TextColumn(
                    "Description", width="medium"
                ),
                "salary_min": st.column_config.NumberColumn(
                    "Salaire Min", format="%d €"
                ),
                "skills": "Compétences",
                "city": "Ville",
                "platform": "Source",
                "original_url": st.column_config.LinkColumn(
                    "Lien offre", display_text="🔗"
                ),
            },
            column_order=(
                "matching_visual",
                "published_at",      # Priorité à la date de publication
                "title",
                "company_name",
                "description",
                "salary_min",
                "skills",
                "city",
                "platform",
                "original_url",
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            f"Dernière synchronisation avec AWS Athena : {datetime.now().strftime('%d/%m/%Y à %H:%M')}"
        )

    else:
        st.warning(
            "Aucune donnée disponible. La Lambda est peut-être en cours de réveil."
        )

# --- ONGLET 2 : ARCHITECTURE & TECH ---
with tab_tech:
    st.header("Spécifications Techniques")

    col_a, col_b = st.columns(2)

    with col_a:
        with st.expander("🏗️ Pipeline Data (ELT)", expanded=True):
            st.markdown("""
            - **Ingestion :** 2 Lambdas Python (Trigger EventBridge).
            - **Storage :** S3 (Bronze/Silver/Gold).
            - **Transformation :** - **PySpark** : Déduplication et hashing.
                - **dbt Core** : Modélisation SQL sur Athena.
            """)

    with col_b:
        with st.expander("☁️ Cloud & Architecture", expanded=True):
            st.markdown("""
            - **API :** FastAPI sur AWS Lambda (Serverless).
            - **IaC :** Terraform pour la reproductibilité.
            - **CI/CD :** GitHub Actions.
            - **Sécurité :** IAM Roles & CORS Middleware.
            """)

    st.divider()
    st.subheader("Extrait Logique : Scoring dbt & Fraîcheur")
    # --- MODIFICATION : Mise à jour du snippet SQL ---
    st.code(
        """
-- Calcul du score avec Bonus Fraîcheur
SELECT 
    *,
    -- Points techniques
    (CASE WHEN contains(skills, 'python') THEN 15 ELSE 0 END + ...) as score_tech,
    
    -- Bonus de 5pts si l'offre a moins de 2 jours
    (CASE WHEN date_diff('day', published_at, current_timestamp) <= 2 THEN 5 ELSE 0 END) as score_freshness
    
FROM {{ ref('stg_silver_jobs') }}
    """,
        language="sql",
    )