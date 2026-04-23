import streamlit as st
import requests
import pandas as pd
from datetime import datetime

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
    try:
        response = requests.get(API_URL, timeout=15)
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

        # NETTOYAGE & CALCULS
        df["matching_score"] = (
            pd.to_numeric(df["matching_score"], errors="coerce").fillna(0).astype(int)
        )
        df["salary_min"] = (
            pd.to_numeric(df["salary_min"], errors="coerce").fillna(0).astype(int)
        )
        df["original_url"] = df["original_url"].fillna("")

        # Nettoyage des crochets et guillemets pour les compétences
        if "skills" in df.columns:
            df["skills"] = df["skills"].str.replace(r"[\[\]']", "", regex=True)

        # Ajout de la colonne Description si elle n'existe pas encore (Mock pour le futur)
        if "description" not in df.columns:
            df["description"] = "Cliquez pour voir le détail..."

        # LOGIQUE VISUELLE : Matching Score (Indicateur de couleur)
        def get_score_visual(score):
            if score >= 85:
                return f"🟢 {score}%"
            if score >= 60:
                return f"🟡 {score}%"
            return f"🔴 {score}%"

        df["matching_visual"] = df["matching_score"].apply(get_score_visual)

        # Calcul des offres du jour
        today_str = datetime.now().strftime("%Y-%m-%d")
        new_jobs_today = (
            len(df[df["ingestion_date"] == today_str])
            if "ingestion_date" in df.columns
            else 0
        )

        # 1. KPI CARDS
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Nombre total d'offres", len(df))
        k2.metric("Nouvelles offres (24h)", f"+{new_jobs_today}")
        k3.metric("Matching Moyen", f"{int(df['matching_score'].mean())}%")
        k4.metric(
            "Salaire Moyen", f"{int(df['salary_min'].mean()):,} €".replace(",", " ")
        )

        st.divider()

        # 2. FILTRES
        col_f1, col_f2 = st.columns([3, 1])
        search_query = col_f1.text_input(
            "🔍 Rechercher (Poste, Entreprise, Ville, Techno...)",
            placeholder="ex: AWS, Nantes, Analyste...",
        )
        min_score = col_f2.slider("Score matching min.", 0, 100, 30)

        filtered_df = df[df["matching_score"] >= min_score]
        if search_query:
            mask = (
                filtered_df["title"].str.contains(search_query, case=False, na=False)
                | filtered_df["company_name"].str.contains(
                    search_query, case=False, na=False
                )
                | filtered_df["city"].str.contains(search_query, case=False, na=False)
                | filtered_df["skills"].str.contains(search_query, case=False, na=False)
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
                "ingestion_date": st.column_config.DateColumn("Date d'ingestion"),
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
                    "Lien offre", display_text="Lien de l'offre (🔗)"
                ),
            },
            column_order=(
                "matching_visual",
                "ingestion_date",
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
            - **CI/CD :** GitHub Actions (Compute déporté à 0€).
            - **Sécurité :** IAM Roles & Function URLs sécurisées.
            """)

    st.divider()
    st.subheader("Extrait Logique : Scoring dbt")
    st.code(
        """
-- Calcul du matching score par pondération de mots-clés
SELECT 
    *,
    (CASE WHEN description ILIKE '%python%' THEN 30 ELSE 0 END +
     CASE WHEN description ILIKE '%aws%' THEN 40 ELSE 0 END +
     CASE WHEN description ILIKE '%dbt%' THEN 30 ELSE 0 END) as matching_score
FROM {{ ref('silver_jobs') }}
    """,
        language="sql",
    )
