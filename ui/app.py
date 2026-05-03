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


@st.cache_data(ttl=300)  # On cache le statut 5 min pour ne pas spammer l'API
def fetch_pipeline_status():
    """Appelle l'API pour connaître l'état de santé du système."""
    BASE_URL = st.secrets["API_URL"].replace("/jobs", "")
    clean_url = f"{BASE_URL.rstrip('/')}/health/pipeline"
    try:
        response = requests.get(clean_url, timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# --- STYLE CSS PERSONNALISÉ ---
st.markdown(
    """
    <style>
    /* Utilisation des variables de thème Streamlit pour le fond et le texte */
    .stApp { background-color: var(--background-color); }
    
    /* KPI Cards (Metrics) adaptatives */
    [data-testid="stMetric"] {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        padding: 15px;
        border-radius: 10px;
        color: var(--text-color);
    }

    /* Force l'étirement des colonnes pour la même hauteur */
    [data-testid="column"] {
        display: flex !important;
    }
    [data-testid="column"] > div {
        flex-grow: 1 !important;
        display: flex !important;
        flex-direction: column !important;
    }
    [data-testid="stVerticalBlock"] {
        flex-grow: 1 !important;
        display: flex !important;
        flex-direction: column !important;
    }

    /* Job Cards adaptatives */
    .job-card {
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        position: relative;
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        height: 100%;
        transition: all 0.3s ease;
        color: var(--text-color);
    }
    
    .job-card:hover {
        border-color: #2563eb;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.2);
        transform: translateY(-2px);
    }
    
    .card-title { font-size: 1.1rem; font-weight: 700; color: var(--text-color); margin-bottom: 5px; padding-right: 60px; }
    .card-company { color: var(--text-color); opacity: 0.8; font-weight: 600; font-size: 0.9rem; margin-bottom: 10px; }
    .card-source { position: absolute; top: 15px; right: 15px; font-style: italic; font-size: 0.7rem; color: var(--text-color); opacity: 0.6; }
    
    .badge {
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        display: inline-block;
        margin-bottom: 10px;
        width: fit-content;
    }
    
    .label-tag {
        display: inline-block;
        font-size: 0.7rem;
        padding: 2px 6px;
        border-radius: 4px;
        margin-right: 4px;
        margin-bottom: 4px;
        border: 1px solid rgba(128, 128, 128, 0.2);
    }
    .pos-tag { background-color: rgba(0, 255, 0, 0.1); color: #22c55e; }
    .neg-tag { background-color: rgba(255, 0, 0, 0.1); color: #ef4444; }
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
        limit_date = datetime.now() - timedelta(hours=24)
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
        k2.metric("Nouveautés (<24h)", f"{new_jobs_count}")
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

        # --- TOP MATCHES CARDS ---
        st.subheader("🎯 Top 3 des meilleures offres")

        # On récupère les 3 meilleures offres depuis filtered_df
        top_jobs = filtered_df.nlargest(3, "matching_score")
        cols = st.columns(3)

        for idx, (_i, row) in enumerate(top_jobs.iterrows()):
            with cols[idx]:
                is_new = row['published_at'] >= limit_date
                new_badge_html = (
                    f'<span class="badge" style="background-color: #ede9fe; color: #5b21b6; border: 1px solid #ddd6fe; margin-left: 8px;">'
                    f'✨ Nouveau</span>'
                ) if is_new else ""
                pos_html = ""
                raw_pos = row.get("positive_labels")
                if pd.notnull(raw_pos) and raw_pos != "":
                    labels = raw_pos.split(",") if isinstance(raw_pos, str) else raw_pos
                    for label in labels[:2]:
                        pos_html += (
                            f'<span class="label-tag pos-tag">▲ {label.strip()}</span>'
                        )

                neg_html = ""
                raw_neg = row.get("negative_labels")
                if pd.notnull(raw_neg) and raw_neg != "":
                    labels = raw_neg.split(",") if isinstance(raw_neg, str) else raw_neg
                    for label in labels[:1]:
                        neg_html += (
                            f'<span class="label-tag neg-tag">▼ {label.strip()}</span>'
                        )

                bg_color = "#dcfce7" if row["matching_score"] >= 80 else "#fef9c3"
                text_color = "#166534" if row["matching_score"] >= 80 else "#854d0e"
                title_disp = str(row.get("title", "Poste sans titre"))

                card_html = (
                    f'<div class="job-card">'
                    f'<div>'
                    f'<div class="card-source">{row.get("platform", "Source")}</div>'
                    f'<div style="display: flex; align-items: center;">' # Flex pour aligner les badges
                    f'<div class="badge" style="background-color: {bg_color}; color: {text_color};">Match : {row["matching_score"]}%</div>'
                    f'{new_badge_html}'
                    f'</div>'
                    f'<div class="card-title">{title_disp[:45]}{"..." if len(title_disp) > 45 else ""}</div>'
                    f'<div class="card-company">🏢 {row.get("company_name", "N/A")}</div>'
                    f'<div style="margin: 10px 0;">{pos_html}{neg_html}</div>'
                    f'</div>'
                    f'<div style="margin-top: auto; padding-top: 15px;">'
                    f'<a href="{row.get("original_url", "#")}" target="_blank" style="text-decoration: none;">'
                    f'<button style="width: 100%; padding: 8px; background-color: #2563eb; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">'
                    f'Détails & Postuler 🔗</button></a></div></div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)
        st.divider()

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
