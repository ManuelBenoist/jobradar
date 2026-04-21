import streamlit as st
import requests
import pandas as pd

# 1. Config Page & Style
st.set_page_config(page_title="JobRadar Live", page_icon="🎯", layout="wide")

# Custom CSS pour un look "Premium"
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; }
    .stDataFrame { border: 1px solid #30363d; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# 2. Fonctions de récupération
@st.cache_data(ttl=600) # On garde les données 10 min en mémoire pour la fluidité
def load_jobs():
    API_URL = "http://127.0.0.1:8000/jobs"
    try:
        r = requests.get(API_URL, params={"limit": 100})
        return pd.DataFrame(r.json()["jobs"])
    except:
        return pd.DataFrame()

df = load_jobs()

# 3. Sidebar - Les Filtres (Expertise Data)
st.sidebar.title("🛠️ Paramètres")
score_min = st.sidebar.slider("Matching Score Minimum", 0, 100, 40)
tech_filter = st.sidebar.multiselect("Filtrer par Techno", ["Python", "Spark", "AWS", "Terraform", "dbt"])

# --- NAVIGATION ---
tab_radar, tab_arch = st.tabs(["🎯 Mon Radar Live", "⚙️ Architecture & Tech"])

with tab_radar:
    if not df.empty:
        # Filtrage dynamique en local (plus rapide que de rappeler l'API)
        mask = df['matching_score'] >= score_min
        filtered_df = df[mask]

        # 4. KPI Cards
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Jobs Pertinents", len(filtered_df))
        kpi2.metric("Score Moyen", f"{filtered_df['matching_score'].mean():.1f}%")
        kpi3.metric("Top Techno", "Python" if len(filtered_df) > 0 else "N/A")

        st.markdown("### 📋 Dernières opportunités détectées")

        # 1. On définit la correspondance entre noms API et noms Affichage
        mapping = {
            "title": "Poste",
            "company_name": "Entreprise",
            "city": "Ville",
            "skills": "Compétences",
            "matching_score": "Score %"
        }

        # 2. On filtre le DataFrame pour ne garder que ce qui nous intéresse
        # On vérifie quand même que les colonnes existent pour éviter de futurs crashs
        present_cols = [c for c in mapping.keys() if c in filtered_df.columns]
        df_display = filtered_df[present_cols].copy()

        # 3. On renomme les colonnes pour le look "Pro"
        df_display = df_display.rename(columns=mapping)

        # 4. Affichage final
        st.dataframe(
            df_display, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Score %": st.column_config.ProgressColumn(
                    "Matching Score",
                    help="Score calculé par dbt en fonction de ton profil",
                    format="%d%%",
                    min_value=0,
                    max_value=100,
                ),
                "Poste": st.column_config.TextColumn("Intitulé du poste")
            }
        )

    else:
        st.error("Impossible de charger les données. Vérifie que l'API tourne.")

with tab_arch:
    st.header("🏗️ Stack Technique & Justifications")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        with st.expander("☁️ Cloud & Infrastructure", expanded=True):
            st.write("**AWS Athena & S3** : Choix du Serverless pour un coût de stockage et de requête proche de 0€.")
            st.write("**Terraform** : Toute l'infrastructure est définie en tant que code pour être reproductible.")
    
    with col_right:
        with st.expander("🔄 Data Pipeline (ELT)", expanded=True):
            st.write("**PySpark (GitHub Actions)** : Transformation lourde déportée pour économiser les ressources AWS.")
            st.write("**dbt Core** : Modélisation SQL pour le calcul du matching score.")

    st.subheader("🚀 Code Highlight : Calcul du Score")
    st.code("""
    -- Extrait de la logique dbt (Gold Layer)
    SELECT 
        *,
        (tech_points + exp_points) / 2 as matching_score
    FROM silver_jobs
    WHERE updated_at >= current_date - interval '7' day
    """, language="sql")