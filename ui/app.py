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
    "Veille automatisée de recherche d'emplois Data & DevOps en région nantaise, personnalisé pour Manuel B.| Pipeline ELT Serverless sur AWS"
)
# --- LIEN GITHUB STYLISÉ ---
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 20px; margin-bottom: 20px; flex-wrap: wrap;">
        <!-- Badge GitHub -->
        <a href="https://github.com/ManuelBenoist/jobradar" target="_blank" style="text-decoration: none;">
            <img src="https://img.shields.io/badge/Voir_le_Code_Source-GitHub-100000?style=for-the-badge&logo=github&logoColor=white" />
        </a>
        <!-- État de la Pipeline -->
        <div style="display: flex; align-items: center; gap: 8px;">
            <span style="font-weight: bold; font-size: 14px;">État de la pipeline :</span>
            <a href="https://github.com/ManuelBenoist/jobradar/actions/workflows/data_pipeline.yml" target="_blank">
                <img src="https://github.com/ManuelBenoist/jobradar/actions/workflows/data_pipeline.yml/badge.svg" />
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
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
        k3.metric("Score matching moyen", f"{int(df['matching_score'].mean())}%")

        valid_salaries = df[df["salary_min"] > 0]["salary_min"]
        avg_salary_text = (
            f"{int(valid_salaries.mean()):,} €".replace(",", " ")
            if not valid_salaries.empty
            else "N/A"
        )
        k4.metric("Salaire Moyen (quand disponible)", avg_salary_text)

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
                is_new = row["published_at"] >= limit_date
                new_badge_html = (
                    (
                        '<span class="badge" style="background-color: #ede9fe; color: #5b21b6; border: 1px solid #ddd6fe; margin-left: 8px;">'
                        "✨ Nouveau</span>"
                    )
                    if is_new
                    else ""
                )
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
                    f"<div>"
                    f'<div class="card-source">{row.get("platform", "Source")}</div>'
                    f'<div style="display: flex; align-items: center;">'  # Flex pour aligner les badges
                    f'<div class="badge" style="background-color: {bg_color}; color: {text_color};">Match : {row["matching_score"]}%</div>'
                    f"{new_badge_html}"
                    f"</div>"
                    f'<div class="card-title">{title_disp[:45]}{"..." if len(title_disp) > 45 else ""}</div>'
                    f'<div class="card-company">🏢 {row.get("company_name", "N/A")}</div>'
                    f'<div style="margin: 10px 0;">{pos_html}{neg_html}</div>'
                    f"</div>"
                    f'<div style="margin-top: auto; padding-top: 15px;">'
                    f'<a href="{row.get("original_url", "#")}" target="_blank" style="text-decoration: none;">'
                    f'<button style="width: 100%; padding: 8px; background-color: #2563eb; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">'
                    f"Détails & Postuler 🔗</button></a></div></div>"
                )
                st.markdown(card_html, unsafe_allow_html=True)
        st.divider()

        # 3. AFFICHAGE DU TABLEAU (Config complète conservée)
        st.subheader("📋 Offres Filtrées :")

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
    st.header("🏗️ Architecture du Projet : JobRadar Live")

    # --- SECTION 0 : Concept ---
    st.subheader("Concept")
    col_mission_1, col_mission_2 = st.columns([2, 1])
    with col_mission_1:
        st.markdown(
            """
            **JobRadar** n'est pas un simple agrégateur d'offres. En plus de collecter les annonces, il uniformise le tout puis applique un moteur de matching pour 
            les classer selon leur pertinence par rapport à l'utilisateur.
            
            Le but est de filtrer automatiquement des milliers d'offres pour n'afficher que celles qui 
            correspondent réellement à un profil spécifique, en utilisant une double validation : 
            la compréhension sémantique par l'IA et le respect strict de règles métier (seniorité, stack technologique et d'autres critères).
            """
        )
    with col_mission_2:
        st.markdown(
            """
            **Valeur ajoutée :**
            - ⏱️ Gain de temps (veille automatisée).
            - 🎯 Calcul d'un score de matching.
            - 💰 Coût d'infrastructure quasi nul.
            """
        )

    st.divider()

    # --- SECTION 1 : PIPELINE ELT & MEDALLION ---
    st.subheader("1. Ingestion et pipeline de Données (ELT & Medallion Architecture)")

    col_flow1, col_flow2 = st.columns([1, 1])

    with col_flow1:
        st.info("**Le flux de données (Data Lifecycle) :**")
        st.write(
            "Le parcours de chaque offre depuis sa source jusqu'à l'affichage final est structuré en plusieurs étapes clés :"
        )
        st.markdown(
            """
            *   **Ingestion (Event-Driven) :** Des fonctions **AWS Lambda** se déclenchent chaque matin pour collecter les données de 4 APIs (France Travail, Adzuna, JSearch, Jooble). 
            *   **Stockage S3 (Data Lakehouse) :** Les données transitent par trois étages :
                - **Bronze :** Stockage des JSON bruts (Source of Truth).
                - **Silver :** Données nettoyées, dédupliquées par hashing et converties en **Parquet** (format colonnaire compressé) par Apache Spark.
                - **Gold :** Tables finales calculées par dbt, prêtes pour l'affichage.
            *   **Transformation (Spark & NLP) :** Le traitement est déporté sur **GitHub Actions** pour transformer le brut en vecteurs sémantiques.
            *   **Analyse (dbt + Athena) :** Les calculs finaux sont exécutés en SQL directement sur les fichiers S3.
            """
        )

    with col_flow2:
        st.success("**Pourquoi ces choix ?**")
        with st.expander("Le choix de PySpark", expanded=True):
            st.write(
                """
                Bien que le volume de données actuel soit gérable en Pandas, **PySpark** a été choisi pour garantir la 
                **scalabilité**. Si demain la source passe de 1 000 à 1 000 000 d'offres, le pipeline reste identique. 
                De plus, Spark permet de distribuer le calcul lourd des embeddings NLP de manière optimale.
                """
            )
        with st.expander("L'Architecture Médaillon"):
            st.write(
                """
                Cette architecture garantit la **traçabilité**. Si un bug survient dans le calcul du score, on peut 
                repartir de la couche Bronze (brute) pour recalculer sans avoir à ré-interroger les APIs (limitées en nombre de requêtes).
                """
            )

    st.divider()

    # --- SECTION 2 : LE MOTEUR DE MATCHING HYBRIDE ---
    st.subheader("2. Calcul du score de matching")
    st.markdown(
        """
        Le matching repose sur une approche hybride. Pourquoi ? Tandis que l'IA compare la sémantique globale de l'offre à celle de mon profil idéal, les règles métier assurent que les critères essentiels sont respectés.
        Par exemple, une offre peut être très proche sémantiquement mais être un stage (veto). L'approche hybride permet de combiner la flexibilité de l'IA avec la rigueur des règles métier, offrant ainsi un score de matching plus fiable et pertinent.
        """
    )

    m1, m2 = st.columns(2)

    with m1:
        st.markdown("**🧠 Pilier Sémantique (IA/NLP)**")
        st.write(
            """
            Pour évaluer la pertinence d'une offre,
            on transforme chaque offre en un vecteur mathématique (Embeddings via `all-MiniLM-L6-v2`). 
            On compare ensuite ce vecteur à celui de mon 'Profil Idéal' (une offre d'emploi "idéale" selon mes critères) via une **Similarité Cosinus**. 
            On obtient ainsi un score de 0 à 100 indiquant à quel point l'offre correspond à mes attentes, même si les mots exacts diffèrent : par exemple, un poste de 'Data Engineer' peut être très proche d'un 'Ingénieur Data'. 
            même si les mots-clés exacts diffèrent.
            
            La formule mathématique de la Similarité Cosinus est la suivante :
            """
        )
        st.latex(
            r"\text{Similarity} = \frac{\mathbf{A} \cdot \mathbf{B}}{\|\mathbf{A}\| \|\mathbf{B}\|}"
        )

    with m2:
        st.markdown("**📋 Pilier Métier (Règles SQL)**")
        st.write(
            """
            L'IA est complétée par des règles déterministes codées en SQL avec **dbt Core**. Cela permet de s'assurer que des critères essentiels sont respectés, indépendamment de la formulation de l'offre. Par exemple :
            Si une offre mentionne 'Stage' ou 'Alternance' dans la description alors que le titre est ambigu, elle reçoit un veto.
            De plus, cela permet de valoriser des éléments précisément définis qui pourraient être sous-représentés dans le score sémantique (ex: mention d'un certain nombre d'années d'expérience minimum). 
            """
        )

    st.markdown("---")

    st.markdown("**Formule du Score Final :**")
    st.latex(r"Score = (Score_{IA} \times 0.5) + (Score_{règles} \times 0.5)")

    st.divider()

    # --- SECTION 3 : CLOUD & FINOPS ---
    st.subheader("3. Architecture Cloud & FinOps")

    col_explanation, col_finops = st.columns([1, 1])

    with col_explanation:
        st.markdown(
            """
            L'architecture est entièrement **Serverless** et **Event-Driven**. 
            
            **Comment le coût reste à 0€ ?**
            - **AWS Lambda :** Facturation à la milliseconde d'exécution. Les runs quotidiens rentrent largement dans le Free Tier permanent.
            - **GitHub Actions as Compute :** Au lieu de payer un serveur EC2 ou un cluster Spark (très cher), j'utilise les ressources gratuites de GitHub Actions pour exécuter les calculs Spark et dbt.
            - **Athena & Parquet :** En stockant en Parquet (compressé), Athena ne scanne que quelques Ko par requête, ce qui rend le coût de recherche quasi nul.
            - **S3 Lifecycle :** Suppression automatique des logs et fichiers temporaires après 7 jours.
            """
        )

    with col_finops:
        st.success("**Points forts de l'Infrastructure :**")
        st.markdown(
            """
            - **IaC (Infrastructure as Code) :** Tout l'environnement AWS est déployé via **Terraform**.
            - **CI/CD :** Déploiement automatique dès qu'un changement est push sur GitHub.
            - **Sécurité :** Utilisation d'AWS Secrets Manager et de rôles IAM restrictifs.
            """
        )

    st.divider()

    # --- SECTION 4 : STACK TECHNIQUE EXHAUSTIVE ---
    st.subheader("4. Stack Technologique Complète")

    st.table(
        {
            "": [
                "Langages",
                "Cloud (AWS)",
                "Data Engineering",
                "Intelligence Artificielle",
                "Infra & DevOps",
                "Visualisation",
            ],
            "Technologies exploitées": [
                "Python 3.11, SQL (Presto/Athena), PySpark",
                "S3, Lambda, Athena, Glue (Catalog), EventBridge, IAM, ECR, Secrets Manager",
                "dbt Core, Spark SQL, Parquet, Delta-like partitioning",
                "Sentence-Transformers (NLP), Scikit-learn (Cosinus Similarity), Pandas",
                "Terraform (IaC), Docker, GitHub Actions (CI/CD), Git",
                "Streamlit, Streamlit Cloud",
            ],
        }
    )

    # --- LIEN GITHUB STYLISÉ ---
    st.markdown(
        """
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
            <a href="https://github.com/ManuelBenoist/jobradar" target="_blank">
                <img src="https://img.shields.io/badge/Voir_le_Code_Source-GitHub-100000?style=for-the-badge&logo=github&logoColor=white" />
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # --- SECTION : CONTACT & NETWORKING ---
    st.divider()
    st.subheader("📬 Contact")
    st.markdown(
        """
        Comme le projet le suggère, je suis ouvert aux nouvelles opportunités !
        """
    )

    # Liens stylisés sous forme de badges
    contact_html = """
    <div style="display: flex; gap: 10px; flex-wrap: wrap;">
        <a href="https://www.linkedin.com/in/manuel-benoist" target="_blank">
            <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" />
        </a>
        <a href="mailto:manuelbenoist@gmail.com" target="_blank">
            <img src="https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white" />
        </a>
        <a href="https://github.com/ManuelBenoist" target="_blank">
            <img src="https://img.shields.io/badge/Portfolio_GitHub-181717?style=for-the-badge&logo=github&logoColor=white" />
        </a>
    </div>
    """
    st.markdown(contact_html, unsafe_allow_html=True)

    st.caption("© 2026 | JobRadar Live par Manuel B.")
