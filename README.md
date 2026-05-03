# 📡 JobRadar Live

**JobRadar** est une plateforme automatisée de veille et d'analyse du marché de l'emploi Data & DevOps. Le projet implémente un pipeline **ELT moderne (architecture Medallion)**, de l'ingestion multi-sources à la visualisation, en passant par une phase de transformation distribuée (Spark) et d'IA sémantique (NLP).

## 🚀 Concept & Valeur Ajoutée
L'outil collecte chaque matin les offres d'emploi, les normalise, les déduplique et les enrichit avec un **score de matching hybride**. Contrairement aux jobboards classiques, JobRadar analyse la **proximité sémantique** entre mon profil idéal et l'offre réelle, tout en appliquant des filtres métiers rigoureux (détection automatique de la seniorité, des faux-positifs et des compétences demandées).

## 🏗️ Architecture Technique

### 1. Ingestion (Event-Driven & Multi-Sources)
* **Sources Standard (Daily) :** **France Travail API** (OAuth2) et **Adzuna**.
* **Sources Premium (MWF) :** **JSearch** (agrégateur via RapidAPI) et **Jooble**.
* **Technos :** AWS Lambda (Python) déclenchées par AWS EventBridge.
* **Architecture :** Découpage granulaire permettant une rotation des clés API et une gestion fine des quotas.

### 2. Stockage (Data Lakehouse S3)
Organisation en architecture médaillon pour une traçabilité totale :
* `bronze/` : Payloads JSON bruts horodatés (Single Source of Truth).
* `silver/` : Données nettoyées, dédupliquées et vectorisées au format **Parquet**.
* `gold/` : Tables finales prêtes pour l'analyse, partitionnées par date d'ingestion.

### 3. Transformation & Intelligence
* **PySpark (Processing Layer) :** 
    * Déduplication multi-sources par hashing SHA256.
    * **NLP (IA) :** Génération d'embeddings via le modèle `all-MiniLM-L6-v2` (Sentence Transformers) pour capturer le contexte sémantique des descriptions.
    * Exécuté sur **GitHub Actions** pour déporter les coûts de calcul.
* **dbt Core (Analytical Layer) :** 
    * Modélisation SQL sur **AWS Athena**.
    * Calcul du score de matching via Similarité Cosinus entre vecteurs d'offres et vecteur de profil idéal.
    * Application de règles métier complexes (Bonus éthique, malus seniorité, détection de faux-positifs contextuels).

### 4. Exposition & Interface
* **API :** FastAPI containerisée (Docker) déployée sur **AWS Lambda (Serverless v2)** via Mangum. Sécurisée par clé API interne et restriction CORS.
* **Dashboard :** Interface **Streamlit** hébergée sur Streamlit Cloud, offrant une visualisation interactive des KPIs (Matching moyen, salaires, labels automatiques).

## 🧠 Moteur de Matching Hybride
Le projet utilise une logique de scoring à deux piliers :
1. **Pilier Sémantique (50%)** : Comparaison vectorielle (IA) pour comprendre les nuances du poste et le comparer à une offre idéale type. 
2. **Pilier Métier (50%)** : Système de bonus/malus paramétrables dans `dbt_project.yml` :
    - **Bonus :** Junior-friendly, Mentorat, Impact écologique (B-Corp), Open source.
    - **Malus/Veto :** Seniorité cachée, stages, ...

## 💰 Cloud Optimization & FinOps
Le projet est conçu pour un coût de fonctionnement approchant des **0€** :
* **Serverless Total** : L'abandon d'App Runner au profit d'AWS Lambda pour l'API élimine tout frais fixe. On ne paye que ce qu'on consomme (souvent couvert par le Free Tier d'AWS).
* **GitHub Actions as Compute** : Utilisation des runners gratuits pour Spark et dbt.
* **Parquet & Partitionnement** : Réduction de 80% du volume de données scanné par Athena.
* **Lifecycle Policies** : Nettoyage automatique des résultats Athena après 7 jours.

## 🛠️ Stack Technologique
| Couche | Technologies |
| :--- | :--- |
| **Langages** | Python 3.11, SQL (Presto/Athena), PySpark |
| **Data & ML** | dbt Core, Sentence-Transformers (NLP), Pandas |
| **Cloud (AWS)** | S3, Lambda, ECR, Glue, EventBridge, Athena |
| **Infra & DevOps** | Terraform (IaC), Docker, GitHub Actions (CI/CD) |
| **Visualisation** | Streamlit, Streamlit Cloud |

## 🚀 Installation & Déploiement
1. **Infra** : `terraform apply` (crée l'intégralité du Data Lake et des rôles IAM).
2. **Profil** : Générer votre vecteur de référence en modifiant l'offre idéale type via `src/scripts/generate_profile.py`.
3. **CI/CD** : Configurer les secrets GitHub pour déclencher la pipeline automatique.

---
*Ce projet démontre une maitrise de différents outils de Data Engineering (Medallion, Spark, dbt) et en Cloud Architecture (AWS Serverless, Terraform).*