# 🏗️ Architecture du Projet : JobRadar Live

*Archivé le 30 juillet 2026 — extrait du dashboard Streamlit `ui/app.py` (onglet "Architecture du projet")*

---

## Concept

**JobRadar** n'est pas un simple agrégateur d'offres. En plus de collecter les annonces, il uniformise le tout puis applique un moteur de matching pour les classer selon leur pertinence par rapport à l'utilisateur.

Le but est de filtrer automatiquement des milliers d'offres pour n'afficher que celles qui correspondent réellement à un profil spécifique, en utilisant une double validation : la compréhension sémantique par l'IA et le respect strict de règles métier (seniorité, stack technologique et d'autres critères).

**Valeur ajoutée :**
- ⏱️ Gain de temps (veille automatisée)
- 🎯 Calcul d'un score de matching
- 💰 Coût d'infrastructure quasi nul

---

## 1. Ingestion et pipeline de Données (ELT & Medallion Architecture)

### Le flux de données (Data Lifecycle)

1. **Orchestration & Collecte (Trigger)**
   - **Déclencheur :** Un événement AWS EventBridge (Cron) réveille chaque matin des fonctions AWS Lambda
   - **Action :** Ces fonctions interrogent les 4 APIs (France Travail, Adzuna, JSearch, Jooble) et déchargent les données brutes dans le Bucket S3 (Layer Bronze) au format JSON

2. **Processing Distribué (Compute - Layer Silver)**
   - **Calcul :** Le workflow est piloté par GitHub Actions, qui déploie des workers éphémères exécutant Apache Spark
   - **Action :** Spark lit la couche Bronze, effectue le nettoyage, la déduplication par hashing (SHA-256) et génère les embeddings NLP
   - **Stockage :** Les données sont réécrites dans S3 (Layer Silver) au format Parquet, optimisé pour la lecture colonnaire

3. **Modélisation & Scoring (Compute - Layer Gold)**
   - **Calcul :** dbt Core (exécuté sur GitHub Actions) orchestre des requêtes SQL complexes sur AWS Athena
   - **Action :** dbt transforme la donnée Silver en tables métier Gold (calcul du score final, labels, agrégations)
   - **Stockage :** Les résultats finaux sont catalogués dans AWS Glue et stockés en S3 (Layer Gold)

4. **Exposition & Service (L'API)**
   - **Accès :** Une API FastAPI (hébergée sur AWS Lambda via Mangum) sert de passerelle sécurisée
   - **Action :** Elle exécute des requêtes SQL via Athena pour récupérer uniquement les données nécessaires au Dashboard Streamlit

5. **Visualisation (Dashboard)**
   - **Outil :** Le dashboard est développé en Streamlit et hébergé sur Streamlit Cloud
   - **Action :** Il interroge l'API pour afficher les offres filtrées, les scores de matching et les KPIs en temps réel

### Pourquoi ces choix ?

- **PySpark :** Bien que le volume de données actuel soit gérable en Pandas, PySpark a été choisi pour garantir la scalabilité. Si la source passe de 1 000 à 1 000 000 d'offres, le pipeline reste identique. De plus, Spark permet de distribuer le calcul lourd des embeddings NLP de manière optimale.

- **Architecture Médaillon :** Cette architecture garantit la traçabilité. Si un bug survient dans le calcul du score, on peut repartir de la couche Bronze (brute) pour recalculer sans avoir à ré-interroger les APIs (limitées en nombre de requêtes).

---

## 2. Calcul du score de matching

Le matching repose sur une approche hybride. Tandis que l'IA compare la sémantique globale de l'offre à celle du profil idéal, les règles métier assurent que les critères essentiels sont respectés.

### 🧠 Pilier Sémantique (IA/NLP)

Pour évaluer la pertinence d'une offre, on transforme chaque offre en un vecteur mathématique (Embeddings via `all-MiniLM-L6-v2`). On compare ensuite ce vecteur à celui du 'Profil Idéal' (une offre d'emploi "idéale" selon les critères) via une **Similarité Cosinus**.

Formule mathématique de la Similarité Cosinus :

```
Similarity = (A · B) / (||A|| ||B||)
```

### 📋 Pilier Métier (Règles SQL)

L'IA est complétée par des règles déterministes codées en SQL avec dbt Core. Cela permet de s'assurer que des critères essentiels sont respectés, indépendamment de la formulation de l'offre.

### Formule du Score Final

```
Score = (Score_IA × 0.5) + (Score_règles × 0.5)
```

---

## 3. Architecture Cloud & FinOps

L'architecture est entièrement **Serverless** et **Event-Driven**.

### Comment le coût reste à 0€ ?

- **AWS Lambda :** Facturation à la milliseconde d'exécution. Les runs quotidiens rentrent largement dans le Free Tier permanent.
- **GitHub Actions as Compute :** Au lieu de payer un serveur EC2 ou un cluster Spark (très cher), les ressources gratuites de GitHub Actions sont utilisées pour exécuter les calculs Spark et dbt.
- **Athena & Parquet :** En stockant en Parquet (compressé), Athena ne scanne que quelques Ko par requête.
- **S3 Lifecycle :** Suppression automatique des logs et fichiers temporaires après 7 jours.

### Points forts de l'Infrastructure

- **IaC (Infrastructure as Code) :** Tout l'environnement AWS est déployé via Terraform
- **CI/CD :** Déploiement automatique dès qu'un changement est push sur GitHub
- **Sécurité :** Utilisation d'AWS Secrets Manager et de rôles IAM restrictifs

---

## 4. Stack Technologique Complète

| Domaine | Technologies |
|---------|-------------|
| Langages | Python 3.11, SQL (Presto/Athena), PySpark |
| Cloud (AWS) | S3, Lambda, Athena, Glue (Catalog), EventBridge, IAM, ECR, Secrets Manager |
| Data Engineering | dbt Core, Spark SQL, Parquet, Delta-like partitioning |
| Intelligence Artificielle | Sentence-Transformers (NLP), Scikit-learn (Cosinus Similarity), Pandas |
| Infra & DevOps | Terraform (IaC), Docker, GitHub Actions (CI/CD), Git |
| Visualisation | Streamlit, Streamlit Cloud |

---

*Projet original : https://github.com/ManuelBenoist/jobradar*
