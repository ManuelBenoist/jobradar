# JobRadar

**JobRadar** est une plateforme automatisée de veille et d'analyse du marché de l'emploi Data & DevOps. Le projet implémente un pipeline ELT complet, de l'ingestion multi-sources à la visualisation, en passant par une phase de transformation distribuée et de modélisation SQL.

## Concept & Valeur Ajoutée
L'outil collecte chaque matin les offres d'emploi, les normalise, les déduplique et les enrichit avec un **score de matching personnalisé**. Il permet de monitorer en temps réel le volume d'offres par technologie chez les acteurs majeurs du marché (ESN, startups, grands comptes).

## Architecture Technique



### 1. Ingestion (Event-Driven)
* **Sources :** * **France Travail API** (via francetravail.io) : Source principale couvrant ~80% du marché (incluant les partenaires Indeed/Monster).
    * **Adzuna API** : Agrégateur européen pour capter les offres complémentaires (startups/ESN directes).
* **Technos :** AWS Lambda (Python + Pandas) déclenchées par AWS EventBridge.
* **Fréquence :** Refresh quotidien à 8h00 (configurable via Terraform).

### 2. Stockage (Data Lake S3)
Organisation en architecture médaillon :
* `raw/` : Payloads JSON bruts horodatés.
* `processed/` : Données nettoyées et partitionnées au format Parquet.
* `curated/` : Tables finales (Gold) prêtes pour l'analyse.

### 3. Transformation & Modélisation
* **PySpark :** Nettoyage lourd et **déduplication multi-sources** via un algorithme de hashing (Titre + Entreprise + Date). Parsing des descriptions pour l'extraction des technos par Regex.
* **dbt Core :** * **Bronze** : Mapping des sources PySpark.
    * **Silver** : Normalisation, typage et tests de cohérence.
    * **Gold** : Calcul du score de matching (SQL analytique) et agrégats métier.
* **AWS Athena :** Moteur de requêtage serverless permettant d'exposer les données Gold sans base de données managée.

### 4. Exposition & Alerting
* **API :** FastAPI containerisée (Docker) servant les données depuis Athena.
* **Dashboard :** Interface web filtrable (Technos, Score, Date) hébergée sur GitHub Pages.
* **Alertes :** Lambda + AWS SES envoyant un récapitulatif quotidien des offres avec un score de matching ≥ 70.

### 💰 Cloud Optimization (FinOps)
Le projet est conçu pour minimiser les coûts opérationnels sur AWS :

Stockage Columnar (Parquet) : Réduit la taille des données stockées de ~80% par rapport au JSON et accélère les requêtes Athena.

Partitionnement : Les données sont partitionnées par date, ce qui permet à Athena de ne scanner que les fichiers nécessaires, réduisant le coût par requête.

Compute Serverless : Utilisation exclusive d'AWS Lambda et Athena (pay-as-you-go). Pas de serveurs allumés 24/7.

Lifecycle Policies S3 : Archivage automatique des fichiers raw vers Glacier après 30 jours (via Terraform).

API via AWS Runner : peut engendrer des coûts donc il faut surveiller le compte AWS, ECR. Il serait possible de passer outre en lisant directement les tables gold pour agrémenter les dashboards. 

Si le projet n'est pas utilisé pour un temps : 
```bash
terraform destroy
```
Sinon, surveiller et adapter. 

## 🛠️ Stack Technologique
| Couche | Technologies |
| :--- | :--- |
| **Langages** | Python, SQL Analytique (Window Functions, CTEs), PySpark |
| **Data Stack** | dbt Core, AWS Athena, Pandas |
| **Cloud (AWS)** | S3, Lambda, EventBridge, SES, ECR, IAM |
| **Infrastructure** | Terraform (Infrastructure as Code) |
| **Orchestration** | Kubernetes (minikube/EKS), GitHub Actions (CI/CD) |

## ⚙️ Justifications Techniques
* **PySpark vs Pandas** : PySpark est utilisé pour le traitement batch complexe et la déduplication. Pandas est réservé aux environnements serverless (Lambda/API) pour sa légèreté.
* **Terraform** : Garantit la reproductibilité totale. `terraform apply` recrée l'infra complète (S3, IAM, Lambda, Athena) en moins de 3 minutes.
* **dbt** : Assure la documentation automatique et la robustesse via des tests de données (uniqueness, non-null) avant l'exposition.

## 🚀 CI/CD Workflow
Le pipeline GitHub Actions automatise :
1.  **Linting** : Validation du code via Ruff.
2.  **Tests** : Tests unitaires avec Pytest.
3.  **Build** : Création de l'image Docker multi-stage.
4.  **Deploy** : Push vers AWS ECR et mise à jour des Lambdas / manifests Kubernetes.

---
*Ce projet démontre une maitrise transverse en Data Engineering et culture DevOps, appliquée à un cas d'usage réel et opérationnel.*