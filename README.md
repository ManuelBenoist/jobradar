# JobRadar

**JobRadar** est une plateforme automatisée de veille et d'analyse du marché de l'emploi Data & DevOps. Le projet implémente un pipeline ELT complet, de l'ingestion multi-sources à la visualisation, en passant par une phase de transformation distribuée et de modélisation SQL.

## Concept & Valeur Ajoutée
L'outil collecte chaque matin les offres d'emploi, les normalise, les déduplique et les enrichit avec un **score de matching personnalisé**. Il permet de monitorer en temps réel le volume d'offres par technologie chez les acteurs majeurs du marché (ESN, startups, grands comptes).

## Architecture Technique

### 1. Ingestion (Event-Driven)
* **Sources :** * **France Travail API** : Source principale (via Auth OAuth2).
    * **Adzuna API** : Agrégateur européen pour capter les offres complémentaires.
* **Technos :** AWS Lambda (Python) déclenchées par AWS EventBridge.
* **Fréquence :** Refresh quotidien à 8h00 UTC.

### 2. Stockage (Data Lake S3)
Organisation en architecture médaillon :
* `raw/` : Payloads JSON bruts horodatés.
* `processed/` : Données nettoyées et partitionnées au format **Parquet**.
* `curated/` : Tables finales (Gold) prêtes pour l'analyse.

### 3. Transformation & Modélisation
* **PySpark (Silver) :** Nettoyage lourd et **déduplication multi-sources** via un algorithme de hashing (SHA256). Parsing des descriptions pour l'extraction des technos par Regex. Exécuté sur **GitHub Actions** (Compute).
* **dbt Core (Gold) :** * **Silver** : Mapping des tables Parquet via Athena.
    * **Gold** : Calcul du score de matching (SQL analytique) et agrégats métier.
* **AWS Athena :** Moteur de requêtage serverless permettant d'exposer les données Gold sans base de données managée.

### 4. Exposition & Orchestration
* **API :** FastAPI containerisée (Docker) déployée sur **AWS App Runner** (Endpoint public sécurisé).
* **Dashboard :** Interface interactive **Streamlit** hébergée sur **Streamlit Community Cloud** (Gratuit à vie), consommant les données via l'API.
* **Orchestration :** Workflow engine piloté par **GitHub Actions**, gérant l'enchaînement automatique chaque matin : Ingestion → Spark → dbt → Refresh API.

### 💰 Cloud Optimization (FinOps)
Le projet est optimisé pour un coût réel de **0€** pour l'utilisateur :
* **Stockage Columnar (Parquet)** : Réduit la taille des données de ~80% et minimise les frais de scan Athena.
* **Compute Hybride** : Les transformations lourdes (Spark/dbt) sont déportées sur les runners GitHub gratuits pour économiser les ressources AWS.
* **Gestion des Crédits** : Le coût fixe d'App Runner (~7$/mois) est intégralement couvert par les crédits AWS (enveloppe de 140$), garantissant 0€ de reste à charge pendant 20 mois. Si le projet n'est pas utilisé pour un temps : 
```bash
terraform destroy
```
Sinon, il faudra passer à une solution 0 frais comme en permettant au dashboard d'aller directement piocher dans la couche gold S3.
* **Lifecycle Policies S3** : Nettoyage automatique des fichiers temporaires et archivage vers Glacier.

## 🛠️ Stack Technologique
| Couche | Technologies |
| :--- | :--- |
| **Langages** | Python, SQL Analytique, PySpark |
| **Data Stack** | dbt Core, AWS Athena, Pandas |
| **Cloud (AWS)** | S3, Lambda, App Runner, ECR, IAM |
| **Infrastructure** | Terraform (Infrastructure as Code) |
| **Orchestration** | GitHub Actions (CI/CD & Cron) |

## ⚙️ Justifications Techniques
* **Sécurité & IAM** : Utilisation de **IAM Instance Roles** pour l'API, évitant la manipulation de clés d'accès statiques.
* **Priorisation Valeur (Dashboard vs K8s)** : Le choix d'App Runner et Streamlit Cloud a été privilégié pour délivrer rapidement une interface fonctionnelle. L'orchestration **Kubernetes (Minikube/EKS)** est conservée en tant qu'évolution technique future pour l'apprentissage du scaling.
* **Terraform** : Garantit la reproductibilité totale de l'infrastructure en quelques minutes.

## 🚀 CI/CD Workflow
Le pipeline GitHub Actions automatise chaque matin :
1.  **Ingestion** : Déclenchement des Lambdas AWS.
2.  **Processing** : Exécution du job PySpark (Nettoyage & Silver).
3.  **Modeling** : Transformation dbt sur Athena (Scoring & Gold).
4.  **Deploy** : Build de l'image Docker, Push vers ECR et mise à jour de l'API sur **App Runner**.

---
*Ce projet démontre une maîtrise transverse en Data Engineering et culture DevOps, appliquée à un cas d'usage réel et opérationnel.*