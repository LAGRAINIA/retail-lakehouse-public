# Documentation du projet Retail Lakehouse

Projet d'entraînement de bout en bout : une enseigne de sport omnicanale fictive (**Stride Sports**), ses
systèmes sources, une plateforme de données **Databricks** (Unity Catalog, Delta Lake), des transformations
**dbt**, une orchestration **Airflow** sur **Astronomer** (local + cloud), et une **CI/CD GitHub Actions**
qui promeut le code de dev vers uat puis prod.

## Ordre de lecture

| # | Document | Ce que tu y trouves |
|---|---|---|
| 1 | [01_VUE_ENSEMBLE.md](01_VUE_ENSEMBLE.md) | le contexte métier, l'architecture, les choix techniques et leurs raisons |
| 2 | [02_ENVIRONNEMENTS.md](02_ENVIRONNEMENTS.md) | dev / uat / prod : Astro, catalogues, variables, secrets, branches, CI/CD |
| 3 | [03_SOURCES.md](03_SOURCES.md) | les 9 flux sources : format, colonnes, volumes, défauts, événements datés |
| 4 | [04_INGESTION_BRONZE.md](04_INGESTION_BRONZE.md) | la couche bronze : `COPY INTO`, idempotence, dérive de schéma, petits fichiers |
| 5 | [05_DBT.md](05_DBT.md) | le projet dbt modèle par modèle : grain, clés, logique, tests, contrats |
| 6 | [06_AIRFLOW.md](06_AIRFLOW.md) | les 3 DAGs, les Assets, Cosmos, le watcher, les paramètres |
| 7 | [07_EXPLOITATION.md](07_EXPLOITATION.md) | le runbook : opérations courantes, incidents, coûts, erreurs connues |
| 8 | [SCENARIOS.md](SCENARIOS.md) | **les exercices** : défauts, événements, incidents à injecter, situations d'équipe |

## Glossaire

| Terme | Sens dans ce projet |
|---|---|
| **Landing** | zone de dépôt des fichiers bruts : le volume Unity Catalog `<catalogue>.landing.raw` |
| **Bronze** | données brutes chargées telles quelles, une table Delta par flux (`<catalogue>.bronze.*`) |
| **Silver** | données typées, nettoyées, dédoublonnées, historisées (`staging` + `intermediate` dans dbt) |
| **Gold** | modèles métier prêts pour la BI : faits, dimensions, KPI (`marts` dans dbt) |
| **Quality** | la quarantaine des lignes rejetées (`dq_rejected_sales_lines`) |
| **Jour de dépôt (landing date)** | le jour où un fichier arrive ; peut différer du **jour métier** (fichier en retard) |
| **Asset** | objet Airflow 3 qui représente une donnée ; un DAG qui la met à jour déclenche ceux qui en dépendent |
| **Deployment** | un environnement Airflow hébergé sur Astro (ici `retail-uat` et `retail-prod`) |
| **Target dbt** | l'environnement visé par dbt : `local`, `dev`, `uat`, `prod` |
| **SCD2** | historisation d'une dimension : une ligne par version, avec `valid_from` / `valid_to` |
| **CDC** | *change data capture* : la source envoie les changements (ici les commandes web et le CRM) |
