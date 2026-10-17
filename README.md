# Retail Lakehouse : Astronomer + dbt + Databricks

Projet d'entraînement réaliste : une enseigne de sport omnicanale fictive, **Stride Sports** (12 magasins dans
5 pays + un site e-commerce). Les systèmes sources déposent chaque jour leurs fichiers ; on les ingère, on les
transforme avec dbt sur Databricks, le tout orchestré par Airflow (Astronomer).

```
 Systèmes sources (simulés)        Databricks (Unity Catalog, catalogue retail)
 ─────────────────────────         ───────────────────────────────────────────────────────────────
 caisses, e-commerce, ERP,   ──►   landing.raw (volume)  ──COPY INTO──►  bronze  ──dbt──►  silver  ──dbt──►  gold
 CRM, stocks, finance              fichiers CSV / JSON                   brut      typé, nettoyé    faits, dimensions,
                                                                                  historisé         KPI, quarantaine

 Airflow (Astro) : retail_source_systems ──Asset──► retail_ingest_bronze ──Asset──► retail_dbt_transform (Cosmos)
```

## Environnements

| Env | Airflow | Catalogue Databricks | Branche | Déploiement |
|---|---|---|---|---|
| **dev** | local (`astro dev start`) | `retail_dev` / `retail` | `dev` | — |
| **uat** | Astro cloud `retail-uat` | `retail_uat` | `uat` | automatique (GitHub Actions) |
| **prod** | Astro cloud `retail-prod` | `retail_prod` | `main` | automatique (GitHub Actions) |

📚 **Documentation complète : [docs/00_INDEX.md](docs/00_INDEX.md)** (architecture, environnements, sources,
bronze, dbt, Airflow, exploitation) et **exercices : [docs/SCENARIOS.md](docs/SCENARIOS.md)**.

## Contenu

| Dossier | Rôle |
|---|---|
| `include/retail_generator/` | génère les fichiers des 9 flux sources, avec défauts réalistes et incidents injectables |
| `include/ingestion/` | couche bronze : `COPY INTO` idempotent, une table par flux |
| `dags/` | 3 DAGs Airflow 3 reliés par des Assets |
| `dags/dbt/retail/` | projet dbt : staging → intermediate → marts (gold) + quality |
| `docs/SCENARIOS.md` | **les exercices** : défauts, événements métier, incidents, situations d'équipe |
| `tests/` | tests du générateur et intégrité des DAGs |

### Les flux sources

| Système | Fichier | Particularités |
|---|---|---|
| Caisses (POS) | `pos/sales/pos_sales_<magasin>_<jour>.csv` | heure locale, devise locale, retours, renvois, retards, v2 du logiciel |
| E-commerce | `ecom/orders/orders_<jour>.json` | un instantané par changement de statut (CDC), lignes imbriquées, événements en retard |
| E-commerce | `ecom/returns/returns_<jour>.csv` | retours web |
| ERP | `erp/products/…csv`, `erp/stores/…csv` | extraits complets quotidiens : changements de prix, nouveau magasin |
| CRM | `crm/customers/…json`, `crm/gdpr_requests/…csv` | flux de changements (I/U), demandes d'effacement RGPD |
| Supply | `supply/inventory/…csv` | stock de fin de journée par magasin et produit |
| Finance | `finance/fx_rates/…json` | taux de change, jours ouvrés seulement |

### Le modèle dbt

- **staging** (silver, vues) : typage, dédoublonnage, réparation, dérive de schéma (`column_or_null`).
- **intermediate** (silver) : taux de change journaliers, produits **SCD2**, clients courants anonymisés RGPD,
  conversion des fuseaux horaires, ventes magasin + web unifiées avec drapeaux qualité.
- **marts** (gold) : `fct_sales_lines` (incrémental `merge`, contrat, liquid clustering), `fct_inventory_daily`,
  `fct_web_returns`, `dim_product` (versions), `dim_store`, `dim_customer` (sans PII), `dim_date`,
  `mart_store_daily_kpis`, `mart_web_fulfilment`.
- **quality** : `dq_rejected_sales_lines`, la quarantaine des lignes rejetées.
- Tests génériques, singuliers (RGPD, réconciliation, complétude, SCD2), **tests unitaires**, **exposures**,
  snapshot `snap_stores`, seed `promo_codes`.

## Mise en route

### 1. Databricks Free Edition
1. Crée un token : *Settings → Developer → Access tokens*.
2. Récupère le *HTTP path* du SQL warehouse : *SQL Warehouses → ton warehouse → Connection details*.
3. `cp .env.example .env`, remplis les 3 valeurs `DATABRICKS_*`, puis :
   ```bash
   python scripts/sync_airflow_connection.py     # crée la connexion Airflow depuis .env
   ```

### 2. Environnement local (sans Airflow, pour itérer vite)
```bash
python3.13 -m venv .venv && .venv/bin/pip install "dbt-databricks==1.12.7" pytest
set -a && . ./.env && set +a

python -m include.ingestion setup                                         # catalogue, schémas, volume
python -m include.retail_generator generate --start 2026-07-01 --end 2026-07-31
python -m include.retail_generator upload
python -m include.ingestion load --start 2026-07-01 --end 2026-07-31     # bronze
cd dags/dbt/retail && dbt deps && dbt build --profiles-dir .              # silver + gold (target dev)
```

### 3. Avec Airflow (Astro)
```bash
astro dev start            # http://localhost:8080
```
1. DAG `retail_source_systems` → *Trigger* avec `start_date` / `end_date` pour charger l'historique.
2. Les DAGs `retail_ingest_bronze` puis `retail_dbt_transform` démarrent seuls (Assets).
3. Ensuite, `retail_source_systems` tourne chaque nuit à 1 h.

### Tests
```bash
.venv/bin/python -m pytest tests/generator      # générateur
astro dev pytest                                # intégrité des DAGs (dans le conteneur)
```

## Commandes utiles
```bash
python -m include.retail_generator incidents                       # lister les incidents
python -m include.retail_generator generate --start 2026-10-17 --incident price_x100
python -m include.ingestion counts                                 # lignes par table bronze
dbt build --select +fct_sales_lines --profiles-dir .
dbt test --select test_type:unit --profiles-dir .
```
