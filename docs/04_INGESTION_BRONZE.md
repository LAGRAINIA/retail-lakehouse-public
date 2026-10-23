# 4. L'ingestion : landing → bronze

Code : `include/ingestion/bronze.py` (SQL généré), `include/ingestion/__main__.py` (CLI),
`dags/retail_ingest_bronze.py` (DAG).

## 4.1 Le principe

Une table Delta bronze par flux, chargée avec **`COPY INTO`** depuis le volume `landing.raw` :

| Table bronze | Dossier du volume | Format |
|---|---|---|
| `erp_stores` | `erp/stores/` | CSV |
| `erp_products` | `erp/products/` | CSV |
| `crm_customers` | `crm/customers/` | JSON |
| `crm_gdpr_requests` | `crm/gdpr_requests/` | CSV |
| `pos_sales` | `pos/sales/` | CSV |
| `ecom_orders` | `ecom/orders/` | JSON |
| `ecom_returns` | `ecom/returns/` | CSV |
| `supply_inventory` | `supply/inventory/` | CSV |
| `finance_fx_rates` | `finance/fx_rates/` | JSON |

Le SQL généré pour un flux :

```sql
CREATE TABLE IF NOT EXISTS retail_prod.bronze.pos_sales;      -- table sans schéma : COPY INTO le crée

COPY INTO retail_prod.bronze.pos_sales
FROM (
    SELECT
        *,
        _metadata.file_path              AS _source_file,      -- lignage : quel fichier
        _metadata.file_modification_time AS _file_modified_at,
        current_timestamp()              AS _ingested_at       -- quand : base de l'incrémental dbt
    FROM '/Volumes/retail_prod/landing/raw/pos/sales/'
)
FILEFORMAT = CSV
PATTERN = '*_{20261016,20261017}*'                             -- seulement les jours livrés
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

## 4.2 Les propriétés importantes

**Idempotence.** `COPY INTO` mémorise les fichiers déjà chargés dans la table. Relancer le même chargement
n'insère rien de plus : on peut relancer une tâche Airflow sans crainte.

> Conséquence piège : un fichier **corrigé** qui garde le **même nom** n'est pas rechargé. Il faut
> `COPY_OPTIONS ('force' = 'true')` sur ce fichier (voir scénario C1). Un fichier renvoyé sous un **autre nom**
> (`_resend`) est chargé une deuxième fois : c'est à dbt de dédoublonner.

**Tout en texte pour les CSV** (`inferSchema = false`). Le bronze accepte tout ce que la source envoie. Le
typage (`try_cast`) se fait dans dbt, où il est testé. Un `try_cast` raté donne `NULL`, que les tests
détectent, au lieu de faire échouer le chargement.

**JSON** avec `primitivesAsString` : les structures imbriquées (`shipping`, `lines`) sont conservées, les
valeurs simples sont en texte.

**Dérive de schéma** (`mergeSchema`). Une nouvelle colonne (POS v2 : `loyalty_points` ; JSON : `device` ; incident
`schema_break` : `qty`) est **ajoutée** à la table au lieu de faire échouer le chargement. Les anciennes lignes
ont `NULL`. Côté dbt, la macro `column_or_null` lit la colonne seulement si elle existe.

**Lignage.** Chaque ligne garde `_source_file`, `_file_modified_at` et `_ingested_at`. dbt s'en sert pour :
- dédoublonner (un fichier `_resend` perd face à l'original) ;
- retrouver la date métier ou de dépôt dans le nom du fichier ;
- filtrer l'incrémental (`_ingested_at`).

## 4.3 Pourquoi un `PATTERN` par jours livrés : l'incident des petits fichiers

**Ce qui s'est passé.** Le premier chargement visait le dossier entier `pos/sales/` : environ **1 300 petits
fichiers CSV** (un par magasin et par jour). La requête est restée **25 minutes en compilation**, sans lire un
seul fichier (`read_files = 0` dans `system.query.history`), puis Databricks l'a arrêtée pour inactivité.
Un seul jour (11 fichiers) se chargeait en 20 secondes, un mois ne finissait pas.

**La cause.** Avec `mergeSchema` sur des CSV sans schéma, Databricks lit l'en-tête de **chaque fichier** pour
fusionner les schémas, pendant la phase de compilation. Le coût est par fichier, et il explose avec le nombre de
fichiers : c'est le classique **problème des petits fichiers**.

**Le diagnostic.**
```sql
SELECT start_time, execution_status, total_duration_ms, read_files, error_message
FROM system.query.history
WHERE statement_text LIKE '%COPY INTO%pos_sales%'
ORDER BY start_time DESC;
```
Pour une requête **en cours**, l'API Query History (`include_metrics`) montrait seulement
`query_compilation_start_timestamp` : la requête n'avait jamais dépassé la compilation.

**La correction.** Chaque `COPY INTO` ne vise que les fichiers des jours livrés, par paquets de 5 jours
(`chunk_size`), avec `PATTERN = '*_{AAAAMMJJ,...}*'`. L'historique complet de `pos_sales` (250 000 lignes) se
charge alors en 200 secondes, un jour en quelques secondes.

**Comment l'ingestion connaît les jours livrés.** Le DAG des sources publie un **événement d'Asset** avec
`{"start": ..., "end": ...}`. Le DAG d'ingestion le lit (`triggering_asset_events`). On prend aussi **la veille**
du premier jour : un fichier de caisse en retard porte la date de son jour métier.

**Les alternatives** :
- un **schéma explicite** : plus d'inférence, mais il faut gérer soi-même l'évolution ;
- **Auto Loader** (`cloudFiles`) ou une *streaming table* : garde l'état des fichiers vus et le schéma inféré
  une seule fois, conçu pour des millions de fichiers ;
- **des fichiers plus gros** côté producteur (un fichier par jour pour tous les magasins).

## 4.4 Utilisation en ligne de commande

```bash
set -a && . ./.env && set +a
python -m include.ingestion setup                                           # catalogue, schémas, volume
python -m include.ingestion load --start 2026-07-01 --end 2026-10-16         # tous les flux
python -m include.ingestion load --start 2026-10-17 --table pos_sales        # un flux, un jour
python -m include.ingestion counts                                          # lignes par table
```

## 4.5 Vérifications utiles

```sql
-- Lignes chargées par fichier pour un jour
SELECT _source_file, count(*) FROM retail_prod.bronze.pos_sales
WHERE _source_file LIKE '%20261017%' GROUP BY 1;

-- Historique des chargements de la table (une version par COPY INTO)
DESCRIBE HISTORY retail_prod.bronze.pos_sales;

-- Colonnes apparues par dérive de schéma
DESCRIBE TABLE retail_prod.bronze.pos_sales;
```
