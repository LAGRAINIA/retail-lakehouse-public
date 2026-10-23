# Scénarios d'entreprise

Chaque scénario reproduit une situation réelle. Pour chacun : **comment le déclencher**, **ce qu'il faut
observer**, **comment le résoudre**. Essaie de résoudre seul avant de lire la solution.

Les incidents s'injectent sur un jour donné, soit en ligne de commande :

```bash
python -m include.retail_generator generate --start 2026-10-17 --incident price_x100
python -m include.retail_generator upload --glob "**/*20261017*"
```

soit dans Airflow : DAG `retail_source_systems` → **Trigger** avec les paramètres `start_date` et `incidents`.

---

## Partie A — Défauts présents en permanence dans les données

Ils existent déjà dans l'historique, le pipeline doit les gérer sans intervention.

| # | Défaut | Où le voir | Traité par |
|---|---|---|---|
| A1 | Fichiers de caisse renvoyés (`_resend`) | `bronze.pos_sales`, `_source_file` | dédoublonnage dans `stg_pos__sales_lines` |
| A2 | Lignes exportées deux fois | idem | même `qualify row_number()` |
| A3 | Fichier d'un magasin arrivé avec un jour de retard | `_source_file` d'une date dans le lot du lendemain | incrémental sur `_ingested_at` (pas sur la date métier) |
| A4 | `store_id` vide | `is_store_id_repaired = true` | réparé depuis le nom du fichier |
| A5 | Produit inconnu, quantité 0, prix négatif | `quality.dq_rejected_sales_lines` | quarantaine : exclus du fait, mais visibles |
| A6 | Événements web arrivés 1 à 3 jours en retard | `late_event_count` dans `mart_web_fulfilment` | l'ordre suit `updated_at`, pas l'arrivée |
| A7 | Commandes sans client (guest checkout, 25 %) | `is_guest_checkout` | `customer_id` NULL accepté, relation en `warn` |
| A8 | Stock négatif | test `warn` sur `stg_supply__inventory` | alerte sans bloquer |
| A9 | Taux de change absents le week-end | `is_carried_forward` dans `int_fx_rates_daily` | dernier taux connu reporté |
| A10 | Demandes RGPD | `int_customers_current.is_erased` | anonymisation en silver + purge du bronze (`purge_erased_customers`) |

**Exercices**
1. Combien de lignes ont été dédoublonnées ? Compare `count(*)` de `bronze.pos_sales` et de `stg_pos__sales_lines`.
2. Quelle part du chiffre d'affaires est en quarantaine, par motif ?
3. Pourquoi l'incrémental de `fct_sales_lines` filtre-t-il sur `_ingested_at` et pas sur `sale_date` ? Que se
   passerait-il pour un fichier arrivé en retard (A3) avec un filtre sur `sale_date` ?

---

## Partie B — Événements métier datés (dans l'historique)

| # | Date (depuis `RETAIL_EPOCH`) | Événement | Ce qu'il teste |
|---|---|---|---|
| B1 | J+30 | Hausse de prix de 5 % sur 20 % du catalogue | **SCD2** : `int_products_scd2`, marge calculée avec le coût de la bonne version |
| B2 | J+31 | Le magasin ST012 Toulouse apparaît en statut `planned` | évolution d'une dimension |
| B3 | J+45 | ST012 ouvre | `snap_stores` (snapshot dbt), test de complétude |
| B4 | J+60 | **Logiciel de caisse v2** : nouvelle colonne `loyalty_points`, horodatage ISO avec fuseau | **dérive de schéma** : `mergeSchema` au bronze, macro `column_or_null`, conversion UTC |
| B5 | J+75 | Le JSON web gagne un champ `device` | même mécanisme côté JSON |

**Exercices**
1. Un produit a changé de prix : retrouve ses versions dans `dim_product`, et vérifie qu'une vente d'avant
   la hausse pointe vers l'ancienne version.
2. Pourquoi `int_products_scd2` reconstruit l'historique à partir des extraits, au lieu d'un snapshot dbt ?
   (Indice : charge 3 mois d'un coup, puis compare avec `snap_stores`.)
3. Une vente à 23 h 30 heure de Londres le 30 septembre : quelle `sale_date`, quel `sold_at_utc` ?

---

## Partie C — Incidents à injecter

### C1. `price_x100` : bug d'export de l'ERP
**Symptôme :** tous les prix du jour sont multipliés par 100.
**Observer :** le test `accepted_range` de `stg_erp__products` échoue, et avec Cosmos, **tout ce qui dépend des
produits s'arrête** (aucun prix faux n'atteint le gold).
**Résoudre :** c'est la source qui est fausse. On ne corrige pas les données à la main : on demande un nouvel
extrait. Simulation : regénère le jour **sans** incident, renvoie-le, recharge. Le nouveau fichier a le même nom :
`COPY INTO` ne le recharge pas (déjà chargé) ! Comment faire ? (`COPY_OPTIONS ('force' = 'true')` sur ce fichier,
puis le dédoublonnage par `extract_date` garde la dernière ingestion.)

### C2. `pos_outage` : panne de l'intégration caisse
**Symptôme :** aucun fichier de caisse ce jour-là. Le pipeline est vert !
**Observer :** `assert_every_open_store_reported` passe en `warn` le lendemain, le dashboard magasins montre un
trou.
**Résoudre :** relancer l'extraction du jour (re-run de la tâche `land_source_files` pour cette date, sans
incident). **Pour aller plus loin :** le run était vert alors que des données manquaient. Comment le rendre rouge ?

### C3. `schema_break` : la colonne `quantity` devient `qty`
**Symptôme :** le bronze absorbe la nouvelle colonne (`mergeSchema`), `quantity` est NULL pour ce jour.
**Observer :** le test `not_null` sur `stg_pos__sales_lines.quantity` bloque.
**Résoudre :** `coalesce(quantity, qty)` avec `column_or_null`, et ouvrir un ticket à l'équipe caisse.
Discute : faut-il plutôt **refuser** les fichiers au schéma inattendu dès le bronze ?

### C4. `duplicate_batch` : le lot entier est renvoyé
**Observer :** le bronze double ; silver et gold restent justes (dédoublonnage).
**Exercice :** mesure le coût. Le dédoublonnage tourne sur tout l'historique à chaque run : comment le limiter ?

### C5. `guest_checkout_flood` : 80 % des commandes sans client
**Symptôme :** énormément de `customer_id` NULL.
**Observer :** les jointures sur `customer_id` concentrent toutes les lignes NULL sur une seule tâche (**skew**).
Ouvre le Query Profile d'un modèle qui joint les clients.
**Résoudre :** filtrer les NULL avant la jointure, et alerter sur le taux de guests (test sur un seuil).

### C6. `empty_pos_file` : fichier vide
**Symptôme :** ST003 envoie un fichier avec l'en-tête seulement.
**Observer :** le pipeline passe, le test de complétude avertit.
**Exercice :** écris un test qui alerte quand le volume d'un magasin chute de plus de 80 % par rapport à sa
moyenne des 4 mêmes jours de semaine précédents.

---

## Partie D — Situations d'équipe

1. **Backfill** : regénère et recharge tout septembre. `fct_sales_lines` est-il correct ? (le test
   `assert_fct_sales_reconciles_with_silver` doit passer).
2. **Full refresh accidentel** : que se passe-t-il avec `dbt build --full-refresh` ? Protège `fct_sales_lines`
   avec `full_refresh: false` et réessaie.
3. **Restaurer une table** : supprime des lignes de `gold.fct_sales_lines`, puis restaure-la avec
   `DESCRIBE HISTORY` et `RESTORE TABLE ... TO VERSION AS OF`.
4. **Nouvelle demande métier** : « taux de retour par sport et par pays, web et magasin ». Où le construire ?
   Quel grain ? Quels tests ?
5. **Performance** : quel modèle est le plus lent (`target/run_results.json`) ? Ouvre son Query Profile.
   Que changerais-tu avant d'agrandir le warehouse ?
6. **Incident vécu en construisant ce projet : le `COPY INTO` qui ne finit jamais.** Le premier chargement de
   `pos_sales` visait le dossier entier (~1 300 petits fichiers CSV) avec `mergeSchema`. La requête est restée
   25 minutes en **compilation** (`read_files = 0`) : pour fusionner les schémas, Databricks lit l'en-tête de
   chaque fichier avant de démarrer. Un jour (11 fichiers) passait en 20 s.
   - **Diagnostic :** `system.query.history` (statut, durée, fichiers lus) et l'API Query History pour les
     requêtes en cours (seule la compilation avait démarré).
   - **Correction :** `PATTERN` sur les dates livrées, par paquets de 5 jours (`include/ingestion/bronze.py`),
     dates transmises par l'événement d'Asset Airflow. Résultat : 250 000 lignes en 200 s.
   - **À retenir :** beaucoup de petits fichiers + inférence de schéma = coût par fichier.
     Autres solutions : un schéma explicite, Auto Loader (qui garde l'état et le schéma), ou des fichiers plus
     gros côté producteur.
