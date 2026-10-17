# 3. Les systèmes sources

Le générateur (`include/retail_generator/`) simule six systèmes qui déposent chaque jour leurs fichiers dans le
volume `<catalogue>.landing.raw`. Il est **déterministe** : un jour donné produit toujours exactement les mêmes
fichiers (graine aléatoire calculée à partir de la date et des identifiants). Rejouer un jour, c'est renvoyer
les mêmes fichiers, comme le ferait un vrai système source.

## 3.1 Utilisation

```bash
python -m include.retail_generator generate --start 2026-07-01 --end 2026-07-31   # une période
python -m include.retail_generator generate --start 2026-10-18 --incident price_x100
python -m include.retail_generator generate --start 2026-10-18 --clean           # sans défauts
python -m include.retail_generator incidents                                      # liste des incidents
python -m include.retail_generator upload --glob "**/*20261018*"                  # vers le volume
```

Dans Airflow : DAG `retail_source_systems`, paramètres `start_date`, `end_date`, `incidents`, `clean`.

## 3.2 Les flux

Chemins relatifs au volume. `AAAAMMJJ` = date dans le nom de fichier (jour de dépôt, sauf POS : jour métier).

| Système | Fichier | Format | Fréquence | Volume / jour |
|---|---|---|---|---|
| ERP | `erp/stores/stores_AAAAMMJJ.csv` | CSV, extrait complet | quotidien | 11-12 lignes |
| ERP | `erp/products/products_AAAAMMJJ.csv` | CSV, extrait complet | quotidien | ~270 lignes |
| CRM | `crm/customers/customers_AAAAMMJJ.json` | JSON lines, flux de changements | quotidien | 30-60 lignes (6 000 le 1er jour) |
| CRM | `crm/gdpr_requests/gdpr_requests_AAAAMMJJ.csv` | CSV | quotidien | 0 à 3 lignes |
| Caisses | `pos/sales/pos_sales_<magasin>_AAAAMMJJ.csv` | CSV, **un fichier par magasin** | quotidien | ~2 400 lignes au total |
| E-commerce | `ecom/orders/orders_AAAAMMJJ.json` | JSON lines, CDC, lignes imbriquées | quotidien | ~2 500 événements |
| E-commerce | `ecom/returns/returns_AAAAMMJJ.csv` | CSV | quotidien | ~60 lignes |
| Stocks | `supply/inventory/inventory_AAAAMMJJ.csv` | CSV, photo de fin de journée | quotidien | ~3 100 lignes |
| Finance | `finance/fx_rates/fx_rates_AAAAMMJJ.json` | JSON | **jours ouvrés** | 1 ligne |

### Caisses (POS) : `pos_sales_<magasin>_<jour>.csv`

Une ligne = une ligne de ticket. Heure **locale** du magasin, montants en **devise locale**.

| Colonne | Exemple | Remarque |
|---|---|---|
| `transaction_id` | `ST001-20260701-00001` | ticket |
| `line_number` | `1` | ligne du ticket |
| `store_id` | `ST001` | parfois vide (défaut A4) |
| `transaction_type` | `SALE` / `RETURN` | un retour a une quantité négative |
| `sku` | `SKU100048` | parfois inconnu (`SKU999999`) |
| `quantity` | `1` | parfois 0 (erreur de scan) |
| `unit_price` | `558.99` | parfois négatif (erreur de signe) |
| `discount_amount` | `0.00` | |
| `currency` | `EUR` / `GBP` / `CHF` | |
| `transaction_ts` | v1 : `2026-07-01 14:04:29` ; v2 : `2026-09-05T14:58:03+01:00` | **change de format** à J+60 |
| `payment_method` | `card` / `cash` / `mobile` | |
| `customer_id` | `C0002994` ou vide | carte de fidélité, 45 % des tickets |
| `cashier_id` | `ST001-K11` | |
| `loyalty_points` | `558` | **n'existe qu'à partir de J+60** (logiciel v2) |

### Commandes web : `orders_<jour>.json`

Une ligne = **un instantané complet de la commande à un changement de statut** (CDC). Une commande apparaît
donc plusieurs fois : `CREATED` → `PAID` → `SHIPPED` → `DELIVERED`, ou `CREATED` → `CANCELLED` (3 %).

```json
{"order_id": "W2026092000001", "customer_id": "C0000350", "order_status": "PAID",
 "created_at": "2026-09-20T02:03:19Z", "updated_at": "2026-09-20T02:06:19Z",
 "country": "ES", "currency": "EUR", "promo_code": "SPORT20",
 "shipping": {"method": "home", "pickup_store_id": null, "cost": 4.99},
 "lines": [{"line_id": "W2026092000001-1", "sku": "SKU100084", "quantity": 1, "unit_price": 14.99}],
 "device": "ios"}
```

- Heures en **UTC** (suffixe `Z`), à la différence des caisses.
- `customer_id` est `null` pour 25 % des commandes (commande invité).
- `pickup_store_id` n'est renseigné qu'en click & collect.
- `device` n'existe qu'à partir de J+75.
- 4 % des événements arrivent **1 à 3 jours en retard** (dans le fichier d'un jour ultérieur).

### Retours web : `returns_<jour>.csv`
`return_id, order_id, line_id, sku, quantity, reason, requested_at, return_channel, refund_amount, currency`.
Environ 9 % des commandes livrées, entre 2 et 25 jours après la livraison. Motifs : `wrong_size`, `defective`,
`changed_mind`, `not_as_described`, `late_delivery`.

### Produits : `products_<jour>.csv`
`sku, product_name, sport, category, brand, unit_price_eur, unit_cost_eur, status, launch_date, last_modified`.
Extrait **complet** chaque jour. Le prix change : promotions hebdomadaires (-20 %, 4 % des produits par
semaine), hausse de 5 % sur 20 % du catalogue à J+30, produits lancés en cours de période, quelques produits
arrêtés (`discontinued`).

### Magasins : `stores_<jour>.csv`
`store_id, store_name, city, country, timezone, currency, opening_date, status, surface_m2, updated_at`.
Toulouse (ST012) apparaît 14 jours avant son ouverture avec le statut `planned`, puis passe à `open` à J+45.

### Clients : `customers_<jour>.json`
Flux de changements : `op` vaut `I` (création) ou `U` (mise à jour, image complète de la ligne).
`customer_id, first_name, last_name, email, birth_year, country, city, postal_code, loyalty_tier,
marketing_opt_in, signup_date, updated_at`. Le premier jour contient le chargement initial (6 000 clients).
**Données personnelles** : nom, email, ville, code postal, année de naissance.

### Demandes RGPD : `gdpr_requests_<jour>.csv`
`request_id, customer_id, request_type (erasure), requested_at`. 0 à 3 par jour.

### Stocks : `inventory_<jour>.csv`
`snapshot_date, store_id, sku, quantity_on_hand, quantity_reserved, extracted_at`. 2 % de ruptures (0),
quelques stocks **négatifs** (erreur de saisie).

### Taux de change : `fx_rates_<jour>.json`
```json
{"base_currency": "EUR", "rate_date": "2026-07-01", "rates": {"GBP": 0.8573, "CHF": 0.9382, "USD": 1.0877},
 "published_at": "2026-07-01T14:15:00Z"}
```
« 1 EUR = x ». **Pas de fichier le week-end** : les ventes du samedi et du dimanche doivent utiliser le dernier
taux publié.

## 3.3 Les défauts intégrés (désactivables avec `--clean`)

| Défaut | Fréquence | Flux |
|---|---|---|
| fichier renvoyé (`_resend`) | 2 % des fichiers magasin-jour | POS |
| ligne exportée deux fois | 0,2 % | POS |
| fichier arrivé un jour en retard | 1,5 % des fichiers magasin-jour | POS |
| produit inconnu | 0,3 % | POS |
| quantité 0 | 0,2 % | POS |
| prix négatif | 0,1 % | POS |
| `store_id` vide | 0,1 % | POS |
| événement en retard (1-3 jours) | 4 % | commandes web |
| commande invité (client NULL) | 25 % | commandes web |
| stock négatif | 0,05 % | stocks |
| pas de taux le week-end | systématique | finance |

## 3.4 Les événements datés

Définis dans `include/retail_generator/config.py`, relativement à `RETAIL_EPOCH` (1er juillet 2026 par défaut).

| Date | Événement |
|---|---|
| J+30 (31 juillet) | hausse de prix de 5 % sur 20 % du catalogue |
| J+31 (1er août) | ST012 Toulouse apparaît dans l'ERP (`planned`) |
| J+45 (15 août) | ST012 ouvre |
| J+60 (30 août) | logiciel de caisse v2 : `loyalty_points` + horodatage ISO avec fuseau |
| J+75 (14 septembre) | le JSON web gagne `device` |

## 3.5 Les incidents injectables

| Incident | Effet | Ce que ça teste |
|---|---|---|
| `price_x100` | tous les prix de l'extrait produits × 100 | test bloquant `accepted_range`, arrêt de l'aval par Cosmos |
| `pos_outage` | aucun fichier de caisse ce jour-là | pipeline vert mais données manquantes : test de complétude |
| `schema_break` | la colonne `quantity` devient `qty` | dérive de schéma non prévue, `not_null` bloquant |
| `duplicate_batch` | tous les fichiers de caisse envoyés deux fois | dédoublonnage |
| `guest_checkout_flood` | 80 % des commandes du jour sans client | skew de jointure, seuils d'alerte |
| `empty_pos_file` | ST003 envoie un fichier vide | détection d'une chute de volume |

Exercices détaillés : [SCENARIOS.md](SCENARIOS.md).
