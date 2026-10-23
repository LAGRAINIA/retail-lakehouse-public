# 8. Astronomer, Airflow 3 et Cosmos : toutes les fonctionnalités utilisées

Ce document explique **chaque fonctionnalité** utilisée dans le projet : ce qu'elle fait, comment elle
fonctionne en interne, où elle est dans le code, et ce qu'on a **réellement observé** en la mettant en place.
Chaque partie finit par un résumé « à retenir ».

Sommaire :
- [Partie A : Astronomer](#partie-a--astronomer) — CLI, Astro Runtime, projet Astro, Deployments, déploiement, tokens
- [Partie B : Airflow 3](#partie-b--airflow-3) — architecture, TaskFlow, planning, paramètres, Assets, mapping, trigger rules, connexions, API REST
- [Partie C : Cosmos](#partie-c--cosmos-dbt-dans-airflow) — rendu du projet dbt en tâches Airflow
- [Partie D : pièges et améliorations](#partie-d--pièges-identifiés-et-améliorations-possibles)

---

# Partie A : Astronomer

Astronomer est l'éditeur commercial d'Airflow. Deux produits sont utilisés :
- **l'Astro CLI** : outil en ligne de commande pour développer en local et piloter le cloud ;
- **Astro** (Astro Hosted) : Airflow géré dans le cloud, organisé en *Deployments*.

## A1. Le projet Astro (`astro dev init`)

`astro dev init` crée une arborescence standard. Chaque fichier a un rôle précis :

| Fichier / dossier | Rôle | Dans ce projet |
|---|---|---|
| `Dockerfile` | image de l'environnement Airflow, basée sur **Astro Runtime** | ajoute un virtualenv dbt |
| `requirements.txt` | paquets Python installés dans l'image | Cosmos, provider Databricks, SDK Databricks |
| `packages.txt` | paquets système (apt) | vide |
| `dags/` | les DAGs, et tout ce qui doit être livré avec eux | 3 DAGs + le projet dbt (`dags/dbt/retail/`) |
| `include/` | code Python partagé, importable depuis les DAGs (`from include...`) | générateur, ingestion |
| `plugins/` | plugins Airflow | vide |
| `tests/` | tests pytest lancés par `astro dev pytest` | intégrité des DAGs, générateur |
| `.env` | variables d'environnement injectées **en local** dans tous les conteneurs | identifiants Databricks, connexion Airflow |
| `airflow_settings.yaml` | connexions, variables et pools **locaux** | non utilisé (on passe par `.env`) |
| `.astro/` | configuration de la CLI (nom du projet, test d'intégrité par défaut) | généré |

**Pourquoi le projet dbt est dans `dags/` ?** Sur Astro, le dossier `dags/` est livré par les *DAG deploys*
(voir A6) : mettre dbt à côté permet de déployer une modification de modèle sans reconstruire l'image.

## A2. Astro Runtime

`FROM astrocrpublic.azurecr.io/runtime:3.3-9` : l'image officielle d'Astronomer, qui contient Airflow (ici
**3.3.2**), Python (**3.14**), les providers courants, et les réglages d'Astro (logs, métriques, OpenLineage).
Le numéro `3.3-9` = version d'Airflow 3.3, 9e version de l'image.

L'image utilise des instructions **`ONBUILD`** : quand on construit *notre* Dockerfile, elle copie
automatiquement le projet et installe `requirements.txt` et `packages.txt`. Nos lignes `RUN` s'exécutent
**après** : le projet est déjà dans l'image.

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-9
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-databricks==1.12.7"
ENV DBT_EXECUTABLE=/usr/local/airflow/dbt_venv/bin/dbt
```

**Pourquoi un virtualenv pour dbt ?** dbt et Airflow fixent des versions incompatibles de bibliothèques
communes. Installer dbt dans `requirements.txt` finit souvent en conflit de dépendances. Le venv isole dbt ;
Airflow l'appelle comme un programme externe (voir C3, `InvocationMode.SUBPROCESS`).

## A3. L'environnement local (`astro dev ...`)

`astro dev start` lance, avec Docker Compose, **les composants d'Airflow 3** dans des conteneurs séparés :

| Conteneur | Rôle |
|---|---|
| `postgres` | base de métadonnées (runs, états des tâches, connexions, variables…) |
| `db-migration` | crée ou met à jour le schéma de la base, puis s'arrête |
| `scheduler` | décide quoi lancer et quand ; en local, exécute aussi les tâches (LocalExecutor) |
| `dag-processor` | lit et analyse les fichiers de DAG (processus séparé dans Airflow 3) |
| `api-server` | l'interface web et l'API REST |
| `triggerer` | fait tourner les tâches *deferrable* (attente asynchrone) |

`dags/`, `include/`, `plugins/` et `tests/` sont **montés** dans les conteneurs : une modification de DAG est
visible en quelques secondes, sans reconstruire l'image. Le `Dockerfile` ou `requirements.txt` modifiés
demandent un `astro dev restart`.

Les commandes utilisées :

| Commande | Usage dans le projet |
|---|---|
| `astro dev start --wait 10m` | construire l'image et démarrer (avec attente de bonne santé) |
| `astro dev stop` / `astro dev kill` | arrêter / tout supprimer (y compris la base locale) |
| `astro dev run <commande airflow>` | lancer la CLI Airflow dans le conteneur : `dags list`, `dags list-import-errors`, `dags unpause`, `dags trigger -c '{...}'`, `dags list-runs` |
| `astro dev pytest tests` | lancer les tests **dans l'image** (mêmes versions qu'en production) |
| `docker exec <conteneur> ...` | inspecter le code installé (on a lu le code de Cosmos pour comprendre une erreur) |

> **Observé :** `astro dev bash -s -- -c "..."` refuse plusieurs arguments ; `docker exec` sur le conteneur
> `dag-processor` a servi à lire `cosmos/converter.py` et trouver la vraie condition de l'erreur (C3).

## A4. Astro cloud : organisation, workspace, Deployments

```
Organisation « Anass »
└── Workspace « data »
    ├── Deployment retail-uat   (mode développement, hibernation possible)
    └── Deployment retail-prod
```

Un **Deployment** est un Airflow complet et isolé (sa base, son scheduler, ses workers, ses variables). Créé
avec la CLI :

```bash
astro deployment create --name retail-prod \
  --cloud-provider aws --region us-east-1 \
  --runtime-version 3.3-9 \
  --executor CeleryExecutor \
  --scheduler-size small \
  --development-mode disable \
  --workspace-id <id> --force
```

| Option | Ce qu'elle règle | Choix |
|---|---|---|
| `--type standard` (défaut) | infrastructure partagée gérée par Astronomer (vs `dedicated` : cluster isolé) | standard |
| `--cloud-provider` / `--region` | où tourne Airflow | AWS us-east-1 |
| `--runtime-version` | version d'Astro Runtime : **doit correspondre** au `FROM` du Dockerfile | 3.3-9 |
| `--executor` | Celery (workers permanents qui prennent les tâches dans une file), Kubernetes (un pod par tâche), AstroExecutor | Celery |
| `--scheduler-size` | ressources du scheduler | small |
| `--development-mode enable` | autorise l'**hibernation** (mise en veille) ; pas de garantie de disponibilité | uat oui, prod non |
| `--high-availability` | deux schedulers | non |
| `--cicd-enforcement enable` | n'accepter que les déploiements faits avec un token (pas depuis un poste) | non (à activer en entreprise) |

> **Observé :** l'essai gratuit refuse un 3e Deployment (« Payment is required to create more than 2
> deployment »). D'où dev en local.

`astro deployment inspect <id> --key metadata.status` lit un champ précis (`HEALTHY`, `DEPLOYING`…), et
`--key metadata.airflow_api_url` donne l'adresse de l'API REST du Deployment.

## A5. Variables d'environnement et secrets d'un Deployment

Sur Astro, la configuration d'un environnement = ses **variables d'environnement**. Elles sont lues par
Airflow (`AIRFLOW__...`, `AIRFLOW_CONN_...`) et par notre code (`os.getenv`).

```bash
astro deployment variable create -d <id> --load --env uat.plain.env            # variables normales
astro deployment variable create -d <id> --load --env uat.secret.env --secret  # secrets
astro deployment variable list -d <id>                                         # valeurs secrètes masquées
```

- Une variable **secrète** est chiffrée et n'est plus jamais affichée (ni interface, ni CLI, ni API).
- On les a chargées depuis des **fichiers temporaires** générés à partir de `.env`, puis supprimés : le token
  n'apparaît jamais dans une ligne de commande, donc ni dans l'historique du shell ni dans les logs.
- C'est le cœur du principe **« même code, configuration différente »** : l'image de uat et celle de prod sont
  identiques ; seules `DATABRICKS_CATALOG`, `DBT_TARGET`, etc. changent.

## A6. Déployer : `astro deploy`

```bash
astro deploy <deployment-id> --force
```

Avec *DAG deploy* activé (`dag_deploy_enabled: true`, par défaut), un déploiement complet fait **deux choses** :

1. **image** : construit l'image Docker (Dockerfile + requirements), la pousse dans le registre d'Astro, puis
   redémarre progressivement scheduler et workers sur la nouvelle image ;
2. **DAG bundle** : envoie le contenu de `dags/` séparément. Airflow 3 le range dans un *DAG bundle* versionné
   (chemin vu dans les logs : `/tmp/airflow/dag_bundles/astro/dags/main/<horodatage>/dags/...`).

`astro deploy --dags` n'envoie que les DAGs (quelques secondes, sans reconstruire l'image) : idéal pour une
modification de DAG ou de modèle dbt.

> **Observé :** juste après le premier déploiement de uat, les trois DAGs étaient en erreur
> `ModuleNotFoundError: No module named 'cosmos'`. Le **bundle de DAGs était arrivé avant que les workers
> tournent sur la nouvelle image** (celle qui contient Cosmos). L'erreur a disparu seule une fois le
> Deployment passé à `HEALTHY`. À retenir : un déploiement n'est terminé que lorsque l'image est déployée
> partout, et une erreur d'import juste après un déploiement peut être transitoire.

## A7. Les tokens d'API

| Type | Portée | Utilisé pour |
|---|---|---|
| token de **workspace** | tous les Deployments du workspace | — (refusé par l'essai sauf rôle Owner) |
| token de **Deployment** (`DEPLOYMENT_ADMIN`) | un seul Deployment | la CI/CD (`ASTRO_API_TOKEN`) et l'API REST d'Airflow |

```bash
astro deployment token create --deployment-id <id> --name github-actions \
  --role DEPLOYMENT_ADMIN --expiration 90 --clean-output | gh secret set ASTRO_API_TOKEN --env prod
```

- La CLI Astro lit automatiquement `ASTRO_API_TOKEN` : c'est ainsi que GitHub Actions s'authentifie, sans
  `astro login`.
- `--clean-output` n'imprime que le token, pour le passer directement à `gh secret set` (jamais affiché).
- **Moindre privilège** : le token de uat ne peut pas déployer en prod.

> **Observé :** `astro workspace token create --role WORKSPACE_OPERATOR` a échoué (« not entitled to Non-Owner
> Workspace Roles ») ; comme la sortie était envoyée à `gh secret set`, **le secret contenait le message
> d'erreur**. Le déploiement a échoué avec « no context set ». Leçon : vérifier la sortie avant de la stocker
> (le script final contrôle la longueur et l'absence du mot « error »).

**À retenir (Astronomer)**
- Astro Runtime = Airflow + Python + réglages, versionné ; la version du Deployment doit suivre le Dockerfile.
- Image deploy (dépendances) vs DAG deploy (code des DAGs) : ce dernier est rapide et sans redémarrage.
- Un Deployment par environnement, configuré par variables ; secrets marqués *secret*.
- Déploiement par CI avec un token de Deployment, idéalement avec *CI/CD enforcement*.

---

# Partie B : Airflow 3

## B1. L'architecture d'Airflow 3 (ce qui change par rapport à Airflow 2)

- Le **dag-processor** est un composant séparé : le scheduler ne lit plus les fichiers de DAG.
- L'**api-server** remplace le webserver et sert l'API REST **v2** (`/api/v2`).
- Les tâches n'accèdent plus directement à la base : elles passent par l'**Execution API** via le **Task SDK**.
  C'est pourquoi on importe depuis `airflow.sdk` (`dag`, `task`, `Asset`, `Param`, `get_current_context`).
- Les **DAG bundles** versionnent le code des DAGs : un run s'exécute avec la version du code avec laquelle il a
  démarré.
- Les **Datasets** d'Airflow 2 s'appellent désormais **Assets**.
- `logical_date` peut être **nulle** pour un run manuel (on l'a déclenché avec `"logical_date": null`).

## B2. TaskFlow API : `@dag` et `@task`

```python
from airflow.sdk import Asset, Param, dag, get_current_context, task

@dag(dag_id="retail_source_systems", schedule=..., start_date=..., catchup=False, ...)
def retail_source_systems():

    @task(outlets=[LANDING])
    def land_source_files() -> dict:
        context = get_current_context()
        ...
        return summary            # la valeur retournée est stockée en XCom

    ensure_landing_zone >> land_source_files()

retail_source_systems()           # appeler la fonction crée le DAG
```

- `@task` transforme une fonction Python en tâche ; sa valeur de retour devient un **XCom** (petit message
  stocké en base, lisible par les tâches suivantes).
- `get_current_context()` donne le contexte du run (paramètres, intervalle, événements d'Asset…).
- Les opérateurs classiques (`BashOperator`, `DatabricksSqlOperator`) se mélangent librement aux tâches TaskFlow.
- `>>` définit l'ordre ; une liste à gauche ou à droite crée plusieurs dépendances d'un coup.

## B3. Planning : `schedule`, `start_date`, `catchup`, `max_active_runs`

```python
schedule=None if SCHEDULE.lower() == "none" else SCHEDULE,   # "0 1 * * *" : chaque nuit à 1 h
start_date=datetime.combine(C.EPOCH, datetime.min.time()),
catchup=False,
max_active_runs=1,
```

| Paramètre | Effet |
|---|---|
| `schedule` (cron) | un run par intervalle ; le run de l'intervalle [J 01:00, J+1 01:00) s'exécute **à la fin** de l'intervalle |
| `schedule=None` | uniquement manuel (environnement dev) |
| `schedule=[Asset]` | déclenché par la mise à jour d'un Asset (B5) |
| `catchup=False` | à l'activation, **seul le dernier intervalle** est exécuté, pas tous ceux depuis `start_date` |
| `max_active_runs=1` | jamais deux runs du même DAG en parallèle : pas d'écriture concurrente |

> **Observé :** dès l'activation du DAG des sources, Airflow a lancé de lui-même le run
> `scheduled__2026-10-17T01:00` (le dernier intervalle complet), en plus de notre run manuel. C'est le
> comportement normal de `catchup=False`.

Le planning est **piloté par une variable d'environnement** (`RETAIL_SOURCES_SCHEDULE`) : le même fichier donne
un DAG manuel en dev et nocturne en uat et prod.

## B4. Les paramètres (`params`) et le formulaire de déclenchement

```python
params={
    "start_date": Param(None, type=["null", "string"], format="date", description="..."),
    "end_date":   Param(None, type=["null", "string"], format="date"),
    "incidents":  Param([], type="array", items={"type": "string", "enum": sorted(C.INCIDENTS)}),
    "clean":      Param(False, type="boolean"),
}
```

- Les `Param` suivent **JSON Schema** : Airflow valide les valeurs et génère le **formulaire** de
  l'interface (*Trigger*) : sélecteur de date, liste déroulante des incidents, case à cocher.
- `type=["null", "string"]` : valeur facultative.
- Les valeurs arrivent dans `context["params"]`. Par l'API ou la CLI : `conf` (`-c '{"start_date": ...}'`).

## B5. Les Assets : un ordonnancement piloté par les données

```python
LANDING = Asset("dbx://retail/landing/raw")       # une donnée, identifiée par une URI
BRONZE  = Asset("dbx://retail/bronze")

@task(outlets=[LANDING])                          # producteur : cette tâche met à jour LANDING
def land_source_files(): ...

@dag(schedule=[LANDING])                          # consommateur : démarre quand LANDING est mis à jour
def retail_ingest_bronze(): ...
```

**Fonctionnement :** quand une tâche avec `outlets` **réussit**, Airflow enregistre un **événement d'Asset**. Le
scheduler crée alors un run de chaque DAG planifié sur cet Asset (`run_type = asset_triggered`). Plusieurs
événements arrivés avant le démarrage du run sont regroupés dans **un seul** run.

**Transmettre des informations avec l'événement** (`extra`) :

```python
# producteur
context["outlet_events"][LANDING].extra = {"start": "2026-07-01", "end": "2026-10-17", "files": 2102}

# consommateur
for events in context["triggering_asset_events"].values():
    for event in events:
        days.update(landing_days(date.fromisoformat(event.extra["start"]), ...))
```

C'est ce qui permet au DAG bronze de ne charger **que les jours livrés** (voir 04, l'incident des petits
fichiers).

> **Observé (important) :** dans le DAG bronze, les **9 tâches** `copy_into` ont `outlets=[BRONZE]`. La première
> qui finit émet un événement, et le DAG dbt démarre **avant que les 8 autres aient fini**. Au chargement de
> l'historique, le premier run dbt a donc tourné sur un bronze **partiel**. Ce n'était pas grave ici (le run
> suivant a tout rattrapé grâce à l'incrémental sur `_ingested_at`), mais c'est un vrai défaut de conception.
> Correction possible : une seule tâche finale `bronze_ready` qui porte l'`outlet` (voir partie D).

## B6. Le *dynamic task mapping*

```python
@task
def plan_loads() -> list[list[str]]:
    return [copy_into_statements(source, days) for source in SOURCES]     # 9 listes de SQL

loads = DatabricksSqlOperator.partial(
    task_id="copy_into",
    databricks_conn_id=CONN_ID,
    max_active_tis_per_dagrun=3,
    map_index_template="{{ task.sql[0].split('.')[-1] }}",
    outlets=[BRONZE],
).expand(sql=plan_loads())
```

- `partial(...)` fixe les arguments communs, `expand(sql=...)` crée **une instance de tâche par élément** de la
  liste, **au moment de l'exécution** (le nombre peut changer d'un run à l'autre).
- `map_index_template` : nom affiché de chaque instance (ici le nom de la table) au lieu de `0, 1, 2…`.
- `max_active_tis_per_dagrun=3` : au plus 3 instances en parallèle dans un run, pour ménager le warehouse.
- Chaque instance se relance **indépendamment** : un flux en échec n'oblige pas à recharger les 8 autres.

## B7. Les trigger rules et le pattern « watcher »

Par défaut (`all_success`), une tâche tourne si tous ses parents ont réussi. Le DAG dbt ajoute :

```python
@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException("At least one task failed: the run must be marked as failed.")

[dbt_deps, source_freshness, dbt, gdpr_purge_bronze] >> watcher()
```

- **Règle d'Airflow :** l'état d'un run est calculé sur ses **tâches feuilles**.
- Le watcher est une feuille qui ne tourne **que si un parent a échoué**, et échoue alors : le run devient rouge.
  Si tout va bien, il est `skipped`, et le run reste vert.
- Sans lui, une tâche finale en `all_done` (nettoyage) peut rendre un run **vert malgré un échec**.
- Alternative native : les tâches **setup / teardown** (`as_teardown()`), ignorées dans le calcul de l'état.

## B8. Reprises et arguments par défaut

```python
default_args={"retries": 2, "retry_delay": timedelta(minutes=2)}
```

Toute tâche est relancée automatiquement en cas d'échec **transitoire** (réseau, warehouse qui démarre). Le
watcher a `retries=0` : relancer un signal d'échec n'a pas de sens. Un test d'intégrité vérifie que chaque tâche
a une reprise.

> **Observé :** la tâche des sources en uat est passée à `try_number = 3` avant d'échouer : les reprises
> fonctionnent, mais elles ne servent à rien contre une erreur **permanente** (catalogue absent). Distinguer
> erreurs transitoires et permanentes, c'est savoir quand relancer et quand corriger.

## B9. Les providers et les opérateurs

| Opérateur | Provider | Usage |
|---|---|---|
| `DatabricksSqlOperator` | `apache-airflow-providers-databricks` | exécute du SQL sur un SQL warehouse (création du catalogue, `COPY INTO`) |
| `BashOperator` | `apache-airflow-providers-standard` | commandes dbt hors Cosmos (`deps`, `source freshness`, `run-operation`) |
| `@task` (PythonOperator) | standard | générer et déposer les fichiers, préparer le SQL |

Dans Airflow 3, même les opérateurs « de base » (Bash, Python) sont dans le provider **standard**
(`airflow.providers.standard.operators.bash`).

## B10. Les connexions par variable d'environnement

```
AIRFLOW_CONN_DATABRICKS_DEFAULT='{"conn_type": "databricks", "host": "dbc-....cloud.databricks.com",
                                  "password": "<token>", "extra": {"http_path": "/sql/1.0/warehouses/<id>"}}'
```

- Toute variable `AIRFLOW_CONN_<ID>` définit la connexion `<id>` (en minuscules), au format JSON ou URI.
- Avantages : rien en base, même mécanisme en local (`.env`) et sur Astro (variable secrète), versionnable
  sans le secret.
- `scripts/sync_airflow_connection.py` la construit à partir des `DATABRICKS_*` : on ne tape le token qu'une fois.
- En entreprise : un **secrets backend** (AWS Secrets Manager, Vault) à la place.

## B11. L'API REST v2

Utilisée pour piloter les Deployments depuis le poste, avec un token de Deployment :

| Appel | Usage |
|---|---|
| `GET /api/v2/importErrors` | DAGs en erreur d'import |
| `GET /api/v2/dags?dag_id_pattern=retail` | liste des DAGs et état (pause) |
| `PATCH /api/v2/dags/{dag_id}` `{"is_paused": false}` | activer un DAG |
| `POST /api/v2/dags/{dag_id}/dagRuns` `{"logical_date": null, "conf": {...}, "note": "..."}` | déclencher avec paramètres |
| `GET /api/v2/dags/{dag_id}/dagRuns?order_by=-start_date` | suivre les runs |
| `POST /api/v2/dags/{dag_id}/dagRuns/{run_id}/clear` | relancer un run en échec |
| `GET .../taskInstances/{task_id}/logs/{try}` | lire le log d'une tentative |

En-tête : `Authorization: Bearer <token>`. Le `run_id` contient `:` et `+` : il faut l'**encoder** dans l'URL.

## B12. Les tests de DAG et la base de métadonnées

- `DagBag(dags_folder)` charge les DAGs comme le fait le dag-processor ; `import_errors` liste les erreurs.
- `tests/conftest.py` met `AIRFLOW__COSMOS__ENABLE_CACHE=False` : Cosmos cache son rendu dans des **Variables
  Airflow**, qui demandent une base absente du conteneur de test.
- Pour déboguer en local, on peut lire directement la base Postgres (`dag_run`, `task_instance`) :
  ```sql
  select dag_id, run_id, state, start_date, end_date from dag_run order by start_date;
  ```

## B13. OpenLineage (vu dans les logs)

Les logs des tâches sur Astro contiennent « Successfully emitted OpenLineage `START` / `FAIL` event ». Astro
Runtime intègre **OpenLineage** : chaque tâche émet des événements de lignage (début, fin, entrées, sorties),
exploités par l'interface de lignage d'Astro ou un outil comme Marquez. On n'a rien configuré : c'est fourni.

**À retenir (Airflow)**
- Airflow 3 : dag-processor séparé, api-server, Task SDK, DAG bundles, Assets.
- `catchup=False` exécute quand même le dernier intervalle à l'activation.
- Assets + `extra` : un pipeline piloté par les données, qui transporte « quoi » est arrivé.
- Mapping dynamique : une tâche par élément, relance indépendante, parallélisme borné.
- L'état d'un run vient de ses feuilles : watcher ou setup/teardown.

---

# Partie C : Cosmos (dbt dans Airflow)

**Astronomer Cosmos** transforme un projet dbt en tâches Airflow : chaque modèle, seed et snapshot devient une
tâche, avec les dépendances du DAG dbt.

## C1. Les quatre objets de configuration

```python
DbtTaskGroup(
    group_id="dbt",
    project_config=ProjectConfig(DBT_PROJECT, install_dbt_deps=True),
    profile_config=ProfileConfig(profile_name="retail", target_name=DBT_TARGET,
                                 profiles_yml_filepath=DBT_PROJECT / "profiles.yml"),
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE,
                                     invocation_mode=InvocationMode.SUBPROCESS),
    render_config=RenderConfig(load_method=LoadMode.DBT_LS,
                               test_behavior=TestBehavior.AFTER_EACH,
                               should_detach_multiple_parents_tests=True,
                               dbt_executable_path=DBT_EXECUTABLE,
                               invocation_mode=InvocationMode.SUBPROCESS),
)
```

| Objet | Répond à la question |
|---|---|
| `ProjectConfig` | **où** est le projet dbt, faut-il installer les packages ? |
| `ProfileConfig` | **avec quelles** connexion et target dbt se connecter ? |
| `ExecutionConfig` | **comment exécuter** dbt pendant les tâches ? |
| `RenderConfig` | **comment construire** le graphe de tâches au chargement du DAG ? |

`DbtTaskGroup` insère le projet comme un **groupe** dans un DAG existant (on l'entoure de nos tâches) ;
`DbtDag` créerait un DAG entier.

## C2. Le rendu : du projet dbt aux tâches

Au **chargement du DAG** (dans le dag-processor), Cosmos lit le graphe dbt :

| `load_method` | Comment | Coût |
|---|---|---|
| `DBT_LS` (utilisé) | lance `dbt ls` et lit le résultat | lent ; mis en cache dans des Variables Airflow |
| `DBT_MANIFEST` | lit un `manifest.json` déjà produit (par la CI) | rapide, recommandé en production |
| `AUTOMATIC` | manifest s'il existe, sinon `dbt ls` | — |

Puis il crée les tâches : `<modèle>.run` suivie de `<modèle>.test` (avec `AFTER_EACH`), une tâche par seed et par
snapshot. Dans ce projet : environ **60 tâches** pour 27 modèles, 1 seed, 1 snapshot et leurs tests.

| `test_behavior` | Effet |
|---|---|
| `AFTER_EACH` (utilisé) | les tests d'un modèle juste après lui : un test en échec **bloque l'aval** |
| `AFTER_ALL` | tous les tests à la fin |
| `BUILD` | une tâche `dbt build` par modèle (run + test ensemble) |
| `NONE` | pas de tests |

`should_detach_multiple_parents_tests=True` : un test qui dépend de **plusieurs** modèles (relation, test
singulier de réconciliation) devient une tâche à part, lancée après **tous** ses parents. Sinon, il tournerait
après le premier parent, sur des données incomplètes.

## C3. L'exécution : `ExecutionMode` et `InvocationMode`

- **ExecutionMode** (où tourne dbt) : `LOCAL` par défaut (dans le worker Airflow, utilisé ici) ; aussi
  `VIRTUALENV`, `DOCKER`, `KUBERNETES` (un pod par tâche)…
- **InvocationMode** (comment Cosmos appelle dbt en mode LOCAL) :
  - `DBT_RUNNER` : dbt importé comme bibliothèque Python, dans le même processus (plus rapide) ;
  - `SUBPROCESS` : dbt lancé comme programme externe (le binaire du venv).

> **Observé :** `CosmosValueError: RenderConfig.dbt_executable_path is set, but it is not the same as the system
> dbt executable path. Do not set render_config.dbt_executable_path when using InvocationMode.DBT_RUNNER.`
> Cosmos 1.15 choisit `DBT_RUNNER` par défaut, or dbt n'est pas importable depuis l'environnement d'Airflow (il
> est dans le venv). Mettre `SUBPROCESS` dans `ExecutionConfig` **n'a pas suffi** : la vérification qui
> échouait portait sur `RenderConfig`. Il faut l'option **dans les deux**. On l'a trouvé en lisant le code de
> `cosmos/converter.py` dans le conteneur.

## C4. Le profil : fichier ou *profile mapping*

Deux façons de donner les identifiants à dbt :
- **`profiles_yml_filepath`** (utilisé) : notre `profiles.yml`, qui lit des variables d'environnement. Même
  fichier en local et dans le cloud, testable avec la CLI dbt.
- **profile mapping** (`DatabricksTokenProfileMapping(conn_id=...)`) : Cosmos fabrique le profil à partir d'une
  **connexion Airflow**. Avantage : un seul endroit pour le secret (la connexion).

## C5. `install_dbt_deps` et le répertoire temporaire

Pour chaque tâche, Cosmos **copie le projet dans un répertoire temporaire** (pour ne pas écrire `target/` dans le
dossier des DAGs, en lecture seule sur Astro), puis lance dbt. Avec `install_dbt_deps=True`, il y installe les
packages (`dbt_utils`) avant chaque commande. Coût : quelques secondes par tâche. Optimisation : livrer
`dbt_packages/` dans l'image ou le bundle et mettre `False`.

**À retenir (Cosmos)**
- Un modèle = une tâche : visibilité, relance ciblée, tests bloquants.
- `DBT_MANIFEST` en production pour un chargement de DAG rapide.
- dbt dans un venv séparé → `InvocationMode.SUBPROCESS` (render **et** execution).
- `AFTER_EACH` + `should_detach_multiple_parents_tests` pour des tests justes et bloquants.

---

# Partie D : pièges identifiés et améliorations possibles

Ce sont de **bonnes premières évolutions** : chacune corrige un vrai défaut, à faire sur `dev` puis à promouvoir.

| # | Constat | Amélioration | Fonctionnalités en jeu |
|---|---|---|---|
| D1 | Le DAG dbt démarre dès la **première** des 9 tâches bronze terminée (bronze partiel) | une tâche finale `bronze_ready` après le mapping porte seule l'`outlet` | Assets, dépendances après mapping |
| D2 | Run manuel **sans paramètres** : `data_interval_start` peut être vide dans Airflow 3 (run sans `logical_date`) → `land_source_files` planterait. **Non testé** : à vérifier | repli sur `dag_run.run_after` moins un jour | contexte d'exécution Airflow 3 |
| D3 | `LoadMode.DBT_LS` : `dbt ls` au chargement du DAG | la CI produit `manifest.json`, Cosmos le lit (`DBT_MANIFEST`) | Cosmos, CI |
| D4 | `install_dbt_deps=True` : packages réinstallés à chaque tâche | `dbt deps` à la construction, `install_dbt_deps=False` | Cosmos, Dockerfile |
| D5 | Aucune alerte : un échec n'est vu qu'en ouvrant Airflow | `on_failure_callback` (Slack, email) ou les **alertes Astro** | callbacks, Astro alerts |
| D6 | Pas de garde-fou de durée | `execution_timeout` par tâche, *Deadline Alerts* d'Airflow 3 | robustesse |
| D7 | Le warehouse est partagé par uat et prod (le dbt a pris 15 min au lieu de 4) | un **pool** Airflow par environnement, ou des horaires décalés | pools, planning |
| D8 | Les déploiements depuis un poste restent possibles | `--cicd-enforcement enable` sur les Deployments | Astro |
| D9 | Le DAG des sources fait génération **et** envoi dans une tâche | deux tâches avec un dossier partagé (ou objet) ; ou l'envoi en *deferrable* | conception de tâches |
