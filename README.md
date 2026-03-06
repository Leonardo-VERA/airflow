# Projet Apache Airflow - TPs

**Nom :** VERA
**Prénom :** Ray Leonardo
**Formation :** Master Data Engineer — École DSP Paris

---

## Description

Ce projet regroupe les 4 TPs du cours Apache Airflow. Chaque TP est un DAG (ou ensemble de DAGs) illustrant des concepts progressifs d'orchestration de pipelines de données.

## Infrastructure

L'environnement tourne via **Docker Compose** avec les services suivants :

- **Airflow 3.1.7** (API Server, Scheduler, DAG Processor, Worker Celery, Triggerer)
- **PostgreSQL 16** — base de données backend + stockage des résultats
- **Redis 7.2** — broker de messages Celery

### Lancement

```bash
docker compose up -d
```

L'interface web Airflow est accessible sur **http://localhost:8080** (login : `airflow` / `airflow`).

### Packages additionnels

Définis dans `.env` : `pandas` et `apache-airflow-providers-postgres`.

---

## TP1 — Pipeline ETL basique

**Fichier :** `dags/tp_1/etlpipeline.py`
**DAG :** `etl_pipeline_example`

**Objectif :** Créer un pipeline ETL simple avec un `PythonOperator`.

**Ce qu'il fait :**
- Lit le fichier CSV `data/bdd_tp_1/sales_data.csv` (données de ventes)
- Supprime les doublons
- Calcule une colonne `profit_margin` : `(revenue - cost) / revenue`
- Écrit le résultat nettoyé

**Résultat :** `output/results_tp_1/result.csv` — 3 produits avec leur marge de profit.

**Comment voir le résultat :** Déclencher le DAG manuellement dans l'UI Airflow, puis consulter le fichier CSV de sortie.

---

## TP2 — XCom et intégration de données

### Exo 2 — XCom basique
**Fichier :** `dags/tp_2/exo_2_values.py` | **DAG :** `values_xcom_pipeline`

- Lit `data/bdd_tp_2/values_data.csv`, calcule la somme de `column1`
- Passe la valeur entre 3 tâches via **XCom** (`extraction → communication → affichage`)
- Résultat visible dans les **logs de la tâche `affichage`** dans l'UI Airflow

### Exo 3 — Fusion CSV + PostgreSQL
**Fichier :** `dags/tp_2/exo_3_orders.py` | **DAG :** `customer_orders_pipeline`

- Charge `orders.csv` dans PostgreSQL
- Extrait les clients depuis `customers_data.csv` et les commandes depuis PostgreSQL
- Fusionne les deux sources et calcule un `discount` (10% du montant)
- **Résultat :** `output/results_tp_2/customer_orders.csv` + table PostgreSQL `customer_orders`

### Exo 4 — XCom par chemins de fichiers
**Fichier :** `dags/tp_2/exo_4_suite_orders.py` | **DAG :** `customer_orders_xcom_paths`

- Même logique que l'exo 3, mais les XCom transmettent des **chemins de fichiers temporaires** au lieu du contenu JSON
- Calcule un `total_amount`
- **Résultat :** `output/results_tp_2/customer_orders_v2.csv` + table PostgreSQL `customer_orders_v2`

### Weather DAG — Données météo
**Fichier :** `dags/tp_2/weather_dag.py` | **DAG :** `weather_pipeline`

- Lit `data/bdd_tp_2/weather.csv` (200 enregistrements météo)
- Remplit les valeurs manquantes (forward fill)
- Calcule une température ressentie (`feels_like_temp`)
- **Résultat :** `output/results_tp_2/weather_result.csv`

---

## TP3 — ETL SQL avec PostgreSQL

**Fichier :** `dags/tp_3/exo_meteo.py`
**DAG :** `tp_postgres_etl_pipeline`

**Objectif :** Pipeline ETL entièrement en SQL, sans code Python.

**Ce qu'il fait :**
- Crée une table `weather_data` et insère 5 enregistrements météo
- Transforme les données dans une table `weather_summary` avec catégorisation :
  - `> 28°C` → "Hot"
  - `20–28°C` → "Warm"
  - `< 20°C` → "Cold"
- Utilise uniquement des `SQLExecuteQueryOperator`

**Résultat :** Table PostgreSQL `weather_summary`. Visible dans les **logs de la tâche `select_weather_summary`** dans l'UI Airflow.

---

## TP4 — Concepts avancés d'Airflow

### Exo 1 — Branching
**Fichier :** `dags/tp_4/branching.py` | **DAG :** `branching_example`

- Utilise `BranchPythonOperator` pour choisir une tâche selon l'heure (avant/après 13h, fuseau Paris)
- `task_A` s'exécute le matin, `task_B` l'après-midi
- **Résultat :** Visible dans les logs et le graph du DAG (la branche non choisie est skippée)

### Exo 2 — XCom avec objets
**Fichier :** `dags/tp_4/xcoms_exo2.py` | **DAG :** `xcom_example`

- `push_task` envoie un dictionnaire (message, heure, valeur) via XCom
- `pull_task` récupère et affiche les données
- **Résultat :** Visible dans les **logs de `pull_task`**

### Exo 3 — Sensors
**Fichier :** `dags/tp_4/sensors_exo3.py` | **DAG :** `sensor_example`

- `FileSensor` attend l'apparition du fichier `data/bdd_tp_4/test_exo3.csv`
- Une fois détecté, lit le CSV et affiche les informations (taille, colonnes, premières lignes)
- **Résultat :** Visible dans les **logs de `file_ready`**

### Exo 4 — DAGs dynamiques
**Fichier :** `dags/tp_4/dynamic_dags_exo4.py` | **DAG :** `dynamic_dag_example`

- Génère dynamiquement 5 tâches parallèles via une boucle Python (une par ville : Paris, Lyon, Marseille, Toulouse, Bordeaux)
- Chaque tâche est un `BashOperator` qui affiche un message
- **Résultat :** Visible dans le **graph du DAG** (5 tâches parallèles) et dans les logs de chaque tâche

### Exo 5 — Trigger Rules
**Fichier :** `dags/tp_4/trigger_rules_exo5.py` | **DAG :** `trigger_rules_example`

- `task_success` réussit, `task_fail` échoue volontairement (`exit 1`)
- `task_final` s'exécute quand même grâce à `trigger_rule="one_failed"`
- **Résultat :** Visible dans le **graph du DAG** (task_fail en rouge, task_final en vert)

### Exo 6 — TaskFlow API
**Fichier :** `dags/tp_4/task_flow_api_exo6.py` | **DAG :** `customer_orders_taskflow`

- Reprend la logique de l'exo 3 du TP2, mais avec l'API moderne `@task` / `@dag`
- Les XCom sont gérés automatiquement via les valeurs de retour des fonctions
- **Résultat :** `output/results_tp_4/customer_orders_taskflow.csv`

---

## Structure du projet

```
airflow/
├── dags/
│   ├── tp_1/          # TP1 - ETL basique
│   ├── tp_2/          # TP2 - XCom et intégration
│   ├── tp_3/          # TP3 - SQL ETL
│   └── tp_4/          # TP4 - Concepts avancés
├── data/
│   ├── bdd_tp_1/      # sales_data.csv
│   ├── bdd_tp_2/      # orders, customers, weather, values
│   ├── bdd_tp_3/      # (vide — données en SQL)
│   └── bdd_tp_4/      # test_exo3.csv
├── output/
│   ├── results_tp_1/  # result.csv
│   ├── results_tp_2/  # customer_orders, weather_result, etc.
│   ├── results_tp_3/  # (résultats en PostgreSQL)
│   └── results_tp_4/  # customer_orders_taskflow.csv
├── docker-compose.yaml
├── .env
└── README.md
```
