# 🏦 Analyse des avis clients sur les agences bancaires marocaines

## 🌟 Objectif

Ce projet vise à **collecter, nettoyer, transformer, analyser et modéliser** les avis Google Maps concernant les agences bancaires marocaines, afin d’extraire :

* des **indicateurs de satisfaction client**,
* des **thématiques fréquentes** (topics),
* et d’identifier les **forces et faiblesses** de chaque agence.

---

## 🏗️ Architecture du pipeline

* **Collecte des données** : scraping automatisé avec **Python/Selenium**.
* **Orchestration** : orchestrée avec **Apache Airflow**.
* **Stockage** : base de données **PostgreSQL**.
* **Transformation** :

  * nettoyage, détection de langue, analyse de sentiment avec **Python/NLP**,
  * modélisation avec **DBT**.
* **Modélisation** : création d’un **schéma en étoile** (star schema) pour faciliter l’analyse.
* **Visualisation** : **dashboard interactif Looker Studio**.

---

## 📁 Structure du projet

```
DW_Project-LAOUAD_Ayoub/
├── Banque_General_Dag.py
├── LookerStudio-Dashboard.pdf
├── Projet DW INSEA.pdf

├── Phase1-DataCollection/
│   ├── Banque_Collecting_Dag.py
│   ├── Scraping.py
│   └── Resultat/

├── Phase2-DataCleaning&Transformation/
│   ├── Banque_Cleaning_Dag.py
│   ├── DBT_bank_model/
│   └── Resultat/

├── Phase3-DataModeling/
│   ├── Banque_Modeling_Dag.py
│   ├── DBT_reviews_DW/
│   └── Resultat/
```

---

## 🚀 Démarrage rapide

### ✅ Prérequis

* Python 3.8+
* PostgreSQL (ex. : `bank_reviews`)
* Apache Airflow
* DBT (`pip install dbt-postgres`)
* ChromeDriver (compatible avec ta version de Google Chrome)

---

### ⚙️ Installation des dépendances

```bash
pip install -r requirements.txt
```

---

### 🔧 Configuration

* Créer la connexion `banques_db_conn` dans Airflow.
* Adapter les **chemins des fichiers** et **noms de base/schema/table** si nécessaire.
* Vérifier les variables d’environnement (DBT, Airflow, Python virtualenv, etc.).

---

### 💠 Exécution du pipeline

1. Démarrer Airflow :

   ```bash
   airflow scheduler
   airflow webserver
   ```

2. Exécuter les DAGs dans l’ordre suivant :

   * `Banque_Collecting_Dag` : Scraping des avis Google.
   * `Banque_Cleaning_Dag` : Nettoyage, enrichissement NLP, DBT (clean\_reviews).
   * `Banque_Modeling_Dag` : Création du schéma en étoile via DBT (star\_schema).

3. Les résultats sont stockés dans :

   * `public.final_reviews` → table nettoyée enrichie.
   * `star_schema.fact_reviews`, `dim_bank`, etc. → tables du Data Warehouse.

---

### 📊 Visualisation

* Le fichier `LookerStudio-Dashboard.pdf` contient un aperçu du dashboard créé.
* Le dashboard complet est publié sur **Looker Studio** (lien à ajouter si disponible).

---

## 📦 Livrables

* Scripts Python (scraping, traitement NLP)
* DAGs Airflow
* Modèles DBT (staging et star schema)
* Base de données PostgreSQL avec schémas `public` et `star_schema`
* Dashboard Looker Studio
* Documentation PDF du projet (`Projet DW INSEA.pdf`)

---

## 👤 Auteur

**Ayoub LAOUAD**
Étudiant en Master Systèmes d’Information et Systèmes Intelligents – INSEA Rabat

Pour toute suggestion, amélioration ou question, n’hésitez pas à ouvrir une issue ou à me contacter.

---

## 📒 Références

* [DBT Documentation](https://docs.getdbt.com/)
* [Airflow Documentation](https://airflow.apache.org/docs/)
* [Transformers by Hugging Face](https://huggingface.co/transformers/)
* [Looker Studio](https://lookerstudio.google.com/)
