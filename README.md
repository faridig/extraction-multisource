# extraction-multisource
![alt text](image.png)

# Résumé de ma mission principale

Créer un **pipeline unifié** pour l’extraction, la transformation et l’organisation des données provenant de plusieurs sources :

- **Base OLTP** (Adventure Works SQL),
- **Fichiers Parquet et CSV compressés** sur un Data Lake,
- **Données de reviews textuelles** issues de fichiers.

Ces données serviront ensuite aux équipes de **Data Scientists** pour construire et affiner un modèle capable d’analyser à la fois des données **textuelles** et **visuelles**.

---

## Objectifs clés de ta mission

### 1. Centraliser les données multi-sources

**Sources principales :**

- **Base de données SQL** : Extraire les données sur les clients, produits et ventes.
- **Data Lake (Parquet)** : Collecter et transformer les avis enrichis (texte + images).
- **Data Lake (CSV compressés)** : Extraire et organiser les données tabulaires.
- **Data Lake (NLP Data)** : Traiter les fichiers de reviews textuelles non structurées.

---

### 2. Automatiser les extractions

Créer des **scripts robustes** pour automatiser l'extraction des données depuis chaque source, tout en respectant les contraintes techniques et sécuritaires :

- **Base SQL** : Programmation d’extractions pendant les heures creuses (20h-8h).
- **Data Lake** : Utilisation de tokens SAS pour un accès sécurisé.
- **Transformation** : Convertir les données brutes en fichiers CSV ou images utilisables.

---

### 3. Structurer les données

**Format de sortie attendu :**

- **CSV** pour les données tabulaires (clients, produits, ventes).
- **Images (.png)** pour les fichiers visuels extraits des fichiers Parquet.
- Un **fichier CSV** qui associe les métadonnées des avis aux noms des images.

---

### 4. Journaliser et documenter

Concevoir un pipeline organisé avec des étapes distinctes et bien documentées :

- **Extraction** : Mise en place des connexions sécurisées et récupération des données.
- **Transformation** : Nettoyage et formatage des données.
- **Journalisation** : Logs détaillés pour suivre chaque étape du processus.
- **Versionnement** : Scripts accessibles et documentés dans un dépôt Git.

---

### 5. Optimisation et sécurité

**Sécuriser les accès aux données :**

- Génération et gestion des **tokens SAS** pour le Data Lake.
- Utilisation de bibliothèques sécurisées pour les connexions SQL.

**Optimiser les performances :**

- Limiter l’impact sur les bases de production (extraction planifiée pendant les heures creuses).
- Manipuler les fichiers Parquet et CSV compressés sans étape de décompression inutile.

Voici l’arborescence du projet pour organiser les données, scripts, logs et configurations :

```plaintext
extraction-multisource/
├── config/                    # Fichiers de configuration (ex : config.yaml, credentials.json)
├── data/                      # Données brutes et transformées
│   ├── raw/                   # Données brutes extraites des sources
│   │   ├── sql/               # Données extraites de la base SQL
│   │   ├── parquet/           # Données extraites des fichiers Parquet
│   │   ├── csv/               # Données extraites des fichiers CSV compressés
│   │   └── nlp/               # Données textuelles brutes (reviews textuelles)
│   ├── processed/             # Données transformées et prêtes à l'utilisation
│   │   ├── images/            # Images extraites des fichiers Parquet
│   │   ├── csv/               # Données tabulaires nettoyées
│   │   └── metadata/          # Fichiers associant les métadonnées des avis et des images
├── logs/                      # Logs pour suivre l’exécution des scripts
│   ├── extraction.log         # Log des étapes d'extraction
│   └── transformation.log     # Log des étapes de transformation
├── notebooks/                 # Notebooks Jupyter pour l'exploration initiale des données
│   ├── exploration_sql.ipynb  # Exploration des données SQL
│   ├── exploration_parquet.ipynb # Exploration des fichiers Parquet
│   └── exploration_csv.ipynb  # Exploration des fichiers CSV
├── scripts/                   # Scripts d'extraction et transformation
│   ├── sql_extraction.py      # Script pour extraire les données SQL
│   ├── parquet_extraction.py  # Script pour manipuler les fichiers Parquet
│   ├── csv_extraction.py      # Script pour traiter les fichiers CSV compressés
│   ├── nlp_extraction.py      # Script pour organiser les données NLP
│   ├── transform.py           # Script de transformation des données
│   └── main_pipeline.py       # Script principal orchestrant toutes les étapes
├── .gitignore                 # Fichiers ou dossiers à ignorer par Git
├── README.md                  # Documentation principale du projet
├── requirements.txt           # Liste des dépendances Python nécessaires
└── install.sh                 # Script d'installation et de configuration initiale