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
