#!/bin/bash

echo "Création de la structure de dossiers pour le projet extraction-multisource..."

# Créer le dossier principal des données
mkdir -p data/raw/sql
mkdir -p data/raw/parquet
mkdir -p data/raw/csv
mkdir -p data/raw/nlp

# Créer le dossier pour les données transformées
mkdir -p data/processed/images
mkdir -p data/processed/csv
mkdir -p data/processed/metadata

# Créer les dossiers pour les scripts
mkdir -p scripts

# Créer les dossiers pour les logs
mkdir -p logs

# Créer le dossier de configuration
mkdir -p config

# Créer le dossier pour les notebooks
mkdir -p notebooks

echo "Structure de dossiers créée avec succès pour extraction-multisource ! 🎉"
