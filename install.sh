#!/bin/bash
echo "Installation des dépendances..."
pip install -r requirements.txt
sudo apt-get update && sudo apt-get install -y libpq-dev
echo "Installation terminée ✅"
