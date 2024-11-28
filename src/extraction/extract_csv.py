import os
import shutil

def extract_and_copy_csv(source_folder, destination_folder):
    """
    Parcourt tous les sous-dossiers du dossier source pour trouver des fichiers CSV
    et les copie dans le dossier de destination.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)  # Crée le dossier destination s'il n'existe pas
    
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith('.csv'):  # Vérifie si le fichier est un CSV
                source_file = os.path.join(root, file)
                destination_file = os.path.join(destination_folder, file)
                
                # Copie le fichier CSV
                shutil.copy2(source_file, destination_file)
                print(f"Copié : {source_file} → {destination_file}")

# Définir les chemins source et destination
source_folder = "data/raw/blob/nlp_data"
destination_folder = "data/processed/csv_final/nlp_data"

extract_and_copy_csv(source_folder, destination_folder)
