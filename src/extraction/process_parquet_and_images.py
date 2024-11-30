import os
import pandas as pd
import base64

# 📂 Étape 1 : Conversion des fichiers Parquet en CSV
def convert_parquet_to_csv(source_folder, destination_folder):
    """
    Convertit les fichiers Parquet en CSV et les sauvegarde dans le dossier destination.
    """
    os.makedirs(destination_folder, exist_ok=True)  # Crée le dossier destination si inexistant

    parquet_files = [file for file in os.listdir(source_folder) if file.endswith('.parquet')]

    if not parquet_files:
        print(f"Aucun fichier Parquet trouvé dans {source_folder}")
        return

    for file in parquet_files:
        source_file = os.path.join(source_folder, file)
        dest_file = os.path.join(destination_folder, file.replace('.parquet', '.csv'))

        print(f"Conversion de {source_file} en CSV...")
        df = pd.read_parquet(source_file)
        df.to_csv(dest_file, index=False)
        print(f"Fichier CSV sauvegardé : {dest_file}")


# 📂 Étape 2 : Concaténation des fichiers CSV (avec chunking)
def concatenate_csv_files(folder_path, output_file, chunksize=5000):
    """
    Concatène plusieurs fichiers CSV en un seul, en utilisant un traitement par morceaux (chunking).
    """
    csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans {folder_path}")
        return

    print(f"Concaténation des fichiers CSV : {csv_files}")
    is_first_file = True

    # Lecture et écriture par morceaux
    with open(output_file, 'w') as output:
        for file in csv_files:
            print(f"Traitement de {file}...")
            for chunk in pd.read_csv(file, chunksize=chunksize):
                chunk.to_csv(output, index=False, header=is_first_file, mode='a')
                is_first_file = False

    print(f"Fichier concaténé sauvegardé : {output_file}")


# 📂 Étape 3 : Décodage des images et mise à jour du CSV
def decode_images_and_update_csv(csv_path, image_folder, updated_csv_path):
    """
    Décoder les images encodées dans un CSV, les sauvegarder et mettre à jour le CSV.
    """
    os.makedirs(image_folder, exist_ok=True)  # Crée le dossier pour les images

    reader = pd.read_csv(csv_path, chunksize=1000)
    is_first_chunk = True

    for chunk in reader:
        image_paths = []

        for _, row in chunk.iterrows():
            try:
                # Récupérer la chaîne encodée et corriger le padding si nécessaire
                encoded_data = row['image']
                if len(encoded_data) % 4 != 0:
                    padding = 4 - (len(encoded_data) % 4)
                    encoded_data += "=" * padding

                # Décoder les données
                image_data = base64.b64decode(encoded_data)

                # Sauvegarder l'image
                image_filename = f"{row['item_ID']}.webp"
                image_path = os.path.join(image_folder, image_filename)
                with open(image_path, 'wb') as img_file:
                    img_file.write(image_data)

                image_paths.append(image_path)  # Ajouter le chemin de l'image
                print(f"Image sauvegardée : {image_path}")
            except Exception as e:
                print(f"Erreur pour item_ID {row.get('item_ID', 'inconnu')} : {e}")
                image_paths.append(None)

        # Ajouter les chemins d'images au chunk
        chunk['image_path'] = image_paths
        chunk.drop(columns=['image'], inplace=True)  # Supprimer la colonne originale
        chunk.to_csv(updated_csv_path, index=False, mode='a', header=is_first_chunk)
        is_first_chunk = False

    print(f"CSV mis à jour sauvegardé : {updated_csv_path}")


# 📂 Étape 4 : Suppression des fichiers temporaires
def clean_temporary_files(folder_path):
    """
    Supprime les fichiers temporaires dans un dossier spécifique.
    """
    try:
        if not os.path.exists(folder_path):
            print(f"Le dossier {folder_path} n'existe pas, aucune suppression nécessaire.")
            return
        
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Fichier supprimé : {file_path}")
            else:
                print(f"Non supprimé (non fichier) : {file_path}")
    except Exception as e:
        print(f"Erreur lors de la suppression des fichiers : {e}")


# 🔧 Configuration des chemins
source_folder = "data_temporaire/product_eval"
destination_folder = "data_temporaire/product_eval"
output_file = "data_temporaire/product_eval/merged_product_eval.csv"
image_output_folder = "data/product_eval/product_eval_images"
updated_csv_path = "data/product_eval/updated_product_eval.csv"

# 🚀 Étapes principales
if __name__ == "__main__":
    # Crée les dossiers nécessaires
    os.makedirs("data/product_eval", exist_ok=True)
    os.makedirs(image_output_folder, exist_ok=True)

    print("\nÉtape 1 : Conversion des fichiers Parquet en CSV")
    convert_parquet_to_csv(source_folder, destination_folder)

    print("\nÉtape 2 : Concaténation des fichiers CSV")
    concatenate_csv_files(destination_folder, output_file)

    print("\nÉtape 3 : Décodage des images et mise à jour du CSV")
    decode_images_and_update_csv(output_file, image_output_folder, updated_csv_path)

    print("\nÉtape 4 : Suppression des fichiers temporaires")
    clean_temporary_files(destination_folder)
