import os
import dask.dataframe as dd
import pandas as pd
import base64

# Étape 1 : Traitement des fichiers Parquet et conversion en CSV
def process_parquet_files_with_dask(source_folder, destination_folder):
    """
    Lit et convertit tous les fichiers Parquet dans un dossier source en fichiers CSV
    dans le dossier de destination, en utilisant Dask.
    """
    os.makedirs(destination_folder, exist_ok=True)  # Crée le dossier si inexistant

    # Liste des fichiers Parquet
    parquet_files = [file for file in os.listdir(source_folder) if file.endswith('.parquet')]

    if not parquet_files:
        print(f"Aucun fichier Parquet trouvé dans le dossier : {source_folder}")
        return []

    csv_files = []
    for file_name in parquet_files:
        source_file = os.path.join(source_folder, file_name)
        destination_file = os.path.join(destination_folder, file_name.replace('.parquet', '.csv'))

        try:
            print(f"Lecture de : {source_file}")
            df = dd.read_parquet(source_file)
            df.to_csv(destination_file, single_file=True, index=False)
            print(f"Fichier converti en CSV : {destination_file}")
            csv_files.append(destination_file)
        except Exception as e:
            print(f"Erreur lors du traitement de {source_file} : {e}")

    return csv_files


# Étape 2 : Concaténation des fichiers CSV avec chunking
def concatenate_csv_files_in_chunks(folder_path, output_file, chunksize=10000):
    """
    Concatène les fichiers CSV dans un dossier en utilisant un traitement par morceaux (chunking)
    pour limiter l'utilisation de mémoire.
    """
    csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans le dossier : {folder_path}")
        return

    print(f"Fichiers trouvés pour concaténation : {csv_files}")
    
    try:
        # Initialiser un fichier de sortie vide
        is_first_file = True
        with open(output_file, 'w') as output:
            for file in csv_files:
                print(f"Traitement du fichier : {file}")
                # Lire par morceaux
                for chunk in pd.read_csv(file, chunksize=chunksize):
                    # Écrire l'en-tête uniquement pour le premier fichier
                    chunk.to_csv(output, index=False, header=is_first_file, mode='a')
                    is_first_file = False
        print(f"Fichier concaténé sauvegardé à : {output_file}")
    except Exception as e:
        print(f"Erreur lors de la concaténation : {e}")


# Étape 3 : Décodage et sauvegarde des images
def decode_and_save_images_with_chunking(csv_path, output_folder, updated_csv_path, chunksize=1000):
    """
    Décoder les images encodées dans un CSV et les sauvegarder dans un dossier.
    Met à jour le CSV avec les chemins des images par morceaux (chunking).
    """
    os.makedirs(output_folder, exist_ok=True)  # Crée le dossier si inexistant

    # Charger le CSV par chunks
    try:
        reader = pd.read_csv(csv_path, chunksize=chunksize)
    except Exception as e:
        print(f"Erreur lors de la lecture du CSV : {e}")
        return

    # Initialiser le fichier de sortie pour le CSV mis à jour
    is_first_chunk = True

    for chunk in reader:
        # Initialiser la liste des chemins d'images
        image_paths = []

        for index, row in chunk.iterrows():
            try:
                # Décoder les données d'image
                image_data = base64.b64decode(row['image'])
                image_filename = f"{row['item_ID']}.webp"
                image_path = os.path.join(output_folder, image_filename)

                # Sauvegarder l'image
                with open(image_path, 'wb') as image_file:
                    image_file.write(image_data)

                print(f"Image sauvegardée : {image_path}")
                image_paths.append(image_path)
            except Exception as e:
                print(f"Erreur pour l'item_ID {row['item_ID']} : {e}")
                image_paths.append(None)

        # Ajouter les chemins d'images au chunk
        chunk['image_path'] = image_paths

        # Supprimer la colonne 'image' si elle existe
        if 'image' in chunk.columns:
            chunk = chunk.drop(columns=['image'])

        # Sauvegarder le chunk mis à jour
        chunk.to_csv(updated_csv_path, index=False, mode='a', header=is_first_chunk)
        is_first_chunk = False

    print(f"CSV mis à jour sauvegardé à : {updated_csv_path}")


# Configuration des chemins
source_folder = "data/raw/blob/product_eval"
destination_folder = "data/raw/parquet"
output_file = "data/raw/csv/product_eval/product_eval.csv"
image_output_folder = "data/processed/product_eval_images"
updated_csv_path = "data/processed/csv_final/product_eval/product_eval.csv"

# Étape 1 : Conversion des fichiers Parquet en CSV
csv_files = process_parquet_files_with_dask(source_folder, destination_folder)

# Étape 2 : Concaténation des fichiers CSV
concatenate_csv_files_in_chunks(destination_folder, output_file, chunksize=5000)

# Étape 3 : Décodage et sauvegarde des images
decode_and_save_images_with_chunking(output_file, image_output_folder, updated_csv_path, chunksize=1000)
