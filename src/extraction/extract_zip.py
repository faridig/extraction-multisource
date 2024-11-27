import os
import zipfile
import tarfile
import shutil

def extract_zip_file(zip_file_path, extract_to_path):
    """
    Extrait un fichier ZIP dans un répertoire donné.
    """
    if not os.path.exists(zip_file_path):
        print(f"❌ Le fichier ZIP {zip_file_path} n'existe pas.")
        return False

    if not os.path.exists(extract_to_path):
        os.makedirs(extract_to_path)

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)
            print(f"✅ Fichiers ZIP extraits avec succès dans {extract_to_path}")
            return True
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction du ZIP : {e}")
        return False

def extract_tgz_file(tgz_file_path, extract_to_path):
    """
    Extrait un fichier TGZ (tar.gz) dans un répertoire donné.
    """
    if not os.path.exists(tgz_file_path):
        print(f"❌ Le fichier TGZ {tgz_file_path} n'existe pas.")
        return False

    if not os.path.exists(extract_to_path):
        os.makedirs(extract_to_path)

    try:
        with tarfile.open(tgz_file_path, "r:gz") as tar_ref:
            tar_ref.extractall(path=extract_to_path)
            print(f"✅ Fichiers TGZ extraits avec succès dans {extract_to_path}")
            return True
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction du TGZ : {e}")
        return False

def move_csv_files(source_dir, target_dir):
    """
    Déplace tous les fichiers CSV d'un répertoire source (y compris ses sous-répertoires) vers un répertoire cible.
    """
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for root, dirs, files in os.walk(source_dir):  # Exploration récursive des sous-dossiers
        for file_name in files:
            if file_name.endswith('.csv'):
                source_path = os.path.join(root, file_name)
                target_path = os.path.join(target_dir, file_name)
                shutil.move(source_path, target_path)
                print(f"✅ Fichier déplacé : {file_name} vers {target_path}")

# if __name__ == "__main__":
    # # Chemins des fichiers et répertoires d'extraction
    # zip_file = "data/raw/blob/machine_learning/reviews.zip"
    # tgz_file = "data/raw/blob/machine_learning/reviews/amazon_review_polarity_csv.tgz"

    # extract_to_zip = "data/raw/blob/machine_learning/reviews/"
    # extract_to_tgz = "data/raw/blob/machine_learning/reviews/extracted/"
    # processed_csv_dir = "data/raw/csv"

    # # Extraction des fichiers ZIP
    # print(f"📂 Début de l'extraction de {zip_file}...")
    # extract_zip_file(zip_file, extract_to_zip)

    # # Extraction des fichiers TGZ
    # print(f"📂 Début de l'extraction de {tgz_file}...")
    # extract_tgz_file(tgz_file, extract_to_tgz)

    # # Déplacement des fichiers CSV extraits
    # print(f"📂 Organisation des fichiers CSV...")
    # move_csv_files(extract_to_tgz, processed_csv_dir)
