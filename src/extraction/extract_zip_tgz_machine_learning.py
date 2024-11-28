import os
import zipfile
import tarfile
import shutil

# Définition des chemins
local_download_path = "data_temporaire/machine_learning"
processed_csv_dir = os.path.join(local_download_path, "machine_learning_extract")

def extract_zip_file(zip_file_path, extract_to_path):
    """
    Extrait un fichier ZIP dans un répertoire donné.
    """
    if not os.path.exists(zip_file_path):
        print(f"❌ Le fichier ZIP '{zip_file_path}' n'existe pas.")
        return False

    os.makedirs(extract_to_path, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)
            print(f"✅ Fichiers ZIP extraits avec succès dans '{extract_to_path}'")
            return True
    except zipfile.BadZipFile:
        print(f"❌ Fichier ZIP corrompu : '{zip_file_path}'.")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction du ZIP : {e}")
    return False

def extract_tgz_file(tgz_file_path, extract_to_path):
    """
    Extrait un fichier TGZ (tar.gz) dans un répertoire donné.
    """
    if not os.path.exists(tgz_file_path):
        print(f"❌ Le fichier TGZ '{tgz_file_path}' n'existe pas.")
        return False

    os.makedirs(extract_to_path, exist_ok=True)

    try:
        with tarfile.open(tgz_file_path, "r:gz") as tar_ref:
            tar_ref.extractall(path=extract_to_path)
            print(f"✅ Fichiers TGZ extraits avec succès dans '{extract_to_path}'")
            return True
    except tarfile.ReadError:
        print(f"❌ Fichier TGZ corrompu : '{tgz_file_path}'.")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction du TGZ : {e}")
    return False

def move_csv_files(source_dir, target_dir):
    """
    Déplace tous les fichiers CSV d'un répertoire source (y compris ses sous-répertoires) vers un répertoire cible.
    """
    if not os.path.exists(source_dir):
        print(f"❌ Le répertoire source '{source_dir}' n'existe pas.")
        return False

    os.makedirs(target_dir, exist_ok=True)

    csv_found = False
    for root, _, files in os.walk(source_dir):  # Exploration récursive des sous-dossiers
        for file_name in files:
            if file_name.lower().endswith('.csv'):
                csv_found = True
                source_path = os.path.join(root, file_name)
                target_path = os.path.join(target_dir, file_name)
                shutil.move(source_path, target_path)
                print(f"✅ Fichier déplacé : {file_name} vers '{target_path}'")

    if not csv_found:
        print("❌ Aucun fichier CSV trouvé.")
    else:
        print(f"✅ Tous les fichiers CSV ont été déplacés vers '{target_dir}'.")

if __name__ == "__main__":
    # Extraction du fichier ZIP dans machine_learning_extract
    print("\n📂 Extraction du fichier ZIP...")
    zip_file_path = os.path.join(local_download_path, "reviews.zip")
    extract_zip_file(zip_file_path, processed_csv_dir)

    # Extraction du fichier TGZ
    print("\n📂 Extraction du fichier TGZ...")
    tgz_file_path = os.path.join(local_download_path, "data_temporaire/machine_learning/machine_learning_extract/amazon_review_polarity_csv.tgz")
    extract_to_tgz_path = os.path.join(processed_csv_dir, "extracted_tgz")
    extract_tgz_file(tgz_file_path, extract_to_tgz_path)

    # Organisation des fichiers CSV
    print("\n📂 Organisation des fichiers CSV...")
    move_csv_files(extract_to_tgz_path, processed_csv_dir)

    print("✅ Téléchargement, extraction et organisation terminés.")
