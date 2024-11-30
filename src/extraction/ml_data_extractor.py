import os
import zipfile
import tarfile
import shutil

# Définition des chemins principaux
base_path = "data_temporaire/machine_learning"
zip_file_path = os.path.join(base_path, "reviews.zip")
tgz_file_path = os.path.join(base_path, "machine_learning_extract/amazon_review_polarity_csv.tgz")
extract_dir = os.path.join(base_path, "machine_learning_extract")
target_dir = "data/machine_learning"

# Fonction pour s'assurer qu'un dossier existe, sinon le créer
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Extraction d'un fichier ZIP
def extract_zip(zip_path, extract_to):
    ensure_directory_exists(extract_to)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"✔️ ZIP extrait dans : {extract_to}")

# Extraction d'un fichier TGZ
def extract_tgz(tgz_path, extract_to):
    ensure_directory_exists(extract_to)
    with tarfile.open(tgz_path, "r:gz") as tar_ref:
        tar_ref.extractall(extract_to)
    print(f"✔️ TGZ extrait dans : {extract_to}")

# Déplacement d'un dossier vers un autre
def move_directory(source, destination):
    ensure_directory_exists(destination)
    shutil.move(source, destination)
    print(f"✔️ Dossier déplacé vers : {destination}")

# Suppression du contenu d'un répertoire
def delete_directory_content(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        print(f"✔️ Contenu de '{directory}' supprimé.")
    else:
        print(f"❌ Le répertoire '{directory}' n'existe pas.")

# Exécution des étapes principales
if __name__ == "__main__":
    # Étape 1 : Extraction du fichier ZIP
    print("📂 Extraction du fichier ZIP...")
    extract_zip(zip_file_path, extract_dir)

    # Étape 2 : Extraction du fichier TGZ
    print("\n📂 Extraction du fichier TGZ...")
    extract_tgz(tgz_file_path, extract_dir)

    # Étape 3 : Déplacement du dossier extrait
    print("\n📂 Déplacement du dossier de données...")
    source_dir = os.path.join(extract_dir, "amazon_review_polarity_csv")
    move_directory(source_dir, target_dir)

    # Étape 4 : Suppression du contenu de data_temporaire/machine_learning
    print("\n🗑️ Suppression du contenu temporaire...")
    delete_directory_content(base_path)

    print("\n✅ Toutes les étapes sont terminées avec succès.")
