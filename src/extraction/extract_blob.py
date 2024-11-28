import os
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv
from tqdm import tqdm
from extraction.extract_zip_tgz_machine_learning import extract_zip_file, extract_tgz_file, move_csv_files  # Importer les fonctions nécessaires

# Charger les variables d'environnement
load_dotenv()

# Variables Azure
account_name = os.getenv("ACCOUNT_NAME")
container_name = "data"
sas_token = os.getenv("SAS_TOKEN")
local_download_path = "data_temporaire"
# processed_csv_dir = "data/raw/csv/machine_learning"
account_url = f"https://{account_name}.blob.core.windows.net"

if not account_name:
    raise ValueError("Le nom du compte (ACCOUNT_NAME) est introuvable dans le fichier .env.")
if not sas_token:
    raise ValueError("Le SAS token est introuvable dans le fichier .env.")

# Fonction pour lister les blobs
def list_blobs(container_client):
    """
    Récupère la liste des blobs avec leur taille.
    """
    print(f"🔍 Récupération de la liste des blobs du conteneur '{container_name}'...")
    blobs = container_client.list_blobs()
    blob_list = [{"name": blob.name, "size": blob.size} for blob in blobs if blob.size > 0]
    print(f"👍 {len(blob_list)} blobs trouvés.")
    return blob_list

# Fonction pour télécharger un blob avec suivi par morceaux
def download_blob(container_client, blob_name, local_path):
    """
    Télécharge un fichier blob Azure sur le système local s'il n'existe pas ou si sa taille est différente.
    """
    full_local_path = os.path.join(local_path, blob_name)
    os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

    blob_client = container_client.get_blob_client(blob_name)
    blob_properties = blob_client.get_blob_properties()
    blob_size = blob_properties.size

    # Vérifier si le fichier existe et si sa taille correspond
    if os.path.exists(full_local_path):
        local_size = os.path.getsize(full_local_path)
        if local_size == blob_size:
            print(f"✅ Fichier déjà téléchargé : {blob_name} (taille identique)")
            return full_local_path

    # Télécharger le fichier par morceaux
    try:
        with open(full_local_path, "wb") as file:
            with tqdm(total=blob_size, unit="o", unit_scale=True, desc=f"Téléchargement {blob_name}") as pbar:
                stream = blob_client.download_blob()
                for chunk in stream.chunks():
                    file.write(chunk)
                    pbar.update(len(chunk))

        print(f"📥 Fichier téléchargé : {blob_name}")
        return full_local_path
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement de {blob_name} : {e}")
        return None

# Télécharger les fichiers sans extraction
def download_files(container_client, blobs, local_path):
    """
    Télécharge les blobs Azure.
    """
    print("\n📥 Début du téléchargement des fichiers...")
    total_files = len(blobs)
    with tqdm(total=total_files, desc="Progression globale", unit="fichier") as pbar:
        for blob in blobs:
            blob_name = blob["name"]
            try:
                downloaded_file_path = download_blob(container_client, blob_name, local_path)
                if downloaded_file_path:
                    print(f"🔍 Fichier téléchargé à : {downloaded_file_path}")
            except Exception as e:
                print(f"❌ Erreur critique pour {blob_name} : {e}")
            finally:
                pbar.update(1)
    print("✅ Téléchargement terminé.")

# Main logic
if __name__ == "__main__":
    container_client = ContainerClient(account_url, container_name, credential=sas_token)

    # Étape 1 : Lister les blobs
    blobs = list_blobs(container_client)

    if not blobs:
        print("🚫 Aucun fichier à télécharger.")
    else:
        # Étape 2 : Télécharger les fichiers
        download_files(container_client, blobs, local_download_path)

        # # Étape 3 : Extraire le fichier ZIP
        # print("\n📂 Extraction du fichier ZIP...")
        # zip_file_path = os.path.join(local_download_path, "machine_learning/reviews.zip")
        # extract_to_zip_path = os.path.join(local_download_path, "machine_learning/reviews/")
        # extract_zip_file(zip_file_path, extract_to_zip_path)

        # # Étape 4 : Extraire le fichier TGZ
        # print("\n📂 Extraction du fichier TGZ...")
        # tgz_file_path = os.path.join(local_download_path, "machine_learning/reviews/amazon_review_polarity_csv.tgz")
        # extract_to_tgz_path = os.path.join(local_download_path, "machine_learning/reviews/extracted/")
        # extract_tgz_file(tgz_file_path, extract_to_tgz_path)

        # # Étape 5 : Déplacer les fichiers CSV extraits
        # print("\n📂 Organisation des fichiers CSV...")
        # move_csv_files(extract_to_tgz_path, processed_csv_dir)

        # print("✅ Téléchargement, extraction et organisation terminés.")
