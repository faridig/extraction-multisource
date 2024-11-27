from azure.storage.blob import generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta
from dotenv import load_dotenv, set_key
import os

# Charger les variables d'environnement à partir du fichier .env
env_file_path = ".env"  # Chemin du fichier .env
load_dotenv()

# Récupérer les informations sensibles depuis les variables d'environnement
account_name = os.getenv("ACCOUNT_NAME")
account_key = os.getenv("ACCOUNT_KEY")

def generate_common_sas_token(account_name, container_name, account_key, duration_hours=6):
    """
    Génère un SAS token commun pour un conteneur contenant plusieurs sources.

    :param account_name: Nom du compte de stockage Azure.
    :param container_name: Nom du conteneur contenant les sources (ex: "data").
    :param account_key: Clé principale du compte de stockage Azure.
    :param duration_hours: Durée de validité du SAS token en heures.
    :return: Le SAS token généré (string).
    """
    expiry_time = datetime.utcnow() + timedelta(hours=duration_hours)

    # Générer un SAS token pour tout le conteneur avec les permissions read et list
    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, list=True),  # Ajout de la permission "list"
        expiry=expiry_time
    )

    # S'assurer que le SAS token commence par "?"
    if not sas_token.startswith("?"):
        sas_token = f"?{sas_token}"

    return sas_token


def update_env_file(key, value, env_file_path=".env"):
    """
    Met à jour ou ajoute une clé dans le fichier .env.

    :param key: Nom de la variable d'environnement.
    :param value: Valeur à assigner.
    :param env_file_path: Chemin vers le fichier .env.
    """
    set_key(env_file_path, key, value)
    print(f"✅ La variable {key} a été mise à jour dans le fichier {env_file_path}.")


if __name__ == "__main__":
    # Vérifier si les variables d'environnement sont disponibles
    if not account_name or not account_key:
        raise ValueError("Les variables d'environnement ACCOUNT_NAME et ACCOUNT_KEY doivent être définies.")

    container_name = "data"  # Conteneur contenant tes sources (product_eval, machine_learning, nlp_data)

    print("🔑 Génération du SAS token...")
    sas_token = generate_common_sas_token(account_name, container_name, account_key)
    print(f"SAS token généré : {sas_token}")

    # Mettre à jour le SAS token dans le fichier .env
    update_env_file("SAS_TOKEN", sas_token, env_file_path)
    print("✅ SAS token mis à jour dans le fichier .env avec succès.")
