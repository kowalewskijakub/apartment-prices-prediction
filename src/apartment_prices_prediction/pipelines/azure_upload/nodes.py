import logging
import os
from pathlib import Path

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

CONTAINER_NAME = "model"


def upload_model_to_blob(model_directory_path: str) -> None:
    """Wysyła cały katalog z modelem do Azure Blob Storage.

    Funkcja iteruje po wszystkich plikach w danym katalogu i wysyła je
    do kontenera w Azure Blob Storage, zachowując strukturę katalogów.

    Args:
        model_directory_path: Ścieżka do lokalnego katalogu z modelem.
    """
    azure_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not azure_connection_string:
        logger.warning("Zmienna AZURE_STORAGE_CONNECTION_STRING nie jest ustawiona. Pomijanie wysyłania do Azure.")
        return

    try:
        logger.info(f"Rozpoczynanie wysyłania katalogu modelu: {model_directory_path}")
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"Utworzono kontener: {CONTAINER_NAME}")

        model_path = Path(model_directory_path)
        files_uploaded = 0
        for local_file_path in model_path.rglob('*'):
            if local_file_path.is_file():
                blob_path = local_file_path.relative_to(model_path).as_posix()
                blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)

                logger.info(f"Wysyłanie: {local_file_path} -> {blob_path}")
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                files_uploaded += 1

        logger.info(f"Pomyślnie wysłano {files_uploaded} plików z {model_directory_path} do Azure Blob Storage.")

    except AzureError as e:
        logger.error(f"Błąd Azure podczas wysyłania do Blob Storage: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd podczas wysyłania do Azure Blob Storage: {e}", exc_info=True)
