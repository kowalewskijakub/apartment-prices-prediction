import os
from datetime import datetime

from azure.storage.blob import BlobServiceClient

AZURE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "model"

def upload_model_to_blob(model_path):
    """
    Upload a model file to Azure Blob Storage, overwriting if it already exists.
    
    Args:
        model_path: Path to the model file
    """
    if not AZURE_CONNECTION_STRING:
        print("AZURE_STORAGE_CONNECTION_STRING not set. Skipping upload to Azure.")
        return

    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        model_filename = os.path.basename(model_path)
        
        # Upload to a fixed location (predictor.pkl) so it will be overwritten
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=model_filename
        )
        
        # Read and upload the model file
        with open(model_path, "rb") as model_file:
            blob_client.upload_blob(model_file, overwrite=True)
            
        print(f"Successfully uploaded {model_filename} to Azure Blob Storage")
        
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")
