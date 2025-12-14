import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

LOCAL_ROOT = Path("data_lake")
CONTAINER_NAME = "data-lake"


conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

service_client = BlobServiceClient.from_connection_string(conn_str)
container_client = service_client.get_container_client(CONTAINER_NAME)

for local_path in LOCAL_ROOT.rglob("*"):
    if local_path.is_file():
        blob_path = str(local_path.relative_to(LOCAL_ROOT))
        with open(local_path, "rb") as f:
            container_client.upload_blob(
                name=blob_path,
                data=f,
                overwrite=True,
            )
            print(f"Uploaded {blob_path}")
