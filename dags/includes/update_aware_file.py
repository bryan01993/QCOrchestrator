from typing import Tuple, Optional

from airflow.operators.python import get_current_context
from google.cloud import storage


def get_blob_and_content(bucket: storage.Bucket, file_name: str) -> Tuple[storage.Blob, Optional[str]]:
    """
    Get the Blob object and content of the specified file in the bucket.
    """
    blob = bucket.blob(file_name)

    if blob.exists():
        current_content = blob.download_as_text()
        return blob, current_content
    else:
        return blob, None


def update_content(current_content: Optional[str], content_to_write: str) -> Optional[str]:
    """
    Update the content string with the new content, ensuring no duplicate entries.
    """
    if current_content == content_to_write:
        print(f"El contenido {content_to_write} ya se encuentra en el archivo.")
        return None

    current_content_list = current_content.split(",") if current_content else []
    current_content_list.append(content_to_write)
    return ",".join(current_content_list)


def create_or_update_aware_file(target_project_id: str, dataproc_bucket: str, table_name: str,
                                **kwargs) -> None:
    """
    Create or update a data-aware file inserting a data-aware dataset name in the file.
    The idea is to use the list inside the file to identify what date or dates need to
    be processed in the silver stage.
    """
    context = get_current_context()
    date = context["ds_nodash"]
    epoc = "bronze"

    file_name = f"aware_files/{epoc}/{table_name}.txt"
    content_to_write = f"s3://{epoc}/{table_name}/{date}"

    client = storage.Client(project=target_project_id)
    bucket = client.bucket(dataproc_bucket)

    blob, current_content = get_blob_and_content(bucket, file_name)
    new_content = update_content(current_content, content_to_write)

    if new_content:
        blob.upload_from_string(new_content)