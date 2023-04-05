import os
import logging

log = logging.getLogger(__name__)


def AwsCloudProvider(
    provider: str,
    conn_id: str,
    bucket_name: str,
    file_path: str,
    file_name: str,
    release_name: str,
) -> tuple:
    """
    Uploads a file to an Amazon S3 bucket using the S3Hook class.

    Args:
        provider (str): The name of the cloud provider. In this case, it should be "aws".
        conn_id (str): The connection ID for the Amazon Web Services (AWS) account.
        bucket_name (str): The name of the S3 bucket.
        file_path (str): The path to the file to be uploaded.
        file_name (str): The name of the file to be uploaded.
        release_name (str): The name of the release.

    Returns:
        tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
    """
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        logging.info(
            "Connecting to aws s3 service to validate bucket connection........"
        )
        s3Class = S3Hook(aws_conn_id=conn_id)
        s3Class.check_for_bucket(bucket_name=bucket_name)
        with open(file_path, "rb") as f:
            s3Class._upload_file_obj(file_obj=f, key=file_name, bucket_name=bucket_name)
        log.info("data sent to s3 bucket sucessfully")
    except Exception as e:
        return False, release_name, provider, e


def GcsCloudProvider(
    provider: str,
    conn_id: str,
    bucket_name: str,
    file_path: str,
    file_name: str,
    provider_secret_env_name: str,
    release_name: str,
) -> tuple:
    """
    Uploads a file to a Google Cloud Storage (GCS) bucket using the GCSHook class.

    Args:
        provider (str): The name of the cloud provider. In this case, it should be "gcs".
        conn_id (str): The connection ID for the Google Cloud Storage (GCS) account.
        bucket_name (str): The name of the GCS bucket.
        file_path (str): The path to the file to be uploaded.
        file_name (str): The name of the file to be uploaded.
        provider_secret_env_name (str): The name of the environment variable containing the credentials for the GCS account.
        release_name (str): The name of the release.

    Returns:
        tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
    """
    try:
        from airflow.providers.google.cloud.operators.gcs import GCSHook

        logging.info("Connecting to gcs service to validate bucket connection........")
        if conn_id == "" or conn_id is None:
            logging.info("fallback to google connection default connection flow")
            os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = os.getenv(
                provider_secret_env_name
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
                provider_secret_env_name
            )
            gcsClass = GCSHook()
        else:
            logging.info("connecting to google service from conn_id flow")
            gcsClass = GCSHook(gcp_conn_id=conn_id)
        gcsClass.upload(
            bucket_name=bucket_name,
            filename=file_path,
            object_name=file_name,
        )
    except Exception as e:
        return False, release_name, provider, e


def AzureCloudProvider(
    provider: str,
    conn_id: str,
    bucket_name: str,
    file_path: str,
    file_name: str,
    release_name: str,
) -> tuple:
    """
    Uploads a file to an Azure blob container using the provided connection and credentials.

    Args:
        provider (str): The name of the cloud provider (Azure in this case).
        conn_id (str): The connection ID for the Azure blob storage account.
        bucket_name (str): The name of the blob container where the file will be uploaded.
        file_path (str): The local path of the file to be uploaded.
        file_name (str): The name of the file to be uploaded.
        release_name (str): The name of the release being uploaded.

    Returns:
        tuple: A tuple containing a boolean indicating if the upload was successful, the name of the release, the name of the cloud provider, and any error message if the upload was unsuccessful.
    """
    try:
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        logging.info(
            "Connecting to azure blob service to validate bucket connection........"
        )
        azureClass = WasbHook(wasb_conn_id=conn_id)
        with open(file_path, "rb") as f:
            azureClass.upload(
                container_name=bucket_name,
                data=f,
                blob_name=file_name,
            )
    except Exception as e:
        return False, release_name, provider, e


def LocalCloudProvider(
    provider: str, bucket_name: str, file_path: str, file_name: str, release_name: str
) -> tuple:
    """
    Uploads a file to a local directory.

    Args:
        provider (str): The name of the cloud provider (Local in this case).
        bucket_name (str): The name of the local directory where the file will be uploaded.
        file_path (str): The local path of the file to be uploaded.
        file_name (str): The name of the file to be uploaded.
        release_name (str): The name of the release being uploaded.

    Returns:
        tuple: A tuple containing a boolean indicating if the upload was successful, the name of the release, the name of the cloud provider, and any error message if the upload was unsuccessful.
    """
    try:
        from shutil import copyfile

        logging.info("Connecting to local storage ........")
        destPath = os.path.join(f"{bucket_name}", f"{release_name}")
        os.makedirs(destPath, exist_ok=True)
        copyfile(file_path, f"{bucket_name}/{file_name}")
        return True, release_name, provider, None
    except Exception as e:
        return False, release_name, provider, e
