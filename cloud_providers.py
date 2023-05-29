import os
import logging
import shutil

log = logging.getLogger(__name__)


class CloudProvider:
    def __init__(self, provider: str, **kwargs):
        """
        Initializes the CloudProvider class.

        Args:
            provider (str): The name of the cloud provider.
            **kwargs: Arbitrary keyword arguments.
        """
        self.provider = provider
        self.kwargs = kwargs

    def upload(self) -> tuple:
        """
        Uploads a file to a cloud provider.

        Returns:
            tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
        """
        raise NotImplementedError


class AwsCloudProvider(CloudProvider):
    def __init__(self, provider: str, **kwargs):
        """
        Initializes the AwsCloudProvider class.

        Args:
            provider (str): The name of the cloud provider.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(provider, **kwargs)

    def upload(self, **kwargs) -> tuple:
        """
        Uploads a file to an Amazon S3 bucket using the S3Hook class.

        Returns:
            tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
        """
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            logging.info(
                "Connecting to aws s3 service to validate bucket connection........"
            )
            s3Class = S3Hook(aws_conn_id=kwargs["conn_id"])
            s3Class.check_for_bucket(bucket_name=kwargs["bucket_name"])
            with open(kwargs["file_path"], "rb") as f:
                s3Class._upload_file_obj(
                    file_obj=f,
                    key=kwargs["file_name"],
                    bucket_name=kwargs["bucket_name"],
                    replace=kwargs["replace"] or False,
                )
            log.info("data sent to s3 bucket sucessfully")
            return True, kwargs["release_name"], self.provider, None
        except Exception as e:
            return False, kwargs["release_name"], self.provider, e


class GcsCloudProvider(CloudProvider):
    def __init__(self, provider: str, **kwargs):
        """
        Initializes the GcsCloudProvider class.

        Args:
            provider (str): The name of the cloud provider.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(provider, **kwargs)

    def upload(self, **kwargs) -> tuple:
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

            logging.info(
                "Connecting to gcs service to validate bucket connection........"
            )
            if not kwargs["conn_id"] or kwargs["conn_id"] is None:
                if (
                    os.getenv(kwargs["provider_secret_env_name"])
                    == "google-cloud-platform://"
                ):
                    logging.info(
                        "configuring workload identity for  google connection flow"
                    )
                    os.environ[
                        "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"
                    ] = "google-cloud-platform://"
                else:
                    logging.info(
                        "fallback to google connection default connection flow"
                    )
                    os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = os.getenv(
                        kwargs["provider_secret_env_name"]
                    )
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
                        kwargs["provider_secret_env_name"]
                    )
                gcsClass = GCSHook()
            else:
                logging.info("Connecting to google service using conn_id flow")
                gcsClass = GCSHook(gcp_conn_id=kwargs["conn_id"])
            gcsClass.upload(
                bucket_name=kwargs["bucket_name"],
                filename=kwargs["file_path"],
                object_name=kwargs["file_name"],
            )
            return True, kwargs["release_name"], self.provider, None
        except Exception as e:
            return False, kwargs["release_name"], self.provider, e


class AzureCloudProvider(CloudProvider):
    def __init__(self, provider: str, **kwargs):
        """
        Initializes the AzureCloudProvider class.

        Args:
            provider (str): The name of the cloud provider.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(provider, **kwargs)

    def upload(self, **kwargs) -> tuple:
        """
        Uploads a file to an Azure Blob Storage (ABS) container using the WasbHook class.

        Args:
            provider (str): The name of the cloud provider. In this case, it should be "azure".
            conn_id (str): The connection ID for the Azure Blob Storage (ABS) account.
            bucket_name (str): The name of the ABS container.
            file_path (str): The path to the file to be uploaded.
            file_name (str): The name of the file to be uploaded.
            provider_secret_env_name (str): The name of the environment variable containing the credentials for the ABS account.
            release_name (str): The name of the release.

        Returns:
            tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
        """
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

            logging.info(
                "Connecting to azure blob service to validate bucket connection........"
            )
            azureClass = WasbHook(wasb_conn_id=kwargs["conn_id"])
            with open(kwargs["file_path"], "rb") as f:
                azureClass.upload(
                    container_name=kwargs["bucket_name"],
                    data=f,
                    blob_name=kwargs["file_name"],
                )
            return True, kwargs["release_name"], self.provider, None
        except Exception as e:
            return False, kwargs["release_name"], self.provider, e


class LocalProvider(CloudProvider):
    def __init__(self, provider: str, **kwargs):
        """
        Initializes the LocalCloudProvider class.

        Args:
            provider (str): The name of the cloud provider.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(provider, **kwargs)

    def upload(self, **kwargs) -> tuple:
        """
        Uploads a file to a local directory.

        Args:
            provider (str): The name of the cloud provider. In this case, it should be "local".
            file_path (str): The path to the file to be uploaded.
            file_name (str): The name of the file to be uploaded.
            release_name (str): The name of the release.

        Returns:
            tuple: A tuple containing a boolean value indicating success or failure, the release name, the provider, and any exception that occurred.
        """
        try:
            destinationPath = os.path.join(
                "/tmp", kwargs["bucket_name"], kwargs["file_name"]
            )
            if not os.path.exists(destinationPath):
                os.makedirs(os.path.dirname(destinationPath), exist_ok=True)
            shutil.copy(kwargs["file_path"], destinationPath)
            logging.debug(f"File copied to {destinationPath}")
            return True, kwargs["release_name"], self.provider, None
        except Exception as e:
            return False, kwargs["release_name"], self.provider, e


ProviderFactory = {
    "aws": AwsCloudProvider,
    "gcp": GcsCloudProvider,
    "azure": AzureCloudProvider,
    "local": LocalProvider,
}
