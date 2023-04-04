import os
import logging
log = logging.getLogger(__name__)

def AwsCloudProvider(provider, conn_id,bucket_name,file_path,file_name,provider_secret_env_name,release_name):
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        logging.info(
            "Connecting to aws s3 service to validate bucket connection........"
        )
        s3Class = S3Hook(aws_conn_id=conn_id)
        s3Class.check_for_bucket(bucket_name=bucket_name)
        with open(file_path, "rb") as f:
            s3Class._upload_file_obj(
                file_obj=f, key=file_name, bucket_name=bucket_name
            )
        log.info("data sent to s3 bucket sucessfully")
    except Exception as e:
        return False, release_name, provider, e
    
def GcsCloudProvider(provider,conn_id,bucket_name,file_path,file_name,provider_secret_env_name,release_name):
    try:
        from airflow.providers.google.cloud.operators.gcs import GCSHook
        logging.info(
            "Connecting to gcs service to validate bucket connection........"
        )
        logging.info(f"what comes here as conn_id {conn_id}")
        if conn_id == "" or conn_id is None:
            logging.info("relying on google connection default logic")
            os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = os.getenv(
                provider_secret_env_name
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
                provider_secret_env_name
            )
            gcsClass = GCSHook()
        else:
            gcsClass = GCSHook(gcp_conn_id=conn_id)
        gcsClass.upload(
            bucket_name=bucket_name,
            filename=file_path,
            object_name=file_name,
        )
    except Exception as e:
        return False, release_name, provider, e


def AzureCloudProvider(provider,conn_id,bucket_name,file_path,file_name,provider_secret_env_name,release_name):
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
    
def LocalCloudProvider(provider,bucket_name,file_path,file_name,release_name):
    try:
        from shutil import copyfile
        logging.info("Connecting to local storage ........")
        destPath = os.path.join(f"{bucket_name}", f"{release_name}")
        os.makedirs(destPath, exist_ok=True)
        copyfile(file_path, f"{bucket_name}/{file_name}")
        return True, release_name, provider, None
    except Exception as e:
            return False, release_name, provider, e