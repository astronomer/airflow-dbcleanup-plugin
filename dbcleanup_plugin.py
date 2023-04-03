import csv
import os
import logging
from typing import Any

from flask import Blueprint, request
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from flask_login.utils import _get_user
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text

from airflow.www.app import csrf
from airflow import configuration, AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils import db_cleanup, dates
from airflow.utils.db_cleanup import config_dict
from airflow.settings import conf
from airflow.security import permissions
from airflow.www import auth

__version__ = "1.0.0"

log = logging.getLogger(__name__)


def jwt_token_secure(func):
    def jwt_secure_check(arg):
        log.info("Rest_API_Plugin.jwt_token_secure() called")
        if _get_user().is_anonymous is False:
            return func(arg)
        verify_jwt_in_request()
        return jwt_required(func(arg))

    return jwt_secure_check


bp = Blueprint(
    "astronomer_dbcleanup",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/",
)

airflow_webserver_base_url = configuration.get("webserver", "BASE_URL")
ARCHIVE_TABLE_PREFIX = "_airflow_deleted__"


# picked code from airflow logic
def getboolean(val: str) -> bool:
    val = val.lower().strip()
    if val in {"t", "true", "1"}:
        return True
    elif val in {"f", "false", "0"}:
        return False
    else:
        raise Exception(
            f"Failed to convert value to bool. Expected bool but got something else."
            f'Current value: "{val}".'
        )


def dbcleanup_report():
    validate_days = request.args.get("olderThan", type=int)
    validate_dry_run = request.args.get("dryRun", type=str, default="True")
    try:
        days = int(validate_days)
        dry_run = getboolean(validate_dry_run)

    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e
    else:
        return cleanupdb(days=days, dry_run=dry_run)


# Added custom export function to be called via endpoint


def _airflow_dbexport():
    validate_export_format = request.args.get("exportFormat", type=str, default="csv")
    validate_output_path = request.args.get("outputPath", type=str, default="/tmp")
    validate_provider = request.args.get("provider", type=str, default="")
    validate_conn_id = request.args.get("connectionId", type=str, default="")
    validate_bucket_name = request.args.get("bucketName", type=str, default="")
    validate_provider_secret_env_name = request.args.get(
        "providerEnvSecretName", type=str, default=""
    )
    validate_drop_archives = request.args.get("purgeTable", type=str, default="False")
    validate_deployment_name = request.args.get("deploymentName", type=str, default="")
    validate_dry_run = request.args.get("dryRun", type=str, default="True")
    try:
        export_format = str(validate_export_format)
        output_path = str(validate_output_path)
        provider = str(validate_provider)
        bucket_name = str(validate_bucket_name)
        drop_archives = getboolean(validate_drop_archives)
        deployment_name = str(validate_deployment_name)
        conn_id = str(validate_conn_id)
        provider_secret_env_name = str(validate_provider_secret_env_name)
        dry_run = getboolean(validate_dry_run)

    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e
    else:
        return export_cleaned_records(
            export_format=export_format,
            output_path=output_path,
            drop_archives=drop_archives,
            provider=provider,
            conn_id=conn_id,
            provider_secret_env_name=provider_secret_env_name,
            bucket_name=bucket_name,
            deployment_name=deployment_name,
            dry_run=dry_run,
        )


@provide_session
def cleanupdb(session, days, dry_run) -> Any:
    if dry_run:
        logging.info("Performing DBcleanup dry run ...")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), dry_run=True
        )
        logging.info("DBcleanup dry run completed ")
    else:
        logging.info("DBcleanup initiated..... ")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), confirm=False
        )
        logging.info("DBcleanup completed successfully....")


# Adopted most of the work from @ephraimbuddy
def _dump_table_to_file(*, target_table, file_path, export_format, session):
    if export_format == "csv":
        with open(file_path, "w") as f:
            csv_writer = csv.writer(f)
            cursor = session.execute(text(f"SELECT * FROM {target_table}"))
            csv_writer.writerow(cursor.keys())
            # csv_writer.writerows(cursor.fetchall())
            batch_size = 5000
            while True:
                if rows := cursor.fetchmany(batch_size):
                    csv_writer.writerows(rows)
                else:
                    break
    else:
        raise AirflowException(
            f"Export format {export_format} is not supported.Currently supported formats are csv"
        )


def _effective_table_names(*, table_names: list[str]):
    desired_table_names = set(table_names or config_dict)
    effective_config_dict = {
        k: v for k, v in config_dict.items() if k in desired_table_names
    }
    effective_table_names = set(effective_config_dict)
    if desired_table_names != effective_table_names:
        outliers = desired_table_names - effective_table_names
        logging.warning(
            "The following table(s) are not valid choices and will be skipped: %s",
            sorted(outliers),
        )
    if not effective_table_names:
        raise SystemExit(
            "No tables selected for DBcleanup. Please choose valid table names."
        )
    return effective_table_names, effective_config_dict


@provide_session
def export_cleaned_records(
    dry_run,
    export_format,
    output_path,
    provider,
    bucket_name,
    conn_id,
    provider_secret_env_name,
    drop_archives,
    deployment_name,
    table_names=None,
    session: Session = NEW_SESSION,
):
    """Export cleaned data to the given output path in the given format."""
    # Logic to send data to cloud storage based on the provider type s3, gcs, azure
    if deployment_name:
        release_name = deployment_name
    else:
        try:
            release_name = conf.get("kubernetes_labels", "release")
        except Exception:
            release_name = "airflow"
    if not dry_run:
        logging.info("Proceeding with export selection")
        effective_table_names, _ = _effective_table_names(table_names=table_names)
        inspector = inspect(session.bind)
        db_table_names = [
            x for x in inspector.get_table_names() if x.startswith(ARCHIVE_TABLE_PREFIX)
        ]
        export_count = 0
        dropped_count = 0
        for table_name in db_table_names:
            if not any("__" + x + "__" in table_name for x in effective_table_names):
                continue
            logging.info("Exporting table %s", table_name)
            os.makedirs(output_path, exist_ok=True)
            _dump_table_to_file(
                target_table=table_name,
                file_path=os.path.join(output_path, f"{table_name}.{export_format}"),
                export_format=export_format,
                session=session,
            )
            export_count += 1
            file_path = os.path.join(output_path, f"{table_name}.{export_format}")
            file_name = f"{release_name}/{table_name}.{export_format}"
            if provider == "aws":
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
                    log.info("Data sent to s3 bucket sucessfully")
                except Exception as e:
                    return False, release_name, provider, e
            elif provider == "gcp":
                try:
                    from airflow.providers.google.cloud.operators.gcs import GCSHook

                    logging.info(
                        "Connecting to gcs service to validate bucket connection........"
                    )
                    if conn_id == "" or conn_id is None:
                        logging.info(
                            "fallback to google connection default connection flow"
                        )
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
            elif provider == "azure":
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
            elif provider == "local":
                try:
                    logging.info("Connecting to local storage ........")
                    from shutil import copyfile

                    destPath = os.path.join(f"{bucket_name}", f"{release_name}")
                    os.makedirs(destPath, exist_ok=True)
                    copyfile(file_path, f"{bucket_name}/{file_name}")
                except Exception as e:
                    return False, release_name, provider, e
            else:
                raise AirflowException(
                    f"Cloud provider {provider} is not supported.Supported providers are aws, gcp, azure, local"
                )
            if drop_archives:
                os.remove(file_path)
                logging.info("Dropping archived table %s", table_name)
                session.execute(text(f"DROP TABLE {table_name}"))
                dropped_count += 1
        logging.info(
            "Total exported tables: %s, Total dropped tables: %s",
            export_count,
            dropped_count,
        )
        return True, release_name, provider, ""
    else:
        logging.info("Skipping export")
        return False, release_name, provider, "skipping export"


# Creating a flask appbuilder BaseView
class AstronomerDbcleanup(AppBuilderBaseView):
    default_view = "dbcleanup"

    @expose("api/v1/dbcleanup", methods=["POST", "GET"])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_RESCHEDULE),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_RESCHEDULE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TRIGGER),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TRIGGER),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_PASSWORD),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
        ]
    )
    @csrf.exempt
    # disabled jwt auth for rest point
    # @jwt_token_secure
    def tasks(self):
        try:
            dbcleanup_report()
            # Additional function to call export and cleanup from db
            export, release, provider, e = _airflow_dbexport()
            if export:
                return {
                    "deploymentName": f"{release}",
                    "jobStatus": "success",
                    "statusCode": 200,
                    "message": f"{release} data exported to provider {provider} completed",
                }
            else:
                return {
                    "deploymentName": f"{release}",
                    "jobStatus": "failed",
                    "statusCode": 500,
                    "message": f"db export failed with exception {e}",
                }
        except Exception as e:
            return {
                "jobStatus": "failed",
                "statusCode": 500,
                "message": f"db export failed with exception {e}",
            }


# Defining the plugin class
class AstronomerPlugin(AirflowPlugin):
    name = "Astronomer Dbcleanup"
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [
        {
            "view": AstronomerDbcleanup(),
        }
    ]
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
