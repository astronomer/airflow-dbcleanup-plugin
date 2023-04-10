import csv
import os
import logging
import json

from flask import Blueprint, request, Response
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from flask_login.utils import _get_user
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text

from airflow.www.app import csrf
from airflow import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils import db_cleanup, dates
from airflow.utils.db_cleanup import config_dict
from airflow.settings import conf
from airflow.security import permissions
from airflow.www import auth

from .cloud_providers import ProviderFactory

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

ARCHIVE_TABLE_PREFIX = "_airflow_deleted__"


# code sourced from airflow logic
def getboolean(val: str) -> bool:
    """
    Converts a string value to a boolean value.
    Args:
        val (str): The string value to be converted to boolean.
    Returns:
        bool: The boolean value of the given string.
    Raises:
        Exception: If the given string cannot be converted to a boolean.
    Example:
        >>> getboolean("True")
        True
        >>> getboolean("False")
        False
    """
    val = val.lower().strip()
    if val in {"t", "true", "1"}:
        return True
    elif val in {"f", "false", "0"}:
        return False
    else:
        raise ValueError(
            "Failed to convert value to bool. Expected bool but got something else."
            f'Current value: "{val}".'
        )


# Added custom export function to be called via endpoint
def _airflow_dbexport():
    validate_dry_run = request.args.get("dryRun", type=str, default="True")
    validate_days = request.args.get("olderThan", type=int)
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
    validate_table_names = request.args.getlist("tableNames")
    try:
        dry_run = getboolean(validate_dry_run)
        days = int(validate_days)
        export_format = str(validate_export_format)
        output_path = str(validate_output_path)
        provider = str(validate_provider)
        bucket_name = str(validate_bucket_name)
        drop_archives = getboolean(validate_drop_archives)
        deployment_name = str(validate_deployment_name)
        conn_id = str(validate_conn_id)
        provider_secret_env_name = str(validate_provider_secret_env_name)
        validate_table_names = list(validate_table_names)

    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e

    else:
        return export_cleaned_records(
            dry_run=dry_run,
            days=days,
            export_format=export_format,
            output_path=output_path,
            drop_archives=drop_archives,
            provider=provider,
            conn_id=conn_id,
            provider_secret_env_name=provider_secret_env_name,
            bucket_name=bucket_name,
            deployment_name=deployment_name,
            table_names=validate_table_names,
        )


# Adopted most of the work from @ephraimbuddy
def _dump_table_to_file(
    *, target_table: str, file_path: str, export_format: str, session
) -> None:
    """
    Dumps the data from the given database table into a file in the specified export format.

    Args:
        target_table (str): The name of the database table to export data from.
        file_path (str): The path of the file to export the data to.
        export_format (str): The format in which to export the data. Currently, only 'csv' format is supported.
        session: A database session object to execute the export query.

    Returns:
        None

    Raises:
        AirflowException: If the specified export format is not supported.

    Example usage:
        _dump_table_to_file(target_table='users', file_path='/path/to/exported_file.csv', export_format='csv', session=db_session)
    """
    if export_format != "csv":
        raise AirflowException(
            f"Export format {export_format} is not supported. Currently supported formats is csv"
        )
    cursor = session.execute(text(f"SELECT * FROM {target_table}"))
    batch_size = 5000
    while rows := cursor.fetchmany(batch_size):
        with open(file_path, "a+") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(cursor.keys())
            csv_writer.writerows(rows)


def _effective_table_names(*, table_names: list[str]):
    """
    Returns the effective table names and their corresponding configuration based on the given list of table names.
    If no table names are specified, returns all table names in the global configuration.
    Raises SystemExit if no valid table names are selected.
    """
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
        raise AirflowException(
            "No tables selected for DBcleanup. Please choose valid table names."
        )
    return effective_table_names, effective_config_dict


@provide_session
def export_cleaned_records(
    dry_run,
    days,
    export_format,
    output_path,
    provider,
    bucket_name,
    conn_id,
    provider_secret_env_name,
    drop_archives,
    deployment_name,
    table_names,
    session: Session = NEW_SESSION,
):
    """Export cleaned data to the given output path in the given format."""
    # Logic to send data to cloud storage based on the provider type s3, gcs, azure
    release_name = deployment_name or conf.get(
        "kubernetes_labels", "release", fallback="airflow"
    )
    if provider not in ProviderFactory.keys():
        raise AirflowException(
            f"Provider {provider} is not supported. Currently supported providers are aws, gcp, azure and local"
        )
    if not dry_run:
        logging.info("DBcleanup initiated..... ")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)),
            table_names=table_names,
            confirm=False,
        )
        logging.info("DBcleanup completed successfully....")
        logging.info("DBcleanup proceeding with export selection")
        effective_table_names, _ = _effective_table_names(table_names=table_names)
        inspector = inspect(session.bind)
        db_table_names = [
            x for x in inspector.get_table_names() if x.startswith(ARCHIVE_TABLE_PREFIX)
        ]
        export_count = 0
        dropped_count = 0
        for table_name in db_table_names:
            if all(f"__{x}__" not in table_name for x in effective_table_names):
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
            provider = ProviderFactory[provider](provider)
            status, release_name, provider, e = provider.upload(
                conn_id=conn_id,
                bucket_name=bucket_name,
                file_path=file_path,
                file_name=file_name,
                provider_secret_env_name=provider_secret_env_name,
                release_name=release_name,
            )
            if not status:
                return False, release_name, provider, e, ""
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

        return (
            True,
            release_name,
            provider,
            "",
            f"{release_name} data exported to provider {provider} completed" "",
        )
    else:
        logging.info("Performing DBcleanup dry run ...")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)),
            dry_run=True,
            table_names=table_names,
        )
        logging.info("DBcleanup dry run completed ")
        return (
            True,
            release_name,
            provider,
            "skipping export",
            f"skipping export for {release_name} as dry run is enabled",
        )


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
            success, release, provider, e, msg = _airflow_dbexport()
            if success:
                res = {
                    "deploymentName": f"{release}",
                    "jobStatus": "success",
                    "statusCode": 200,
                    "message": msg,
                }
                response = Response(json.dumps(res), mimetype="application/json")
                response.status = 200
            else:
                res = {
                    "deploymentName": f"{release}",
                    "jobStatus": "failed",
                    "statusCode": 500,
                    "message": f"db export failed with exception {e}",
                }
                response = Response(json.dumps(res), mimetype="application/json")
                response.status = 500
            return response
        except Exception as e:
            res = {
                "jobStatus": "failed",
                "statusCode": 500,
                "message": f"db export failed with exception {e}",
            }
            response = Response(json.dumps(res), mimetype="application/json")
            response.status = 500
            return response


# Defining the plugin class
class AstronomerPlugin(AirflowPlugin):
    name = "Astronomer Dbcleanup"
    flask_blueprints = [bp]
    appbuilder_views = [
        {
            "view": AstronomerDbcleanup(),
        }
    ]
