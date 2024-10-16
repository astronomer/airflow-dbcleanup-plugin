from __future__ import annotations
import csv
import os
import logging
import json

from flask import Blueprint, request, Response, flash, redirect, render_template, g
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from flask_login.utils import _get_user
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request
from functools import wraps
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text


from airflow.www.app import csrf
from airflow import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils import db_cleanup, dates
from airflow.utils.db_cleanup import config_dict
from airflow.settings import conf

from .cloud_providers import ProviderFactory
from .utils import env_check
from typing import Callable, TypeVar, cast, Sequence

T = TypeVar("T", bound=Callable)

__version__ = "1.0.4"

log = logging.getLogger(__name__)


def has_access_(permissions: Sequence[tuple[str, str]]) -> Callable[[T], T]:
    method: str = permissions[0][0]
    resource_type: str = permissions[0][1]

    from airflow.utils.net import get_hostname
    from airflow.www.extensions.init_auth_manager import get_auth_manager

    def decorated(*, is_authorized: bool, func: Callable, args, kwargs):
        """
        Define the behavior whether the user is authorized to access the resource.
        :param is_authorized: whether the user is authorized to access the resource
        :param func: the function to call if the user is authorized
        :param args: the arguments of ``func``
        :param kwargs: the keyword arguments ``func``
        :meta private:
        """
        if is_authorized:
            return func(*args, **kwargs)
        elif get_auth_manager().is_logged_in() and not g.user.perms:
            return (
                render_template(
                    "airflow/no_roles_permissions.html",
                    hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "redact",
                    logout_url=get_auth_manager().get_url_logout(),
                ),
                403,
            )
        else:
            access_denied = conf.get("webserver", "access_denied_message")
            flash(access_denied, "danger")
        return redirect(get_auth_manager().get_url_login(next=request.url))

    def has_access_decorator(func: T):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return decorated(
                is_authorized=get_auth_manager().is_authorized(method=method, resource_type=resource_type),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, wrapper)

    return has_access_decorator

# This code is introduced to maintain backward compatibility, since with airflow > 2.8
# method `has_access` will be deprecated in airflow.www.auth.
try:
    from airflow.www.auth import has_access
except ImportError:
    has_access = has_access_

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
    validate_days = abs(request.args.get("olderThan", type=int))
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
    validate_table_names = [
        x.strip()
        for x in request.args.get("tableNames", default="").split(",")
        if x.strip() != ""
    ]
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
        table_names = list(validate_table_names)

    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e

    else:
        log.info(
            f"User passing values to export function dry_run : {dry_run}, days: {days}, export_format: {export_format}, output_path: {output_path}, provider_name: {provider}, bucket_name: {bucket_name}, drop_archives: {drop_archives}, deployment_name: {deployment_name}, conn_id: {conn_id}, provider_secret_env: {provider_secret_env_name}, table_names: {table_names}"
        )
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
            table_names=table_names,
        )


def create_folder(folder_path: str):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)


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
    Return the effective table names and their corresponding configuration based on the given list of table names.
    If no table names are specified, returns all table names in the global configuration.
    Raises SystemExit if no valid table names are selected.
    """
    desired_table_names = set(
        table_names if len(table_names) > 0 else config_dict.keys()
    )
    # desired_table_names = set(table_names or config_dict)
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
    create_folder(output_path)
    file_name = "verify.txt"
    file_path = os.path.join(output_path, f"{file_name}")
    file_name = f"{release_name}/{file_name}"
    with open(file_path, "w") as file:
        data = f"Adding demo content for {release_name} to verify bucket existence"
        file.write(data)
    provider_base = ProviderFactory[provider](provider)
    status, release_name, provider, e = provider_base.upload(
        conn_id=conn_id,
        bucket_name=bucket_name,
        file_path=file_path,
        file_name=file_name,
        provider_secret_env_name=provider_secret_env_name,
        release_name=release_name
    )

    if not status:
        return False, release_name, provider, e, "Failed to verify provider credentials"

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
    resource_name = "dbcleanup"
    default_view = "dbcleanup"
    csrf_exempt = False
    base_permissions = ["can_access_dbcleanup"]
    allow_browser_login = True

    @expose("api/v1/dbcleanup", methods=["POST", "GET"])
    @env_check("ASTRONOMER_ENVIRONMENT")
    @has_access([
        ("can_access_dbcleanup", "AstronomerDbcleanup"),
    ])
    @csrf.exempt
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
