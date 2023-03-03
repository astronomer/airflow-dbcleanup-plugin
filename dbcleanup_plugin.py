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
    if val in ("t", "true", "1"):
        return True
    elif val in ("f", "false", "0"):
        return False
    else:
        raise Exception(
            f"Failed to convert value to bool. Expected bool but got something else."
            f'Current value: "{val}".'
        )


def dbcleanup_report():
    validate_days = request.args.get("days", type=int)
    validate_dry_run = request.args.get("dry_run", type=str)
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
    validate_export = request.args.get("export", type=str, default=False)
    validate_export_format = request.args.get("export_format", type=str, default="csv")
    validate_output_path = request.args.get("output_path", type=str, default="/tmp")
    validate_drop_archives = request.args.get("drop_archives", type=str, default=False)
    try:
        export = getboolean(validate_export)
        export_format = str(validate_export_format)
        output_path = str(validate_output_path)
        drop_archives = getboolean(validate_drop_archives)

    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e
    else:
        return export_cleaned_records(
            export=export,
            export_format=export_format,
            output_path=output_path,
            drop_archives=drop_archives,
        )


@provide_session
def cleanupdb(session, days, dry_run) -> Any:
    logging.info("CLEANUP FUNCTION")
    if dry_run:
        logging.info("performing DBcleanup dry run ...")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), dry_run=True
        )
        logging.info("DBcleanup dry completed ")
    else:
        logging.info("DB cleanup initiated..... ")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), confirm=False
        )
        logging.info("DB cleanup completed successfully....")
    # return {"status": "cleanup job executed sucessfully"}


# Adopted most of the work from @ephraimbuddy


def _dump_table_to_file(*, target_table, file_path, export_format, session):
    if export_format == "csv":
        with open(file_path, "w") as f:
            csv_writer = csv.writer(f)
            cursor = session.execute(text(f"SELECT * FROM {target_table}"))
            csv_writer.writerow(cursor.keys())
            csv_writer.writerows(cursor.fetchall())
    else:
        raise AirflowException(f"Export format {export_format} is not supported.Current supported formats are csv")


# self comment - we need to optimise this function for custom use case
# currently we are by passing it
def _confirm_drop_archives(*, tables: list[str]):
    # question = (
    #    f"You have requested that we drop archived records for tables {tables!r}.\n"
    #    f"This is irreversible.  Consider backing up the tables first \n"
    #    f"Enter 'drop archived tables' (without quotes) to proceed."
    # )
    # print(question)
    # answer = input().strip()
    # if not answer == "drop archived tables":
    #    raise SystemExit("User did not confirm; exiting.")
    logging.info("Dropping records from the database.")


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
            "No tables selected for db cleanup. Please choose valid table names."
        )
    return effective_table_names, effective_config_dict


@provide_session
def export_cleaned_records(
    export_format,
    output_path,
    export,
    drop_archives,
    table_names=None,
    session: Session = NEW_SESSION,
):
    """Export cleaned data to the given output path in the given format."""
    if export:
        logging.info("Proceeding with export selection")
        effective_table_names, _ = _effective_table_names(table_names=table_names)
        if drop_archives:
            _confirm_drop_archives(tables=sorted(effective_table_names))
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
            _dump_table_to_file(
                target_table=table_name,
                file_path=os.path.join(output_path, f"{table_name}.{export_format}"),
                export_format=export_format,
                session=session,
            )

            export_count += 1
            if drop_archives:
                logging.info("Dropping archived table %s", table_name)
                session.execute(text(f"DROP TABLE {table_name}"))
                dropped_count += 1
        logging.info(
            "Total exported tables: %s, Total dropped tables: %s",
            export_count,
            dropped_count,
        )
    else:
        logging.info("Skipping export")


# Creating a flask appbuilder BaseView
class AstronomerDbcleanup(AppBuilderBaseView):
    default_view = "dbcleanup"

    @expose("api/v1/dbcleanup", methods=["POST", "GET"])
    @csrf.exempt
    # disabled jwt auth for rest point
    # @jwt_token_secure
    def tasks(self):
        try:
            dbcleanup_report()
            # export_cleaned_records(export_format="csv",output_path="/tmp",drop_archives=True)
            # Additional function to call export and cleanup from db
            _airflow_dbexport()
            return {"status": "completed"}
        except Exception as e:
            return {"status": f"failed with {e}"}


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
