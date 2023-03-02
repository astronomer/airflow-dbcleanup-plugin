import logging
from typing import Any

from flask import Blueprint, request
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

from airflow.www.app import csrf
from flask_login.utils import _get_user
from flask_jwt_extended.view_decorators import jwt_required, verify_jwt_in_request
from airflow import configuration
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import provide_session
from airflow.utils import db_cleanup, dates

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


def dbcleanup_report():
    validate_days = request.args.get("days", type=int)
    validate_dry_run = request.args.get("dry_run", type=bool)
    try:
        days = int(validate_days)
        dry_run = bool(validate_dry_run)
    except ValueError as e:
        log.error(f"Validation Failed for request args: {e}")
        raise e
    else:
        return cleanupdb(days=days, dry_run=dry_run)


@provide_session
def cleanupdb(session, days, dry_run) -> Any:
    logging.info("CLEANUP FUNCTION")
    if dry_run:
        logging.info("performing DBcleanup dry run ...")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), dry_run=True
        )
        logging.info("DBcleanup dry completed...")
    else:
        logging.info("DB cleanup initiated..... ")
        db_cleanup.run_cleanup(
            clean_before_timestamp=dates.days_ago(int(days)), confirm=False
        )
        logging.info("DB cleanup completed successfully....")
    #return {"status": "cleanup job executed sucessfully"}


# Creating a flask appbuilder BaseView
class AstronomerDbcleanup(AppBuilderBaseView):
    default_view = "dbcleanup"

    @expose("api/v1/dbcleanup", methods=["POST", "GET"])
    @csrf.exempt
    # disabled jwt auth for rest point
    # @jwt_token_secure
    def tasks(self):
        # logging.info("API VIEW")
        dbcleanup_report()
        return {"status": "completed"}


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
