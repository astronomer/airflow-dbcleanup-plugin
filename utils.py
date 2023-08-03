from flask import Response
import os
import json


class env_check(object):
    def __init__(self, env_var):
        self.env_var = env_var

    def __call__(self, f):
        def g(*args, **kwargs):
            res = {
                "jobStatus": "failed",
                "statusCode": 501,
                "message": "This feature is only supported on Astronomer Software and Astronomer Nebula",
            }
            response = Response(json.dumps(res), mimetype="application/json")
            response.status_code = 501
            return response

        if (
            os.environ.get(self.env_var) is None
            or os.environ.get(self.env_var) == "software"
        ):
            return f
        return g
