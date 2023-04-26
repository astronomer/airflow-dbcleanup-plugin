from flask import jsonify
import os


class env_check(object):
    def __init__(self, env_var):
        self.env_var = env_var

    def __call__(self, f):
        def g(*args, **kwargs):
            error_message = f"Not Implemented, {self.env_var} not configured"
            response = jsonify(error=error_message)
            response.status_code = 501
            return response

        if  os.environ.get(self.env_var) is None or os.environ.get(self.env_var) == "software":
            return f
        print(f"{self.env_var} not supplied, {f.__name__} route disabled")
        return g
