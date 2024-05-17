from enum import Enum


class ExceptionMessages(Enum):
    user_name_exception = "UserName not in request headers"
    request_gen_exception = "{status_code}, {error_message}"
    bad_request_exception = ("{status_code}, Bad Request, parameters missing or invalid value provided for parameters: "
                             " {params}")


class StringConstants(Enum):
    latest = "latest"
