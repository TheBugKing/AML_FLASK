from enum import Enum


class Messages(Enum):
    user_name_exception = "UserName not in request headers"
    request_gen_exception = "{status_code}, {error_message}"
    bad_request_exception = ("{status_code}, Bad Request, parameters missing or invalid value provided for parameters: "
                             " {params}")
    successfull_request = "{status_code}, request completed"


class StringConstants(Enum):
    latest = "latest"

class AmlJobStatusConstants(Enum):
    completed = "Completed"
