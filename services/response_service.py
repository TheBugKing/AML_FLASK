from utils.constants import ExceptionMessages


class ResponseService:
    @staticmethod
    def get_exception_message(msg, status_code):
        res = ExceptionMessages.request_gen_exception.value.format(status_code=status_code,
                                                                   error_message=msg)
        return {'error': res}

    @staticmethod
    def get_bad_request_message(params, status_code):
        res = ExceptionMessages.bad_request_exception.value.format(status_code=status_code,
                                                                   params=params)
        return {'error': res}
