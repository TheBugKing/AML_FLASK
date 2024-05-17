from utils.constants import Messages


class ResponseService:
    @staticmethod
    def get_exception_message(msg, status_code):
        res = Messages.request_gen_exception.value.format(status_code=status_code,
                                                                   error_message=msg)
        return {'error': res}

    @staticmethod
    def get_bad_request_message(params, status_code):
        res = Messages.bad_request_exception.value.format(status_code=status_code,
                                                                   params=params)
        return {'error': res}
    
    @staticmethod
    def get_success_message(status_code):
        res = Messages.successfull_request.value.format(status_code=status_code)
        return {'success': res}
    