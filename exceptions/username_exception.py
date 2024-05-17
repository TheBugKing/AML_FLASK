class UserNameException(Exception):
    def __init__(self, msg: str):
        msg = f"UserNameException: {msg}"
        super().__init__(msg)
