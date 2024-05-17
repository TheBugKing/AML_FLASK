class UserNameException(Exception):
    def __int__(self, msg: str):
        msg = f"UserNameException: {msg}"
        super().__init__(msg)
