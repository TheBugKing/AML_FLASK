class FileUploadException(Exception):
    def __init__(self, msg: str):
        msg = f"FileUploadException: {msg}"
        super().__init__(msg)
