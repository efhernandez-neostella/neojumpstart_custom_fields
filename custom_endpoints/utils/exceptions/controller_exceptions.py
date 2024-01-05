class ControllerException(Exception):
    def __init__(self, status: int, body: dict = {}) -> None:
        self.response = self._handle_exception(status, body)

    def _handle_exception(self, status, body):
        return {"statusCode": status, "body": body}
