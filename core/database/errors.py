class BackendError(Exception):
    pass

class DoesNotExist(BackendError):
    pass
