class BackendError(Exception):
    pass

class DoesNotExist(BackendError):
    pass

class NotUniqueError(BackendError):
    pass
