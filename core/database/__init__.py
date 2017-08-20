_class_registry = {}

def get_registered_class(class_name):
    if class_name not in _class_registry:
        print _class_registry
        raise ValueError("Requested a class that was not registered: {}".format(class_name))
    return _class_registry[class_name]

def register_class(class_name, klass):
    _class_registry[class_name] = klass
