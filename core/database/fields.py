import six
from wtforms import widgets, Field

from core.database.backends import store
from core.database.backends import BackendDocument

class BaseField(object):
    db_field = None
    name = None

# TODO Implement to_python methods for every field to convert from bson to
# python types
class GenericField(BaseField):

    def __init__(self, default=None, unique=False, verbose_name=None, **kwargs):
        self.value = None
        self._default = default
        self._verbose = verbose_name

    def __get__(self, obj, objtype):
        return self.value or self._default

    def __set__(self, obj, value):
        self.value = value

class StringField(GenericField):
    def _validate(self):
        return isinstance(self.value, six.string_types)

class IntField(GenericField):
    def _validate(self):
        return isinstance(self.value, (int, long))

class BooleanField(GenericField):
    def _validate(self):
        return isinstance(self.value, bool)

class DictField(GenericField):
    # TODO recurse through values to convert them to python
    def _validate(self):
        return isinstance(self.value, dict)

class ListField(GenericField):
    # TODO recurs through values to convert them to python
    def __init__(self, *args, **kwargs):
        self.value = []

    def _validate(self):
        return isinstance(self.value, (list, tuple))

class ReferenceField(GenericField):

    def __get__(self, obj, objtype):
        collection = self.value['collection']
        _id = self.value['_id']
        return BackendDocument.get_from_collection(collection, {"_id": _id})

    def __set__(self, obj, value):
        d = {
            "id": value.id,
            "collection": value.collection_name,
        }
        self.value = d

# WTForms fields

class StringListField(Field):
    widget = widgets.TextInput()

    def _value(self):
        if self.data:
            return u','.join([unicode(d) for d in self.data])
        else:
            return u''

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = [x.strip() for x in valuelist[0].split(',')]
        else:
            self.data = []


class TagListField(StringListField):
    endpoint = "api.Tag:index"


class EntityListField(StringListField):
    endpoint = "api.Entity:index"
