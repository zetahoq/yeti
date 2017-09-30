
import six
from wtforms import widgets, Field

from core.database.backends import BackendDocument
from core.database import get_registered_class

class BaseField(object):
    db_field = None
    name = None

class GenericField(BaseField):

    def __init__(self, default=None, unique=False, verbose_name=None, **kwargs):
        self.value = None
        self._default = default
        self._verbose = verbose_name

    def __get__(self, obj, objtype):
        print "Fetching"
        if self.value is None and self._default is not None:
            self.value = self._default
        if callable(self.value):
            return self.value()
        return self.value

    def __set__(self, obj, value):
        self.value = value

    @property
    def get_class(self):
        if isinstance(self._class, six.string_types):
            self._class = get_registered_class(self._class)
        return self._class

class StringField(GenericField):

    def __init__(self, *args, **kwargs):
        super(StringField, self).__init__(*args, **kwargs)

    def _validate(self):
        return isinstance(self.value, six.string_types)

class IntField(GenericField):

    def __init__(self, *args, **kwargs):
        super(IntField, self).__init__(*args, **kwargs)

    def _validate(self):
        return isinstance(self.value, (int, long))

class BooleanField(GenericField):

    def __init__(self, *args, **kwargs):
        super(BooleanField, self).__init__(*args, **kwargs)

    def _validate(self):
        return isinstance(self.value, bool)

class DictField(GenericField):

    def __init__(self, *args, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = {}
        super(DictField, self).__init__(*args, **kwargs)

    def _validate(self):
        return isinstance(self.value, dict)

class ListField(GenericField):

    def __init__(self, klass, *args, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = []
        super(ListField, self).__init__(*args, **kwargs)
        self._class = klass

    def _validate(self):
        return isinstance(self.value, (list, tuple))

class ReferenceField(GenericField):

    def __init__(self, reference_class, *args, **kwargs):
        super(ReferenceField, self).__init__(*args, **kwargs)
        self._class = reference_class

    def __get__(self, obj, objtype):
        if not self.value:
            return
        if isinstance(self.value, BackendDocument):
            return self.value
        elif isinstance(self.value, dict):
            collection = self.value['collection']
            _id = self.value['_id']
            bson = BackendDocument.get_from_collection(collection, _id)
            return self._class._from_bson(bson)

    def __set__(self, obj, value):
        if isinstance(value, BackendDocument) or value is None:
            self.value = value
        elif isinstance(value, dict):
            self.value = {
                "_id": value['_id'],
                "collection": self._class.collection_name()
            }

class EmbeddedDocumentField(GenericField):
    def __init__(self, embedded_class, *args, **kwargs):
        super(EmbeddedDocumentField, self).__init__(*args, **kwargs)
        self._class = embedded_class

class TimeDeltaField(GenericField):
    def __init(self, *args, **kwargs):
        super(TimeDeltaField, self).__init__(*args, **kwargs)

class DateTimeField(GenericField):
    def __init(self, *args, **kwargs):
        super(DateTimeField, self).__init__(*args, **kwargs)

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
