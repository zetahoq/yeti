from wtforms import widgets, Field, StringField


class BaseField(object):
    db_field = None
    name = None

class GenericField(BaseField):

    def __init__(self, default=None, unique=False):
        self.value = None
        self._default = default

    def __get__(self, obj, objtype):
        return self.value or self._default

    def __set__(self, obj, value):
        self.value = value


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
