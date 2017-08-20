import re
import datetime

from pymongo import MongoClient
from pymongo.son_manipulator import SONManipulator
import pymongo.errors

from core.database.errors import DoesNotExist, NotUniqueError

class MongoStore(object):

    def __init__(self, *args, **kwargs):
        self._connection = MongoClient(host=['localhost:27017'])
        self.db = self._connection['yeti-mongo']
        self.index()

    def index(self):
        # We could call this method from get_collection like Mongoengine does
        # but for now let's stick to manual index creation
        self.db['internals'].create_index("name", unique=True)
        self.db['observable'].create_index("value", unique=True)
        self.db['entity'].create_index("name", unique=True)
        # self.db['schedule_entry'].create_index("name", unique=True)

store = MongoStore()


mongo_query_operators = [
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "not",
    "in",
    "nin",
    "mod",
    "all",
    "size",
    "exists",
]

mongo_update_operators = [
    "set",
    "unset",
    "inc",
    "dec",
    "push",
    "push_all",
    "pop",
    "pull",
    "pull_all",
    "add_to_set",
]

mongo_query_op_re = re.compile(r"\.(?P<op>{})$".format("|".join(mongo_query_operators)))
mongo_update_op_re = re.compile(r"^(?P<op>{})\.".format("|".join(mongo_update_operators)))

def obj_to_bson(obj, field=None):
    if isinstance(obj, (list, tuple)):
        obj = [obj_to_bson(v) for v in obj]
    if isinstance(obj, (dict)):
        obj = {k: obj_to_bson(v) for k, v in obj.items()}
    if isinstance(obj, datetime.timedelta):
        obj = obj.total_seconds()
    if isinstance(obj, BackendDocument):
        from core.database.fields import ReferenceField
        if isinstance(field, ReferenceField):
            obj = obj._to_ref()
        else:
            obj = obj._to_bson()
    return obj


class BackendDocument(object):

    def __init__(self, *args, **kwargs):
        # This is a pretty naive field loading / object construction
        # TODO think of a better way to do this. Maybe use a mongo SON manipulator?
        self.id = kwargs.pop("id", None)
        for name in self._fields:
            if name in kwargs:
                setattr(self, name, kwargs.get(name))

    def save(self):
        if getattr(self, "id"):
            bson = self._to_bson()
            self.get_collection().replace_one({"_id": self.id}, bson)
        else:
            try:
                result = self.get_collection().insert_one(self._to_bson())
                self.id = result.inserted_id
            except pymongo.errors.DuplicateKeyError as e:
                raise NotUniqueError
        return self

    def update(self, **kwargs):
        modifiers = self._update_to_mongo(**kwargs)
        result = self.get_collection().update_one({"_id": self.id}, modifiers)
        return self.reload()

    def reload(self):
        return self.get(_id=self.id)

    @classmethod
    def _from_bson(klass, bson):
        obj = klass()
        obj.id = bson.pop("_id", None)
        for name, field in klass._fields.items():
            value = bson.get(name)
            from core.database.fields import TimeDeltaField
            from core.database.fields import EmbeddedDocumentField
            from core.database.fields import ListField
            from core.database.fields import ReferenceField
            if isinstance(field, TimeDeltaField):
                value = datetime.timedelta(seconds=value)
            if isinstance(field, EmbeddedDocumentField):
                value = field.get_class._from_bson(value)
            if isinstance(field, ListField):
                if hasattr(field, "_class"):
                    value = [field.get_class._from_bson(v) for v in value]
            if isinstance(field, ReferenceField) and value:
                # Lazy loading: don't load this object from the DB untill it's explicitly accessed
                pass

            setattr(obj, name, value)

        return obj

    def _to_ref(self):
        return {"_id": self.id, "collection": self.collection_name()}

    def _to_bson(self):
        bson = {}
        for name, field in self._fields.items():
            value = getattr(self, name)
            bson[name] = obj_to_bson(value, field)
        bson['_cls'] = self.__class__.__name__.lower()
        return bson

    def clean_update(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

        self.validate()

        update_dict = {}
        for key, value in kwargs.iteritems():
            update_dict[key] = getattr(self, key, value)

        self.update(**update_dict)
        self.reload()
        return self

    def add_to_set(self, field, value):
        return self._set_update('$addToSet', field, value)

    def remove_from_set(self, field, value):
        return self._set_update('$pull', field, value)

    def _set_update(self, method, field, value):
        result = self.__class__.get_collection().update_one(
            {'_id': self.pk}, {method: {field: value}})
        return result.modified_count == 1

    @staticmethod
    def get_from_collection(collection, id):
        #TODO Need an index of classes to know which _from_bson method to call
        return store.db[collection].find_one({"_id": id})

    @classmethod
    def get(klass, **kwargs):
        kwargs["_cls"] = klass.__name__.lower()
        obj = klass.get_collection().find_one(kwargs)
        if obj:
            return klass._from_bson(obj)
        else:
            raise DoesNotExist

    @classmethod
    def count(klass):
        fltr = {"_cls": klass.__name__.lower()}
        return klass.get_collection().find(**fltr).count()

    @classmethod
    def all(klass):
        fltr = {"_cls": klass.__name__.lower()}
        return (klass._from_bson(obj) for obj in klass.get_collection().find(**fltr))

    @classmethod
    def objects(klass, **kwargs):
        mongo_query = klass._query_to_mongo(**kwargs)
        mongo_query['_cls'] = klass.__name__.lower()
        return (klass.get_collection().find(mongo_query))

    @classmethod
    def _query_to_mongo(klass, **kwargs):
        mongo_query = {}
        for key, value in kwargs.items():
            key = re.sub("__", ".", key)
            opmatch = mongo_query_op_re.search(key)
            if opmatch:
                op = opmatch.group('op')
                key = re.sub(r"\.{}$".format(op), "", key)
                value = {"${}".format(op): value}
            mongo_query[key] = obj_to_bson(value)
        return mongo_query

    @classmethod
    def _update_to_mongo(klass, **kwargs):
        mongo_update = {}
        for key, value in kwargs.items():
            key = re.sub("__", ".", key)
            key = re.sub(".S.", ".$.", key)
            opmatch = mongo_update_op_re.search(key)
            if opmatch:
                op = opmatch.group('op')
                value = {re.sub(r"^{}\.".format(op), "", key): value}
                key = "${}".format(op)
            mongo_update[key] = obj_to_bson(value)
        return mongo_update

    @classmethod
    def modify(klass, query, **kwargs):
        query = klass._query_to_mongo(**query)
        modifiers = klass._update_to_mongo(**kwargs)
        query['_cls'] = klass.__name__.lower()
        result = klass.get_collection().update_many(query, modifiers)
        return result.matched_count > 0

    @classmethod
    def get_or_create(klass, **kwargs):
        """Attempts to save a node in the database, and loads it if duplicate"""
        obj = klass(**kwargs)
        try:
            return obj.save()
        except NotUniqueError:
            if hasattr(obj, 'name'):
                return klass.get(name=obj.name, _cls=klass.__name__.lower())
            if hasattr(obj, 'value'):
                return klass.get(value=obj.value, _cls=klass.__name__.lower())

    @classmethod
    def get_collection(klass):
        return store.db[klass.collection_name()]
