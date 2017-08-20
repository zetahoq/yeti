import re
import datetime

from pymongo import MongoClient
from pymongo.son_manipulator import SONManipulator
import pymongo.errors

from core.database.errors import DoesNotExist

class MongoStore(object):

    def __init__(self, *args, **kwargs):
        self._connection = MongoClient(host=['localhost:27017'])
        self.db = self._connection['yeti-mongo']
        self.indexes()

    def indexes(self):
        # TODO: See if we can build this with info from each class
        self.db['internals'].create_index("name", unique=True)

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

def obj_to_bson(obj):
    if isinstance(obj, (list, tuple)):
        obj = [obj_to_bson(v) for v in obj]
    if isinstance(obj, (dict)):
        obj = {k: obj_to_bson(v) for k, v in obj.items()}
    if isinstance(obj, datetime.timedelta):
        obj = obj.total_seconds()
    if isinstance(obj, BackendDocument):
        obj = obj._to_bson()
    return obj


class BackendDocument(object):

    collection_name = None

    def __init__(self, *args, **kwargs):
        # This is a pretty naive field loading / object construction
        # TODO think of a better way to do this. Maybe use a mongo SON manipulator?
        self._id = kwargs.pop("_id", None)
        for name in self._fields:
            setattr(self, name, kwargs.get(name))

    def save(self):
        if getattr(self, "_id"):
            bson = self._to_bson()
            self.get_collection().replace_one({"_id": self._id}, bson)
        else:
            result = self.get_collection().insert_one(self._to_bson())
            self._id = result.inserted_id

        return self

    def update(self, **kwargs):
        modifiers = self._update_to_mongo(**kwargs)
        result = self.get_collection().update_one({"_id": self._id}, modifiers)
        return self.reload()

    def reload(self):
        return self.get(_id=self._id)

    @classmethod
    def _from_bson(klass, bson):
        obj = klass()
        for name, field in klass._fields.items():
            value = bson.get(name)
            from core.database.fields import TimeDeltaField
            if isinstance(field, TimeDeltaField):
                value = datetime.timedelta(seconds=value)
            setattr(obj, name, value)

        return obj

    def _to_bson(self):
        bson = {}
        for name in self._fields:
            value = getattr(self, name)
            bson[name] = obj_to_bson(value)
        # print "Generating BSON object"
        # print bson
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
        return store[collection].find_one({"_id": id})

    @classmethod
    def get(klass, **kwargs):
        obj = klass.get_collection().find_one(kwargs)
        if obj:
            return klass._from_bson(obj)
        else:
            raise DoesNotExist

    @classmethod
    def count(klass):
        return klass.get_collection().count()

    @classmethod
    def all(klass):
        return (klass._from_bson(obj) for obj in klass.get_collection().find())

    @classmethod
    def objects(klass, **kwargs):
        mongo_query = klass._query_to_mongo(**kwargs)
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
        print "Generated mongo query", mongo_query
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
        print "Generated mongo update", mongo_update
        return mongo_update

    @classmethod
    def modify(klass, query, **kwargs):
        query = klass._query_to_mongo(**query)
        modifiers = klass._update_to_mongo(**kwargs)
        result = klass.get_collection().update_many(query, modifiers)
        return result.matched_count > 0

    @classmethod
    def get_or_create(cls, **kwargs):
        """Attempts to save a node in the database, and loads it if duplicate"""
        obj = cls(**kwargs)
        try:
            return obj.save()
        except pymongo.errors.DuplicateKeyError:
            if hasattr(obj, 'name'):
                return cls.get(name=obj.name)
            if hasattr(obj, 'value'):
                return cls.get(value=obj.value)

    @classmethod
    def get_collection(klass):
        collection_name = klass.collection_name or klass.__name__.lower()
        return store.db[collection_name]
