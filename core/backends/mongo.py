from pymongo import MongoClient

from pymongo.son_manipulator import SONManipulator
import pymongo.errors


# Placeholder class for database SON manipulation
class Transform(SONManipulator):
    def transform_incoming(self, son, collection):
        # This is where we would call save_to_json
        for (key, value) in son.items():
            if isinstance(value, dict):
                son[key] = self.transform_incoming(value, collection)
        return son

    def transform_outgoing(self, son, collection):
        # this is where we woudl call load_from_json
        return son



class MongoStore(object):

    def __init__(self, *args, **kwargs):
        self._connection = MongoClient(host=['localhost:27017'])
        self.db = self._connection['yeti-mongo']
        # TODO: Find a smart way to do this
        # self.db.add_son_manipulator(Transform())
        self.indexes()

    def indexes(self):
        # TODO: See if we can build this with info from each class
        self.db['internals'].create_index("name", unique=True)

store = MongoStore()

class BackendDocument(dict):

    collection_name = None

    def __init__(self, *args, **kwargs):
        # This is a pretty naive field loading / object construction
        # TODO think of a better way to do this. Maybe use a mongo SON manipulator?
        for key, item in kwargs.items():
            setattr(self, key, item)

    @classmethod
    def get_collection(klass):
        collection_name = klass.collection_name or klass.__name__.lower()
        return store.db[collection_name]

    @classmethod
    def get(klass, **kwargs):
        obj = klass.get_collection().find_one(kwargs)
        if obj:
            obj = klass(**obj)

        return obj

    @classmethod
    def count(klass):
        return klass.get_collection().count()

    @classmethod
    def all(klass):
        return (o for o in klass.get_collection().find())

    def save(self):
        if "_id" in self:
            self.get_collection().replace_one({"_id": self['_id']}, dict(self))
        else:
            result = self.get_collection().insert_one(dict(self))
            self['_id'] = result.inserted_id

        return self

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

    def __getattr__(self, name):
        return dict(self).get(name, None)

    def __setattr__(self, name, value):
        self[name] = value
