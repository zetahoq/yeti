from pymongo import MongoClient

class MongoStore(object):

    def __init__(self, *args, **kwargs):
        self._connection = MongoClient(host=['localhost:27017'])
        self.db = self._connection['yeti-mongo']

    def bootstrap(self):
        # create indexes here
        pass

store = MongoStore()

class BackendDocument(dict):
    collection_name = None

    def __init__(self, values=None):
        if values is None:
            values = {}
        dict.__init__(self, values)
        if not self.collection_name:
            self.collection_name = self.__class__.__name__.lower()
        self.collection = store.db[self.collection_name]

    @classmethod
    def get_collection(klass):
        print "ASD", klass.collection_name
        return store.db[klass.collection_name]

    @classmethod
    def get(klass, **kwargs):
        obj = klass.get_collection().find_one(kwargs)

        if obj:
            obj = klass(obj)

        return obj

    @classmethod
    def count(klass):
        return klass.get_collection().count()
    @classmethod
    def all(klass):
        return (o for o in klass.get_collection().find())
