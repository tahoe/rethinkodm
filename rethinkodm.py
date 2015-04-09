from datetime import datetime, timedelta
from iso8601 import parse_date
from collections import MutableSequence
from uuid import uuid4
from copy import deepcopy
import rethinkdb as r
import json
import pytz
utc = pytz.utc

_debug_ = False
def debug(item):
    if _debug_ is True:
        print item

registry = {}
def register_class(target):
    registry[target.__name__] = target

def settable(name):
    tablename = "%s_table" % name
    return tablename

def deletobj(conn, obj):
    pass

def create_db(conn, dbname):
    if dbname not in r.db_list().run(conn):
        r.db_create(dbname).run(conn)

def drop_db(conn, dbname):
    if dbname in r.db_list().run(conn):
        r.db_drop(dbname).run(conn)

#shared functions
def maketimestring(anytime=None):
    """
        Function to take a datetime string
        or a datetime object
        or None
        and return a datetime string with UTC tzinfo
    """
    if anytime is None:
        anytime = datetime.now()
        return unicode(utc.localize(anytime))
    elif type(anytime) is datetime:
        return unicode(anytime)
    elif type(anytime) is unicode:
        anytime = parse_date(anytime)
        if anytime.tzinfo is not None:
            return unicode(anytime)
        else:
            return unicode(utc.localize(anytime))
    else:
        msg=('anytime should be datetime obj or datetime string')
        raise Exception(msg)

def getRethinkMeta( dbname=None,
                    r=None):
    """ function to return the meta type for the base class """
    class RethinkMeta(type):
        """
            our DB table type metaclass
            it obviously needs the below dct[<item>] items
            in forder for this to use.
            ocb is the database we are using
            r is the rethinkdb connection
            tz is the pytz utc from the above import
            tbl is the tablename for the class that
            is using us, defaulting to the class name
            with underscores and _table_ added.
        """
        def __new__(cls, name, bases, dct):
            dct['ocb'] = r.db(dbname)
            dct['r'] = r
            dct['tablename'] = settable(name)
            dct['tbl'] = dct['ocb'].table(dct['tablename'])
            dct['tz'] = utc
            new_cls = super(RethinkMeta, cls).__new__(cls,
                                                      name,
                                                      bases,
                                                      dct)
            register_class(new_cls)
            return new_cls
            
    return RethinkMeta

def getRethinkBase(RethinkMeta=None, get_conn=None):
    """ a function to return the base class that will be inherited """
    class RethinkBase(object):
        """
            DB connection base class class
            Defines a few helper and an initialize function
            usefull to any class defining a table/document
        """
        __metaclass__ = RethinkMeta


        def _initialize(self):
            """ initialize a table if it needs to be """
            with get_conn() as conn:
                if self.tablename not in self.ocb.table_list().run(conn):
                    self.ocb.table_create(self.tablename).run(conn)

        @property
        def dumpobject(self):
            kwargs = {}
            for k, v in self.__dict__.items():
                if isinstance(v, RelatedItems):
                    kwargs[k] = [unicode(i.id) for i in v]
                else:
                    kwargs[k] = v
            return kwargs

        @classmethod
        def get(cls, id=None):
            """
                A class method for finding objects of a type
                takes the connection
            """
            if id is None:
                raise AttributeError('id must be supplied')
            with get_conn() as conn:
                retdict = cls.tbl.get(unicode(id)).run(conn)
            if retdict is None:
                return None
            else:
                try:
                    return cls(**retdict)
                except Exception as e:
                    raise InstantiateDictError('%s failed to build' % (
                                                cls.__name__))

        @classmethod
        def filter(cls, attr=None, value=None):
            """
                A class method for searching based on an attribute
                returns a list
            """
            if attr is None:
                raise AttributeError('attr must be a string')
            with get_conn() as conn:
                retdict = cls.tbl.filter({unicode(attr):unicode(value)})\
                                 .run(conn)
            if retdict is None:
                msg='cls.tbl.filter(attr).run() returned nothing'
                raise NoResultError(msg)
            else:
                resultlist = []
                try:
                    for result in retdict:
                        resultlist.append(cls(**result))
                except Exception as e:
                    raise InstantiateDictError('%s faild to build' % (
                                                cls.__name__))
            return resultlist

        #relationship building methods
        def _ihaveone(self, objtype, attr, value):
            """
                This sets up the Many side in a Many-To-One
                relationship where this class has an attribute
                with the ID of another object.
                The setup for this is so that when instantiating
                the class using this method, you can instantiate
                with the ID of another object or the object it's self

                The remote side of this will use the _hasoneme method
                For instance, taken from the _hasoneme example:
                I am a Person that has a _family attribute 
                Set up the Person class as follows:
                in __init__:
                    self._family = self._ihaveone(Family,
                                                  '_family',
                                                  kwargs.get('_family',
                                                             None))
                and then the getter/setter for the attribute
                @property
                def family(self):
                    if self._family is not None:
                        return Family.get(id=self._family)
                    else:
                        return self._family (none basically)

                @family.setter(self, fam):
                    self._family = self._ihaveone(Family, '_family', fam)

                where Family, '_family', fam are:
                Class, attribute, object or ID string
            """
            if isinstance(value, objtype):
                value = value.id
            elif isinstance(value, unicode)\
                            or isinstance(value, str)\
                            or value is None:
                value = value
            return value

        def _hasmanyme(self, relation):
            """
                This sets up a pointer to a One-To-Many
                For instance, an Organization has a list
                of people in it's _persons attribute
                Example on the Person class to point back to Orgs:
                    self._orgs = (Organization, '_persons')
                where Organization has a _person attribute that has
                a list of IDs belonging to my class
                Then a property has to be set on the Person class
                as shown here
                Example:
                    @property
                    def orgs(self):
                        return self._hasmanyme(self._orgs)


                Orgs, that has a list of Persons, will use the
                RelatedItems class which is a special list type. See
                RelatedItems for how to set that side up
            """
            if type(relation) is not tuple:
                raise AttributeError('pass a relation tuple please')
            with get_conn() as conn:
                #get the class from the registry
                relCls = registry[relation[0]]
                rowname = relation[1]
                retval = self.ocb.table(relCls.tablename).filter(
                         self.r.row[rowname].contains(self.id)).run(conn)
            if retval is not None:
                return [relCls(**blng) for blng in retval]
            else:
                return [] #no return, empty list, means no relation

        def _hasoneme(self, relation):
            """
                This sets up a Many-To-One relationship where
                the class running this function doesn't store the
                remote class's ID but many instances of the remote
                class will have the ID of a single object of this class.

                For instance, say a Family has Persons but instead of
                the Family storing a persons list attribute, each person
                instead has a single _family attribute that stores
                a Family ID
                Example on the Family class set up an attribute tuple:
                    self._members = (Person, '_family')
                    
                And set up a property to this method
                @property
                def families(self):
                    return self._hasoneme(self._members)

                Still trying to figure out how to do this without
                having to use two steps...
            """
            if type(relation) is not tuple:
                raise AttributeError('pass a relation tuple please')
            with get_conn() as conn:
                relCls = registry[relation[0]]
                hercolumn = relation[1]
                retval = self.ocb.table(relCls.tablename).filter(
                         {hercolumn:self.id}).run(conn)
            if retval is not None:
                return [relCls(**have) for have in retval]
            else:
                return [] #no return, empty list, means no relation

        @property
        def tojson(self):
            return json.dumps(self.dumpobject)

        @property
        def fromdb(self):
            with get_conn() as conn:
                return self.tbl.get(self.id).run(conn)

        def save(self):
            updatedict = {}
            for k, v in self.__dict__.items():
                if isinstance(v, RelatedItems):
                    updatedict[k] = [unicode(i.id) for i in v]
                else:
                    updatedict[k] = v
            with get_conn() as conn:
                self.tbl.insert(updatedict).run(conn)
            return self

        def update(self):
            updatedict = {}
            for k, v in self.__dict__.items():
                if isinstance(v, RelatedItems):
                    updatedict[k] = [unicode(i.id) for i in v]
                else:
                    updatedict[k] = v
            with get_conn() as conn:
                self.tbl.replace(updatedict).run(conn)
    return RethinkBase


class NoResultError(Exception):
    pass


class InstantiateDictError(Exception):
    pass


class RelatedItems(MutableSequence):
    """
        this class allows for a one to many relation

        @args
            cls:    we contain a list of this Class objects
            alist:  a list of objects of the above class that we store

        Then when defining AGroupOfThings class you can do
            in __init__:
                self.things = RelatedItems(Things,
                                           kwargs.get('things',[]))

        The point of this is to 'save' a list of IDs of the remote
        class but when reading from the list, getting the objects instead.

        On the remote class, set up a _hasmanyme relation, see
        the _hasmanyme method to see how to set up that side.
    """
    def __init__(self, cls, alist):
        self._list = alist
        self._cls = cls

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def __setitem__(self, index, item):
        """ __setitem__ override to save the object ID """
        self._list[index] = item.id

    def insert(self, index, value):
        """ same type of thing for insert """
        self._list.insert(index, value.id)

    def __str__(self): return unicode(self._list)

    def __repr__(self):
        """ returns the list of objects, gotten from __getitem__ """
        return repr([self.__getitem__(idx) for idx,
                    val in enumerate(self._list)])

    def __iter__(self):
        """ returns the object when iterating over ourself """
        for value in self._list:
            yield self._cls.get(id=value)

    def __getitem__(self, index):
        """ returns the object from the index of ourself """
        return self._cls.get(id=self._list[index])
