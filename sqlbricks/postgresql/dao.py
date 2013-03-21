'''
Created on 17 Mar 2013

@author: jafd
'''

from .sql import Select, Update, Delete, Insert, Literal
import copy

__DAO__ = {}

def get_dao(name):
    return __DAO__[name]

class Expression(object):
    
    def __init__(self, initval=None):
        self.initval = initval
        
    def __unicode__(self):
        return unicode(self.initval)
        
    @classmethod
    def _escape(cls, something):
        if isinstance(something, (cls, Literal)):
            return something
        else:
            return u"'{0}'".format(unicode(something).replace("'", "''"))

    @classmethod
    def call(cls, funcname, *args):
        return Expression(u"{0}({1})".format(funcname, u', '.join([unicode(cls._escape(x)) for x in args])))
    
    def __str__(self):
        return str(self.initval)
    
    def __add__(self, other):
        return Expression(u'({0} + {1})'.format(self.initval, self._escape(other)))
    
    def __radd__(self, other):
        return Expression(u'({1} + {0})'.format(self.initval, self._escape(other)))

    def __sub__(self, other):
        return Expression(u'({0} - {1})'.format(self.initval, self._escape(other)))
    
    def __rsub__(self, other):
        return Expression(u'({1} - {0})'.format(self.initval, self._escape(other)))

    def __mul__(self, other):
        return Expression(u'({0} * {1})'.format(self.initval, self._escape(other)))
    
    def __rmul__(self, other):
        return Expression(u'({1} * {0})'.format(self.initval, self._escape(other)))

    def __div__(self, other):
        return Expression(u'({0} / {1})'.format(self.initval, self._escape(other)))
    
    def __rdiv__(self, other):
        return Expression(u'({1} / {0})'.format(self.initval, self._escape(other)))

    def __mod__(self, other):
        return Expression(u'({0} % {1})'.format(self.initval, self._escape(other)))
    
    def __rmod__(self, other):
        return Expression(u'({1} % {0})'.format(self.initval, self._escape(other)))

    def __pow__(self, other):
        return Expression(u'POWER({0}, {1})'.format(self.initval, self._escape(other)))
    
    def __rpow__(self, other):
        return Expression(u'POWER({1}, {0})'.format(self.initval, self._escape(other)))

    def __and__(self, other):
        return Expression(u'({0} AND {1})'.format(self.initval, self._escape(other)))
    
    def __rand__(self, other):
        return Expression(u'({1} AND {0})'.format(self.initval, self._escape(other)))

    def __or__(self, other):
        return Expression(u'({0} OR {1})'.format(self.initval, self._escape(other)))
    
    def __ror__(self, other):
        return Expression(u'({1} OR {0})'.format(self.initval, self._escape(other)))

    def __xor__(self, other):
        return Expression(u'({0} XOR {1})'.format(self.initval, self._escape(other)))
    
    def __rxor__(self, other):
        return Expression(u'({1} XOR {0})'.format(self.initval, self._escape(other)))

    def __not__(self):
        return Expression(u'(NOT {0})'.format(self.initval))
    
    def __eq__(self, other):
        return Expression(u'({0} = {1})'.format(self.initval, self._escape(other)))

    def __ne__(self, other):
        return Expression(u'({0} <> {1})'.format(self.initval, self._escape(other)))

    def __lt__(self, other):
        return Expression(u'({0} < {1})'.format(self.initval, self._escape(other)))

    def __gt__(self, other):
        return Expression(u'({0} > {1})'.format(self.initval, self._escape(other)))

    def __lte__(self, other):
        return Expression(u'({0} >= {1})'.format(self.initval, self._escape(other)))

    def __gte__(self, other):
        return Expression(u'({0} <= {1})'.format(self.initval, self._escape(other)))

class Field(object):
    """
    This class should be used for field member of DAO classes.
    Only Fields can be saved and queried automatically.
    """
    def __init__(self, initval=None):
        self.value = initval
        self._name = None
    
    def __get__(self, obj, objtype=None):
        if obj is None:
            return Expression(u'"{0}"."{1}"'.format(objtype.__table__, self._name))
        return self.value

    def __set__(self, obj, value):
        if obj is None:
            raise ValueError("Cannot set value on an unbound field.")
        self.value = value
        

class Collection(object):
    
    def __init__(self, query, conn, objt):
        self.objtype = objt
        self.connection = conn
        self.query = query
        self.started = False
        self.cursor = None
    
    def __iter__(self):
        return self
    
    def start(self):
        self.cursor = self.conn.cursor()
        self.cursor.execute(unicode(self.query), self.query.bound_parameters)
        self.started = True
    
    def next(self):
        if not self.started:
            self.start()
        try:
            inst = self.objtype.one_from_cursor(self.cursor, self.connection)
        except:
            self.started = False
            raise StopIteration
        return inst
    
    def __len__(self):
        if not self.started:
            self.start()
        return self.cursor.rowcount

class Relationship(object):
    
    def __init__(self, entity, mine, theirs, collection=False, via_table=None,
                 via_mine=None, via_theirs=None):
        self.name = None
        self.parent = None
        self.entity = entity
        self.mine = mine
        self.theirs = theirs
        self.collection = collection
        self.via_table = via_table
        self.via_mine = via_mine
        self.via_theirs = via_theirs
        
    def join_via(self, query):
        fmt_via_join = u"JOIN {via_table} ON ({via_mine} = {my_id})".\
            format(
                   via_table = self.via_table,
                   via_mine = u'"{0}"."{1}"'.format(self.via_table, self.via_mine),
                   my_id = getattr(self.parent, self.mine)
                   )
        fmt_theirs_join = u"JOIN {their_table} ON ({via_theirs} = {their_id})".\
            format(
                   their_table = self.entity.__table__,
                   via_theirs = self.via_theirs,
                   their_id = getattr(self.entity, self.theirs)
                   )
        query.add_join(fmt_via_join, fmt_theirs_join)
        return query
    
    def join_theirs(self, query):
        fmt_join = u"JOIN {their_table} ON ({their_id} = {mine_id})".\
            format(
                   their_table = self.entity.__table__,
                   their_id = getattr(self.entity, self.theirs),
                   mine_id = getattr(self.parent.__class__, self.mine)
                   )
        query.add_join(fmt_join)
        return query
        
    def __get__(self, obj, objtype=None):
        if obj is None:
            raise ValueError("Cannot query relationship on an unbounded object")
        if isinstance(self.entity, str):
            self.entity = get_dao(self.entity)
        conn = obj.__connection__
        collection = obj.__class__.load_by(conn)
        if self.via_table is not None:
            self.join_via(collection.query)
        else:
            self.join_theirs(collection.query)
        my_name = getattr(self.parent.__class__, self.mine)
        collection.query.add_where(u"{my_id} = %({rel}_{my_id_short})s".\
                                   format(
                                          my_id = my_name,
                                          my_id_short = self.mine,
                                          rel = self.name
                                          ))
        collection.query.bound_parameters[self.name + '_' + self.mine] = \
            getattr(self.parent, self.mine)
        if self.collection:
            return collection
        return next(collection)

class DataObjectMeta(type):
    def __new__(cls, classname, bases, classdict):
        mutables = find_mutables(classdict, bases)
        fields = find_fields(classdict, bases)
        rels = find_relationships(classdict, bases, cls)
        classdict['_mutables'] = mutables
        classdict['_fields'] = fields
        classdict['_relationships'] = rels
        __DAO__[cls.__name__] = cls
        return type.__new__(cls, classname, bases, classdict)

def find_fields(classdict, bases):
    found_fields = set()
    for name, value in classdict.items():
        if isinstance(value, Field):
            value._name = name
            found_fields.add(name)
    for base in bases:
        if hasattr(base, '_fields'):
            for name, value in base._fields:
                if not name in found_fields:
                    found_fields.add(name)
    return frozenset(found_fields)

def find_relationships(classdict, bases, cls):
    found_rels = set()
    for name, value in classdict.items():
        if isinstance(value, Relationship):
            value.name = name
            value.parent = cls
            found_rels.add(name)
    for base in bases:
        if hasattr(base, '_relationships'):
            for name, value in base._relationships:
                if not name in found_rels:
                    found_rels.add(name)
    return frozenset(found_rels)
   
def find_mutables(classdict, bases):
    def create_default(name, value):
        return name, lambda: copy.copy(value)
    
    found_mutables = set()
    mutables = set()
    for name, value in classdict.items():
        if (not name.startswith('__') \
            and not callable(value) \
            and not hasattr(value, '__get__') \
            and isinstance(value, (list, dict))):
            if not name in found_mutables:
                found_mutables.add(name)
                mutables.add(create_default(name, value))
                del classdict[name]
    for base in bases:
        if hasattr(base, '_mutables'):
            for name, value in base._mutables:
                if not name in found_mutables:
                    found_mutables.add(name)
                    mutables.add((name, value))
    return frozenset(mutables)

class BaseDAO(object):
    _mutables = None
    _immutables = None
    __metaclass__ = DataObjectMeta
    __table__ = None
    __primary__ = 'id'
    id = None

    def __new__(cls, **kwargs):
        obj = object.__new__(cls)
        for name, default_func in cls._mutables:
            if name not in kwargs:
                setattr(obj, name, default_func())
        return obj

    def __init__(self, __conn__=None, **kwargs):
        self.__connection__ = __conn__
        self.__changed__ = set()
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        self.__changed__ = set()
    
    def __setattr__(self, attr, value):
        if attr in self._mutables:
            self.__changed__.add(attr)
        object.__setattr__(attr, value)
    
    @staticmethod
    def fetch_dict_one(self, cursor):
        row = cursor.fetchone()
        result = {}
        description = cursor.description
        i = 0
        for i in range(len(description) - 1):
            result[description[i][0]] = row[i]
        return result
    
    def save(self):
        values = {}
        for c in self.__changed__:
            values[c] = getattr(self, c)
            
        if getattr(self, self.__primary__) is None:
            statement = Insert(self.__table__)
            statement.add_values(**c)
        else:
            statement = Update(self.__table__)
            statement.add_set(**c)
        statement.add_returning(*self._values)
        c = self.__connection__.cursor()
        c.execute(unicode(statement), statement.bound_parameters)
        row = self.fetch_dict_one(c)
        [setattr(self, k, v) for k, v in row.items()]
        self.__changed__ = set()
        
    def delete(self):
        if getattr(self, self.__primary__) is None:
            return None
        statement = Delete(self.__table__)
        statement.add_where('{0} = %({0})s'.format(self.__primary__))
        statement.bound_parameters[self.__primary__] = getattr(self.__primary__)
        c = self.__connection__.cursor()
        c.execute(unicode(statement), statement.bound_parameters)
        return c.rowcount
    
    @classmethod
    def from_cursor(cls, cur, conn):
        result = []
        while True:
            try:
                data = cls.fetch_dict_one(cur)
            except:
                break
            inst = cls(__conn__=conn, **data)
            result.append(inst)
        return result
    
    @classmethod
    def one_from_cursor(cls, cur, conn):
        data = cls.fetch_dict_one(cur)
        inst = cls(__conn__=conn, **data)
        return inst
    
    @classmethod
    def load_by(cls, conn, *args, **kwargs):
        statement = Select()
        statement.add_fields([u'"{0}"."{1}"'.format(cls.__table__, x) for x in cls._fields])
        statement.add_from(cls.__table__)
        for lit in args:
            statement.add_where(lit)
        for nonlit, value in kwargs.iteritems():
            statement.add_where(u'{0} = %({0})s'.format(nonlit))
            statement.bound_parameters[nonlit] = value
        return Collection(statement, conn, cls)

    @classmethod
    def load_by_primary(cls, conn, value):
        cp = { cls.__primary__ : value }
        return cls.load_by(conn, **cp)

    def __getattribute__(self, attr):
        print self
        return super(BaseDAO, self).__getattribute__(attr)
