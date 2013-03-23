'''
Created on 17 Mar 2013

@author: jafd
'''

from .sql import Select, Update, Delete, Insert, Literal
import copy

__DAO__ = {}

def get_dao(name):
    """Undocumented"""
    return __DAO__[name]

class Expression(object): #IGNORE:R0903
    """
    This class is used to build SQL expression for some clauses
    as if they were Python expression. This enables you to put
    into a WHERE clause things like
    
      collection.filter(User.name == 'john')
      
    instead of
    
      collection.filter("users.name = 'john'")
      
    DAO field accessors, when invoked as class methods, will
    return Expression instances. 
    """
    def __init__(self, initval=None):
        self.initval = initval
        
    def __unicode__(self):
        return unicode(self.initval)
        
    @classmethod
    def _escape(cls, something):
        """
        Escapes a string literal to be in accord with SQL expectations:
        that is, "'" is escaped.
        
        @param something: The string to be escaped  
        """
        if isinstance(something, (cls, Literal)):
            return something
        else:
            return u"'{0}'".format(unicode(something).replace("'", "''"))

    @classmethod
    def call(cls, funcname, *args):
        """
        Make a function call.
        
        Expression.call('AVG', User.age) -> 'AVG(users.age)'
        
        """
        processed = u', '.join([unicode(cls._escape(x)) for x in args])
        return Expression(u"{0}({1})".\
                          format(funcname, processed))
    
    def __str__(self):
        return str(self.initval)
    
    def _fmt(self, fmt, other):
        """
        A formatting helper for binary operators.
        """
        return Expression(fmt.\
                          format(self.initval, self._escape(other)))

    def __add__(self, other):
        return self._fmt(u'({0} + {1})', other)
    
    def __radd__(self, other):
        return self._fmt(u'({1} + {0})', other)

    def __sub__(self, other):
        return self._fmt(u'({0} - {1})', other)
    
    def __rsub__(self, other):
        return self._fmt(u'({1} - {0})', other)

    def __mul__(self, other):
        return self._fmt(u'({0} * {1})', other)
    
    def __rmul__(self, other):
        return self._fmt(u'({1} * {0})', other)

    def __div__(self, other):
        return self._fmt(u'({0} / {1})', other)
    
    def __rdiv__(self, other):
        return self._fmt(u'({1} / {0})', other)

    def __mod__(self, other):
        return self._fmt(u'({0} % {1})', other)
    
    def __rmod__(self, other):
        return self._fmt(u'({1} % {0})', other)

    def __pow__(self, other):
        return self._fmt(u'POWER({0}, {1})', other)
    
    def __rpow__(self, other):
        return self._fmt(u'POWER({1}, {0})', other)

    def __and__(self, other):
        return self._fmt(u'({0} AND {1})', other)
    
    def __rand__(self, other):
        return self._fmt(u'({1} AND {0})', other)

    def __or__(self, other):
        return self._fmt(u'({0} OR {1})', other)
    
    def __ror__(self, other):
        return self._fmt(u'({1} OR {0})', other)

    def __xor__(self, other):
        return self._fmt(u'({0} XOR {1})', other)
    
    def __rxor__(self, other):
        return self._fmt(u'({1} XOR {0})', other)

    def __not__(self):
        return Expression(u'(NOT {0})'.format(self.initval))
    
    def __eq__(self, other):
        return self._fmt(u'({0} = {1})', other)

    def __ne__(self, other):
        return self._fmt(u'({0} <> {1})', other)

    def __lt__(self, other):
        return self._fmt(u'({0} < {1})', other)

    def __gt__(self, other):
        return self._fmt(u'({0} > {1})', other)

    def __lte__(self, other):
        return self._fmt(u'({0} >= {1})', other)

    def __gte__(self, other):
        return self._fmt(u'({0} <= {1})', other)

class Field(object): #IGNORE:R0903
    """
    This class should be used for field member of DAO classes.
    Only Fields can be saved and queried automatically.
    """
    def __init__(self, initval=None):
        self.value = initval
        self._name = None
    
    def __get__(self, obj, objtype=None):
        if obj is None:
            return Expression(u'"{0}"."{1}"'.\
                              format(objtype.__table__, self._name))
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
        self.cursor = self.connection.cursor()
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
    
    def __delitem__(self, item):
        raise RuntimeError("Cannot delete items from the collection")
    
    def __getitem__(self, index):
        raise RuntimeError("Only use iteration to access items in a Collection")
    
    def __setitem__(self, idx, value):
        raise RuntimeError("Collections are read-only")

class Relationship(object): #IGNORE:R0902
    
    def __init__(self, entity, mine, theirs, collection=False, #IGNORE:R0913
                 via_table=None, via_mine=None, via_theirs=None): 
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
                   via_mine = u'"{0}"."{1}"'.\
                        format(self.via_table, self.via_mine),
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

    def __new__(mcs, classname, bases, classdict):
        mutables = find_mutables(classdict, bases)
        fields = find_fields(classdict, bases)
        rels = find_relationships(classdict, bases, mcs)
        classdict['_mutables'] = mutables
        classdict['_fields'] = fields
        classdict['_relationships'] = rels
        __DAO__[mcs.__name__] = mcs
        return type.__new__(mcs, classname, bases, classdict)


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
    _fields = None
    __metaclass__ = DataObjectMeta
    __table__ = None
    __primary__ = 'id'
    id = Field(None)

    def __new__(cls, **kwargs):
        obj = object.__new__(cls)
        for name, default_func in cls._mutables:
            if name not in kwargs:
                setattr(obj, name, default_func())
        return obj

    def __init__(self, __conn__=None, **kwargs):
        self.__connection__ = __conn__
        self.__changed__ = set()
        for key, val in kwargs.iteritems():
            setattr(self, key, val)
        self.__changed__ = set()
    
    def __setattr__(self, attr, value):
        if attr in self._mutables:
            self.__changed__.add(attr)
        object.__setattr__(attr, value)
    
    @classmethod
    def fetch_dict_one(cls, cursor):
        """
        Fetch a value from a database cursor and
        make it into a dictionary.
        
        @param cursor: DB-API cursor
        @return: dict
        """
        row = cursor.fetchone()
        result = {}
        description = cursor.description
        i = 0
        for i in range(len(description) - 1):
            result[description[i][0]] = row[i]
        return result
    
    def save(self):
        """
        Insert or update this object into the database.
        """
        values = {}
        for field in self.__changed__:
            values[field] = getattr(self, field)
            
        if getattr(self, self.__primary__) is None:
            statement = Insert(self.__table__)
            statement.add_values(**values)
        else:
            statement = Update(self.__table__)
            statement.add_set(**values)
        statement.add_returning(*self._values)
        cur = self.__connection__.cursor()
        cur.execute(unicode(statement), statement.bound_parameters)
        row = self.fetch_dict_one(cur)
        [setattr(self, key, val) for key, val in row.items()]
        self.__changed__ = set()
        
    def delete(self):
        """
        Deletes this object from the database.
        NB: if you plan on inserting it again and use surrogate
        keys, you will need to set them to None.
        """
        if getattr(self, self.__primary__) is None:
            return None
        statement = Delete(self.__table__)
        statement.add_where('{0} = %({0})s'.format(self.__primary__))
        statement.bound_parameters[self.__primary__] = getattr(self.__primary__)
        cur = self.__connection__.cursor()
        cur.execute(unicode(statement), statement.bound_parameters)
        return cur.rowcount
    
    @classmethod
    def from_cursor(cls, cur, conn):
        """
        Hydrates objects from a database cursor.
        
        @param cur: the cursor in question
        @param conn: the database connection to be used
                     for objects
        @return: list of DAO objects
        """
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
        statement.add_fields([u'"{0}"."{1}"'.\
                              format(cls.__table__, x) for x in cls._fields])
        statement.add_from(cls.__table__)
        for lit in args:
            statement.add_where(lit)
        for nonlit, value in kwargs.iteritems():
            statement.add_where(u'{0} = %({0})s'.format(nonlit))
            statement.bound_parameters[nonlit] = value
        return Collection(statement, conn, cls)

    @classmethod
    def load_by_primary(cls, conn, value):
        """
        Fetch and hydrate an object by its primary key.
        
        @param conn: database connection object
        @param value: the value of primary key
        
        @todo: make it usable for composite keys
        """
        params = { cls.__primary__ : value }
        return cls.load_by(conn, **params)

    def __getattribute__(self, attr):
        print self
        return super(BaseDAO, self).__getattribute__(attr)
