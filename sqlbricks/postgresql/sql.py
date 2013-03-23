'''
Created on 16 Mar 2013

@author: jafd
'''

from ..base.sql import BaseQuery, _BaseMixin, _WhereMixin, _HavingMixin, \
    _GroupMixin, _FieldListMixin, _FromMixin, _JoinMixin, _OrderMixin, \
    _UsingMixin, _LimitMixin, _WithMixin

class _ReturningMixin(_BaseMixin):
    """
    This mixin class provides support for RETURNING clause
    which is PostgreSQL-specific. It is used in DML statements.
    """

    def add_returning(self, *args, **kwargs):
        self.check_clause('returning', set())
        for arg in args:
            self.clauses['returning'][unicode(arg)] = True
        for key, val in kwargs.iteritems():
            clause = u"{0} AS {1}".format(val, key)
            self.clauses['returning'].add(clause)
    
    def format_returning(self):
        result = u''
        if self.clauses.get('returning'):
            result = u"RETURNING {0}".format(u', '.join(self.clauses.get('returning')))
        return result
    
class Select(BaseQuery, _FieldListMixin, _FromMixin, _WhereMixin,
             _JoinMixin, _GroupMixin, _OrderMixin, _HavingMixin,
             _WithMixin, _LimitMixin):
    
    def __unicode__(self):
        result = u'''
            {with_clause} SELECT
                {field_list}
                {from_clause}
                {joins}
                {where_clause}
                {group_by}
                {having}
                {order_by}
                {limit}
            '''.format(
                       with_clause = self.format_with(),
                       field_list = self.format_fields(),
                       from_clause = self.format_from(),
                       joins = self.format_join(),
                       where_clause = self.format_where(),
                       group_by = self.format_group(),
                       having = self.format_having(),
                       order_by = self.format_order(),
                       limit = self.format_limit()
                       ).strip()
        return result

class Literal(object):
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return str(self.value)
    
    def __unicode__(self):
        return unicode(self.value)
    
    def __repr__(self):
        return repr(self.value)

class Update(_WithMixin, _WhereMixin, _FromMixin, _JoinMixin,
             _ReturningMixin, _BaseMixin, BaseQuery):
    
    def __init__(self, table, alias=None, only=False):
        self.table = table
        self.alias = None
        self.only = only
        super(Update, self).__init__()
        
    def add_set(self, **kwargs):
        self.check_clause('set', {})
        for k, v in kwargs.iteritems():
            if isinstance(v, Literal):
                arg = u'{0} = {1}'.format(k, v)
            else:
                self.bound_parameters[k] = v
                arg = u'{0} = %({1})s'.format(k, k)
            self.clauses['set'][arg] = True
            
    def format_set(self):
        if not self.clauses.get('set'):
            return u''
        return u'SET {0}'.format(u', '.join(self.clauses['set'].keys()))
    
    def format_table(self):
        if self.alias is not None:
            return u'{0} AS {1}'.format(self.table, self.alias)
        return self.table
    
    def format_only(self):
        if self.only:
            return u'ONLY'
        return u''

    def __unicode__(self):
        result = u'''
            {with_clause} UPDATE {only} {table_name}
                {set_clause}
                {from_clause}
                {joins}
                {where_clause}
                {returning}
            '''.format(
                       with_clause = self.format_with(),
                       only = self.format_only(),
                       table_name = self.format_table(),
                       set_clause = self.format_set(),
                       from_clause = self.format_from(),
                       joins = self.format_join(),
                       where_clause = self.format_where(),
                       returning = self.format_returning()
                       ).strip()
        return result


class Insert(BaseQuery, _WithMixin, _ReturningMixin):

    def __init__(self, table):
        self.table = table
        super(Insert, self).__init__()

    def add_values(self, **kwargs):
        self.check_clause('values', {})
        for k, v in kwargs.iteritems():
            if isinstance(v, Literal):
                arg_c = k
                arg_v = unicode(v)
            else:
                self.bound_parameters[k] = v
                arg_c = k
                arg_v = u'%({0})s'.format(k)
            self.clauses['values'][arg_c] = arg_v
            
    def format_values(self):
        if not self.clauses.get('values'):
            return u''
        buf1 = []
        buf2 = []
        for k, v in self.clauses.get('values').iteritems():
            buf1.append(k)
            buf2.append(v)
        return u'({0}) VALUES ({1})'.format(u', '.join(buf1), u', '.join(buf2))

    def add_query(self, query):
        self.check_clause('query', None)
        if not isinstance(query, (str, unicode, Select)):
            raise TypeError('INSERT query must be a SELECT')
        self.clauses['query'] = query

    def __unicode__(self):
        result = u'''
            {with_clause} INSERT INTO {table_name}
                {values_or_query}
                {returning}
            '''.format(
                       with_clause = self.format_with(),
                       table_name = self.table,
                       values_or_query = self.format_values() if self.clauses.get('values') else unicode(self.clauses.get('query')),
                       returning = self.format_returning()
                       ).strip()
        return result

class Delete(BaseQuery, _JoinMixin, _WithMixin, _UsingMixin,
             _WhereMixin, _ReturningMixin):

    def __init__(self, table, alias=None, only=False):
        self.table = table
        self.alias = None
        self.only = only
        super(Delete, self).__init__()
        
    def format_table(self):
        if self.alias is not None:
            return u'{0} AS {1}'.format(self.table, self.alias)
        return self.table
    
    def format_only(self):
        if self.only:
            return u'ONLY'
        return u''

    def __unicode__(self):
        result = u'''
            {with_clause} DELETE {only} {table_name}
                {using_clause}
                {joins}
                {where_clause}
                {returning}
            '''.format(
                       with_clause = self.format_with(),
                       only = self.format_only(),
                       table_name = self.format_table(),
                       using_clause = self.format_using(),
                       joins = self.format_join(),
                       where_clause = self.format_where(),
                       returning = self.format_returning()
                       )
        return result
