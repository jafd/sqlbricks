'''
Created on 21 Mar 2013

@author: jafd
'''

from collections import OrderedDict

class BaseQuery(object):
    """
    The base query which sets API.
    """
    
    def __init__(self):
        self.clauses = {}
        self.bound_parameters = {}

    def __str__(self):
        return str(self.__unicode__())
        
    def __unicode__(self):
        raise NotImplementedError

class _BaseMixin(object):

    def check_clause(self, clause, initial=None):
        if initial is None: initial = OrderedDict()
        if not self.clauses.get(clause):
            self.clauses[clause] = initial
    
class _TblListMixin(_BaseMixin):

    def add_tables(self, clause, *args, **kwargs):
        self.check_clause(clause)
        for arg in args:
            self.clauses[clause][unicode(arg)] = True
        for k, v in kwargs.iteritems():
            ks = u"{0} AS {1}".format(v, k)
            self.clauses[clause][ks] = True
    
    def format_tables(self, clause, word):
        result = u''
        if self.clauses.get(clause):
            cf = self.clauses.get(clause).keys()
            result = u'{1} {0}'.format(u', '.join(cf), word)
        return result
    
class _FromMixin(_TblListMixin):

    def add_from(self, *args, **kwargs):
        return self.add_tables('from', *args, **kwargs)
    
    def format_from(self):
        return self.format_tables('from', 'FROM')
    
class _UsingMixin(_TblListMixin):

    def add_using(self, *args, **kwargs):
        return self.add_tables('using', *args, **kwargs)
    
    def format_using(self):
        return self.format_tables('using', 'USING')
    
class _JoinMixin(_BaseMixin):
    
    def add_join(self, *args):
        self.check_clause('join')
        for arg in args:
            self.clauses['join'][unicode(arg)] = True

    def format_join(self):
        result = u''
        if self.clauses.get('join'):
            cf = self.clauses.get('join').keys()
            result = u" ".join(cf)
        return result


class _CondMixin(_BaseMixin):

    def add_cond(self, cond, *args):
        self.check_clause(cond)
        for arg in args:
            self.clauses[cond][unicode(arg)] = True
        
    def format_cond(self, cond, prefix):
        result = u''
        if self.clauses.get(cond):
            cf = self.clauses.get(cond).keys()
            result = u"{0} {1}".format(prefix,
                                       u" AND ".join([u'({0})'.format(x) for x in cf]))
        return result
    
class _WhereMixin(_CondMixin):
    
    def add_where(self, *args):
        return self.add_cond('where', *args)
    
    def format_where(self):
        return self.format_cond('where', 'WHERE')


class _HavingMixin(_CondMixin):
    
    def add_having(self, *args):
        return self.add_cond('having', *args)
    
    def format_having(self):
        return self.format_cond('having', 'HAVING')


class _FieldListMixin(_BaseMixin):

    def add_fields(self, *args, **kwargs):
        self.check_clause('fields')
        for arg in args:
            self.clauses['fields'][unicode(arg)] = True
        for k, v in kwargs.iteritems():
            ks = u"{0} AS {1}".format(v, k)
            self.clauses['fields'][ks] = True
    
    def format_fields(self):
        result = u''
        if self.clauses.get('fields'):
            cf = self.clauses.get('fields').keys()
            result = u', '.join(cf)
        return result


class _OrderMixin(_BaseMixin):

    def add_order(self, *args):
        self.check_clause('order', [])
        for arg in args:
            self.clauses['order'].append(arg)
    
    def format_order(self):
        result = u''
        if self.clauses.get('order'):
            buf = []
            for o in self.clauses['order']:
                if isinstance(o, (list, tuple)):
                    buf.append(u"{0} {1}".format(*o))
                else:
                    buf.append(u'{0} ASC'.format(unicode(o)))
            result = u', '.join(buf)
        return result

class _GroupMixin(_BaseMixin):

    def add_group(self, *args):
        self.check_clause('group', set())
        for arg in args:
            self.clauses['group'].add(arg)
    
    def format_group(self):
        result = u''
        if self.clauses.get('group'):
            result = u', '.join(self.clauses.get('group'))
        return result
    
class _LimitMixin(_BaseMixin):
    
    def add_limit(self, limit, offset=0):
        if limit is not None:
            self.check_clause('limit', None)
            self.clauses['limit'] = limit
        if offset is not None:
            self.check_clause('offset', None)
            self.clauses['offset'] = offset
    
    def format_limit(self):
        result = []
        if self.clauses.get('limit'):
            result.append(u'LIMIT {0}'.format(self.clauses['limit']))
        if self.clauses.get('offset'):
            result.append(u'OFFSET {0}'.format(self.clauses['offset']))
        return u' '.join(result)

class _WithMixin(_BaseMixin):
    RECURSIVE = 1
    def add_with(self, **kwargs):
        self.check_clause('with')
        for k, v in kwargs:
            if isinstance(v, (list, tuple, set)):
                if not isinstance(v[0], (BaseQuery, str, unicode)):
                    raise TypeError("WITH clause must contain other statements")
                self.clauses['with'][k] = v
            else:
                if not isinstance(v, (BaseQuery, str, unicode)):
                    raise TypeError("WITH clause must contain other statements")
                self.clauses['with'][k] = (v, 0)
    
    def format_with(self):
        if not self.clauses.get('with'):
            return u''
        result = []
        fmt = u'{0} ({1}) AS {2}'
        for k, v in self.clauses.get('with').iteritems():
            recursive = u'RECURSIVE' if v[1] & self.RECURSIVE else u''
            result.append(fmt.format(recursive, unicode(v[0]), k))
        return u'WITH {0}'.format(u', '.join(result))

