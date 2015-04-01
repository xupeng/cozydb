import time
import MySQLdb


class CozyStore(object):
    def __init__(self, *args, **kwargs):
        ''' Accept the same arguments as MySQLdb.connect.
        '''

        self.args = args
        self.kwargs = kwargs
        self._cursor = None

    def get_cursor(self, use_cache=True, lazy=True):
        ''' Get a cursor, return the same cursor if use_cache is True.
        '''

        if not use_cache:
            return CozyCursor(lazy=lazy, *self.args, **self.kwargs)

        if self._cursor is None:
            self._cursor = CozyCursor(lazy=lazy, *self.args, **self.kwargs)
        return self._cursor

    def close(self):
        ''' Close the connection.
        '''

        self._cursor.close()
        self._cursor = None


class CozyCursor(object):
    def __init__(self, lazy=True, *args, **kwargs):
        ''' Accept the same arguments as MySQLdb.connect.
        '''

        self.args = args
        self.kwargs = kwargs
        if 'init_command' not in self.args:
            self.kwargs['init_command'] = 'set names utf8'
        self._cursor = None if lazy else self._get_cursor()

    def _get_cursor(self):
        conn = MySQLdb.connect(*self.args, **self.kwargs)
        return conn.cursor()

    @property
    def cursor(self):
        ''' Get a MySQLdb.cursors.Cursor
        '''

        if self._cursor is None:
            self._cursor = self._get_cursor()
        return self._cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def close(self):
        try:
            self._cursor.close()
            self._cursor.connection.close()
        except Exception:
            pass
        finally:
            self._cursor = None

    def execute(self, sql, args=None, retry=0, sleep=0.1, force_retry=False):
        ''' Execute query, with retry support.

        For transaction safety, only retry readonly queries.
        '''

        readonly_queries = {'select', 'show'}
        query_type = sql.strip().split(None, 1)[0].lower()
        retry = max(int(retry), 0) if force_retry or query_type in \
                readonly_queries else 0
        for i in xrange(retry + 1):
            try:
                return self.cursor.execute(sql, args)
            except MySQLdb.OperationalError, oe:
                # Only re-connect when error code is between 2000 and 3000
                if 2000 <= oe.args[0] < 3000:
                    self.close()
                if i == retry:
                    raise
                else:
                    time.sleep(sleep)

    def fetchone(self, as_dict=False):
        ''' Fetch one record from results set.

        return:
            dict if as_dict is True
            tuple if as_dict is not True
        '''

        result = self.cursor.fetchone()
        if not result:
            return None
        if as_dict is True:
            field_names = [i[0] for i in self.cursor.description]
            result = dict(zip(field_names, result))
        return result

    def fetchall(self, as_dict=False):
        ''' Fetch all records from results set

       return:
            tuple of dicts if as_dict is True
            tuple of tuples if result_type is not True
        '''

        results = self.cursor.fetchall()
        if as_dict is True:
            field_names = [i[0] for i in self.cursor.description]
            results = tuple([dict(zip(field_names, r)) for r in results])
        return results
