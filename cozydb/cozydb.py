import time
import MySQLdb


class CozyStore(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.cursor = None

    def get_cursor(self, use_cache=True):
        if not use_cache:
            return CozyCursor(*self.args, **self.kwargs)

        if self.cursor is None or self.cursor.is_closed:
            self.cursor = CozyCursor(*self.args, **self.kwargs)
        return self.cursor

    def close(self):
        self.cursor.connection.close()
        self.cursor = None


class CozyCursor(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        if 'init_command' not in self.args:
            self.kwargs['init_command'] = 'set names utf8'
        self.cursor = self.get_raw_cursor()

    def get_raw_cursor(self):
        conn = MySQLdb.connect(*self.args, **self.kwargs)
        cursor = conn.cursor()
        cursor.execute('select @@version')
        rs = cursor.fetchone()
        if rs:
            if isinstance(rs, dict) and '@@version' in rs:
                self._server_version = rs['@@version']
            elif isinstance(rs, tuple):
                self._server_version = rs[0]
        return cursor

    def __getattr__(self, name):
        if self.is_closed:
            self.cursor = self.get_raw_cursor()
        return getattr(self.cursor, name)

    def close(self):
        try:
            self.cursor.connection.close()
            self.cursor.close()
        except Exception:
            pass
        self.cursor = None

    @property
    def server_version(self):
        return getattr(self, '_server_version', '')

    @property
    def is_closed(self):
        return self.cursor is None

    def execute(self, sql, args=None, retry=0, sleep=0.1, force_retry=False):
        ''' Execute query, with retry support


        For transaction safety, only retry SELECT query
        '''

        read_only_queries = {'select', 'show'}
        query_type = sql.strip().split(None, 1)[0].lower()
        retry = max(int(retry), 0) if force_retry or \
            query_type in read_only_queries else 0
        for i in xrange(retry + 1):
            if self.is_closed:
                self.cursor = self.get_raw_cursor()
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
        ''' Fetch one record from results set

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
