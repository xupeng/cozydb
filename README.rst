cozydb is a cozy MySQL-python wrapper.

Features:

1. Automatically reconnect to MySQL server after disconnectted.
2. Fetch results as dictionary without using DictCursor
3. Support `execute` retry

Example::

  from cozydb import CozyStore
  store = CozyStore(host='server', port=3306, user='test', passwd='test',
                    db='test')
  cursor = store.get_cursor()
  cursor.execute('select name from person where id=%s', (100,))
  cursor.fetchall(as_dict=True)

cozydb.CozyStore has the same params as MySQLdb.connect and cozydb.CozyCursor
has the same interfaces as MySQLdb.cursors.Cursor, so it's easy to replace
your cursors with CozyCursor and have the cozy features.
