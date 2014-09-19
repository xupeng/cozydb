import time
from unittest import TestCase

import MySQLdb
from nose.tools import eq_, ok_, raises

from cozydb import CozyStore, CozyCursor

class CozydbModuleTest(TestCase):
    db_args = {
        'user': 'root',
        'passwd': '',
        'host': '127.0.0.1',
        'port': 3306,
        'db': 'cozydb_test',
    }

    def _get_tmp_cursor(self):
        _args = self.db_args.copy()
        _args.pop('db', None)
        store = CozyStore(**_args)
        return store.get_cursor()

    def setUp(self):
        cursor = self._get_tmp_cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS `cozydb_test`')
        cursor.execute('USE cozydb_test')
        sql = ("CREATE TABLE IF NOT EXISTS `table1` ("
               " `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
               " `name` varchar(10) DEFAULT NULL,"
               " `rank` int(11) NOT NULL DEFAULT '0',"
               " PRIMARY KEY (`id`)"
               ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
        cursor.execute(sql)
        cursor.close()

    def tearDown(self):
        cursor = self._get_tmp_cursor()
        cursor.execute('DROP DATABASE IF EXISTS `cozydb_test`')
        cursor.close()


    ##########################################################################
    # ClusterManager
    ##########################################################################
    def test_cozydb_cozystore_get_cursor(self):
        store = CozyStore(**self.db_args)
        cursor1 = store.get_cursor()
        cursor2 = store.get_cursor()
        ok_(cursor1 is cursor2)

        cursor1.execute('select 1')
        eq_(cursor1.fetchone(), (1,))

        cursor3 = store.get_cursor(use_cache=False)
        ok_(cursor1 is not cursor3)

        cursor3.execute('select 1')
        eq_(cursor3.fetchone(), (1,))

    def test_cozydb_cozycursor(self):
        cursor = CozyCursor(**self.db_args)
        cursor.execute('select 1')
        eq_(cursor.fetchone(), (1,))

    def test_cozydb_cozycursor_fetchone_dict(self):
        cursor = CozyCursor(**self.db_args)
        cursor.execute('select 1 as name')
        eq_(cursor.fetchone(as_dict=True), {'name': 1})

    def test_cozydb_cozycursor_fetchall_dict(self):
        cursor = CozyCursor(**self.db_args)
        cursor.execute('select 1 as name')
        eq_(cursor.fetchall(as_dict=True), ({'name': 1},))

    def test_cozydb_cozycursor_reconnect(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        try:
            cursor1.execute('select 1')
        except MySQLdb.OperationalError:
            pass
        else:
            ok_(False, '%r connection did not break, it should be' % cursor1)

        cursor1.execute('select 1')
        eq_(cursor1.fetchone(), (1,))
        err_msg = 'Thread id did not change after re-connect, it should be'
        ok_(thread_id1 != cursor1.connection.thread_id(), err_msg)

    def test_cozydb_cozycursor_retry_select(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('select 1', retry=1)
        eq_(cursor1.fetchone(), (1,))
        err_msg = 'Thread id did not change after re-connect, it should be'
        ok_(thread_id1 != cursor1.connection.thread_id(), err_msg)

    def test_cozydb_cozycursor_retry_show(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('show variables like "hostname"', retry=1)
        ok_(cursor1.fetchone())
        err_msg = 'Thread id did not change after re-connect, it should be'
        ok_(thread_id1 != cursor1.connection.thread_id(), err_msg)

    @raises(MySQLdb.OperationalError)
    def test_cozydb_cozycursor_does_not_retry_update(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('update table1 set rank=%s where id=%s', (1, 1), retry=1)
        cursor1.connection.rollback()

    @raises(MySQLdb.OperationalError)
    def test_cozydb_cozycursor_does_not_retry_insert(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('insert into table1 (name, rank) values (%s, %s)',
                        ('name4', 4),
                        retry=1)
        cursor1.connection.rollback()

    @raises(MySQLdb.OperationalError)
    def test_cozydb_cozycursor_does_not_retry_delete(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('delete from table1 where id=%s', (100, ), retry=1)
        cursor1.connection.rollback()

    def test_cozydb_cozycursor_force_retry(self):
        cursor1 = CozyCursor(**self.db_args)
        thread_id1 = cursor1.connection.thread_id()

        # Kill connection of cursor1, test re-connect ability then
        cursor2 = CozyCursor(**self.db_args)
        cursor2.execute('kill %d' % thread_id1)

        # wait MySQL server for disconnecting killed connection
        time.sleep(0.02)

        cursor1.execute('update table1 set rank=%s where id=%s', (1, 1),
                        retry=1,
                        force_retry=True)
        cursor1.connection.rollback()
