#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import uuid
import functools
import threading
import logging

import mysql.connector
from mysql.connector import errorcode

# global engine object:
engine = None


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """
    Init mysql engine
    """
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    # 删除特性的key,返回相应的值，若没有，返回value
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True

    engine = _Engine(lambda: mysql.connector.connect(**params))
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


def with_connection(func):
    """@decorator："""

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)

    return _wrapper


@with_connection
def _select(sql, first, *args):
    """mysql查询"""
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


@with_connection
def _execute(sql, *args):
    """执行sql语句，返回影响的行数"""
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            # no transaction enviroment:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def select_one(sql, *args):
    """查询"""
    return _select(sql, True, *args)


def select(sql, *args):
    """查询"""
    return _select(sql, False, *args)


def update(sql, *args):
    """"""
    return _execute(sql, *args)


def insert(table, **kw):
    """"""
    cols, args = zip(*kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % (
        table, ','.join(['%s' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    # logging.info(sql)
    return _execute(sql, *args)


class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        # return self._connect()
        try:
            __connect = self._connect()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        finally:
            return __connect


class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            self.connection = _connection
            logging.info('[CONNECTION] [OPEN] connection <%s>...' % hex(id(_connection)))
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            _connection.close()
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' % hex(id(_connection)))


class _DBCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not (self.connection is None)

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


_db_ctx = _DBCtx()


class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


class DBError(Exception):
    pass


class Dict(dict):
    """"""

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


class _TransactionCtx(object):
    """
    _TransactionCtx object that can handle transactions.
    with _TransactionCtx():
        pass
    """

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            # needs open a connection first:
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions += 1
        logging.info('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions -= 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')


def with_transaction(func):
    """
    A decorator that makes function around transaction.
    >>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     r = update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select_one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select('select * from user where id=?', 9090)
    []
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _TransactionCtx():
            return func(*args, **kw)
    return _wrapper


if __name__ == '__main__':
    # 配置log level
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root', '465084127', 'wp', '192.168.1.101')
    # import doctest
    # doctest.testmod()
    #########################################################
    # update('drop table if exists user')
    # update('create table user (id int primary key, name text,password text)')
    #########################################################
    # select('select * from user')
    #########################################################
    # u = dict(id=1,name='JW', password='######')
    # insert('user', **u)
    # select('select * from user')
    #########################################################
    # update('delete from user where id = 1')
    # select('select * from user')
    #########################################################
    # select_one('select * from user where name=?','JW')
    # select('select * from user where name=?','JW')
    # update('update user set id=?,password=? where name=?', '2','123456', 'JW2')
    # select('select * from user')
    #########################################################
    # select('select * from user')
    #########################################################
    update('drop table if exists urlids')
    update('create table urlids (uk BIGINT UNSIGNED primary key, is_read TINYINT UNSIGNED, last_modified INT UNSIGNED)')
    update('drop table if exists link')
    update('create table link (title TEXT, http_link TEXT)')

