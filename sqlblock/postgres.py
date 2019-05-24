import logging

from asyncpg import create_pool
import inspect

import sys
from inspect import iscoroutinefunction
from functools import update_wrapper as update_func_wrapper
from contextvars import ContextVar
from .sqltext import SQLText

from dataclasses import make_dataclass
from enum import Enum

class AsyncPostgresSQL:
    __slots__ = ('_ctxvar', '_pool', '_pool_kwargs')

    def __init__(self, dsn=None, min_size=10, max_size=10):
        self._pool_kwargs = dict(dsn=dsn, min_size=min_size, max_size=max_size)
        self._ctxvar = ContextVar('connection')

    def transaction(self, *d_args, renew=False, autocommit=False):
        """Decorate the function to access datasbase.

        :param renew: Force the function with a new connection, default is False.
        :param autocommit: autocommit
        """
        def _sqlblk_decorator(func):

            async def _sqlblock_wrapper(*args, **kwargs):
                ctxvar = self._ctxvar
                pool = self._pool

                block = ctxvar.get(None)
                if block is None or renew:
                    try:
                        conn = await pool.acquire()
                        block = SQLBlock(conn, autocommit=autocommit)
                        return await _scoped_invoke(ctxvar, block,
                                                    conn, autocommit,
                                                    func, args, kwargs)
                    finally:
                        await pool.release(conn)
                else:
                    conn = block._conn
                    childBlock = SQLBlock(conn, parent=block,
                                          autocommit=autocommit)

                    return await _scoped_invoke(ctxvar, childBlock, conn,
                                                autocommit, func, args, kwargs)

            return update_func_wrapper(_sqlblock_wrapper, func)

        if len(d_args) > 0 and iscoroutinefunction(d_args[0]):
            # no argument decorator
            return _sqlblk_decorator(d_args[0])
        else:
            return lambda f: _sqlblk_decorator(f)

    async def __aenter__(self):
        """ startup the connection pool """
        self._pool = await create_pool(**self._pool_kwargs)

        return self

    async def __aexit__(self, etyp, exc_val, tb):
        """ gracefull shutdown the connection pool """
        await self._pool.close()
        self._pool = None

    def __lshift__(self, sqltext):
        sqlblock: SQLBlock = self._ctxvar.get()

        sqlblock.join(sqltext, vars=sys._getframe(1).f_locals)

        return self

    async def execute(self, **params):
        sqlblock: SQLBlock = self._ctxvar.get()
        return await sqlblock.fetch(**params)


    def __await__(self):
        sqlblock: SQLBlock = self._ctxvar.get()
        return sqlblock.fetch().__await__()


    async def first(self, **params):
        sqlblock: SQLBlock = self._ctxvar.get()
        return await sqlblock.fetch_first(**params)

    def __aiter__(self):
        sqlblock: SQLBlock = self._ctxvar.get()
        return sqlblock.__aiter__()


class BlockState(Enum):
    PENDING     = 0
    EXECUTED    = 1
    EXHAUSTED   = 2

def make_record_type(stmt):
    return make_dataclass("Rec", [a.name for a in stmt.get_attributes()])

class SQLBlock:
    __slots__ = ('_conn', '_sqltext', '_cursor', '_row_type',
                 '_state', '_autocommit', '_parent', '_statment')

    def __init__(self, conn, autocommit=False, parent=None):
        self._conn = conn
        self._autocommit = autocommit
        self._parent = parent

        self._cursor = None
        self._row_type = None
        self._sqltext = SQLText()
        self._state = BlockState.PENDING
        self._statment = None

    def join(self, sqltext, vars=sys._getframe(1).f_locals):
        if self._state != BlockState.PENDING:
            self._sqltext.clear() ## next a new SQL statement
            self._state = BlockState.PENDING
            self._statment = None

        self._sqltext._join(sqltext, vars=vars)

        return self

    async def fetch_first(self, **params):
        """Execute the statement and return the first record.

        :param params: Query arguments
        :return: The first row as a :class:`Rec` instance.
        """
        
        sql_stmt, sql_vals = self._sqltext.get_statment(params=params)
        if not sql_stmt:
            return

        stmt = await self._conn.prepare(sql_stmt)
        record = await stmt.fetchrow(*sql_vals)
        self._state = BlockState.EXHAUSTED
        
        if record is not None:
            record_type = make_record_type(stmt)
            return record_type(**record)

    async def fetch(self, **params):

        sql_stmt, sql_vals = self._sqltext.get_statment(params=params)
        if not sql_stmt:
            return


        conn = self._conn
        stmt = await conn.prepare(sql_stmt)

        if self._autocommit:
            # cursor cannot be created outside of a transaction
            records = await stmt.fetch(*sql_vals)
            self._cursor = _IteratoAsyncrWrapper(records.__iter__())
        else:
            self._cursor = await _fetch_cursor(stmt, sql_vals)

        self._statment = stmt
        self._row_type = make_dataclass(
            "Rec", [a.name for a in stmt.get_attributes()])

        self._state = BlockState.EXECUTED

        return self

    def get_statusmsg(self):
        return self._statment.get_statusmsg()

    def __aiter__(self):
        if self._state == BlockState.EXHAUSTED:
            self._state = BlockState.PENDING
        return self

    async def __anext__(self):
        if  self._state == BlockState.PENDING:
            await self.fetch()
        else:
            if self._state == BlockState.EXHAUSTED:
                raise StopAsyncIteration

        try:
            record = await self._cursor.__anext__()
            return self._row_type(**record)
        except StopAsyncIteration:
            self._state = BlockState.EXHAUSTED
            self._cursor = None
            self._row_type = None
            raise

async def _fetch_cursor(stmt, sql_vals):
    _iter = stmt.cursor(*sql_vals).__aiter__()
    try:
        this_one = await _iter.__anext__()
        return _ThisOneAsyncIterator(_iter, this_one)
    except StopAsyncIteration:
        return _EmptyAsyncrIterator()

class _ThisOneAsyncIterator:

    def __init__(self, _iter, this_one):
        self._iter = _iter
        self._this_one = this_one

    async def __anext__(self):
        this_one = self._this_one
        if this_one is None:
            raise StopAsyncIteration

        try:
            self._this_one = await self._iter.__anext__()
        except StopAsyncIteration:
            self._this_one = None
        
        return this_one

class _IteratoAsyncrWrapper:

    def __init__(self, _iter):
        self._iter = _iter

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self._iter.__next__()
        except StopIteration:
            raise StopAsyncIteration

class _EmptyAsyncrIterator:

    async def __anext__(self):
        raise StopAsyncIteration


async def _scoped_invoke(ctxvar, block, conn, autocommit, func, args, kwargs):
    try:
        saved_point = ctxvar.set(block)
        if not autocommit:
            transaction = conn.transaction()
            await transaction.start()
            try:
                ret_val = await func(*args, **kwargs)
                await transaction.commit()
                return ret_val
            except:
                await transaction.rollback()
                raise
        else:
            return await func(*args, **kwargs)
    finally:
        ctxvar.reset(saved_point)
