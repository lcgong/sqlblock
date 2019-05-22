import logging

from asyncpg import create_pool
import inspect

import sys
from inspect import iscoroutinefunction
from functools import update_wrapper as update_func_wrapper
from contextvars import ContextVar
from .sqltext import SQLText

from dataclasses import make_dataclass

# _logger = logging.getLogger('sqlblock')


class AsyncPGConnection:
    __slots__ = ('_ctxvar', '_pool', '_pool_kwargs')

    def __init__(self, dsn=None, min_size=10, max_size=10):
        self._pool_kwargs = dict(dsn=dsn, min_size=min_size, max_size=max_size)
        self._ctxvar = ContextVar('connection')

    def transaction(self, *d_args, autocommit=False):
        def _sqlblk_decorator(func):

            async def _sqlblock_wrapper(*args, **kwargs):
                ctxvar = self._ctxvar
                pool = self._pool

                block = ctxvar.get(None)
                if block is None:
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

        sqlblock.join(sqltext, frame=sys._getframe(1))

        return self

    async def execute(self, **params):
        sqlblock: SQLBlock = self._ctxvar.get()

        return await sqlblock.execute(**params)

    async def fetch(self, **params):
        sqlblock: SQLBlock = self._ctxvar.get()

        return await sqlblock.fetch(**params)

    async def fetchfirst(self, **params):
        sqlblock: SQLBlock = self._ctxvar.get()

        return await sqlblock.fetchfirst(**params)

    def __aiter__(self):
        sqlblock: SQLBlock = self._ctxvar.get()
        return sqlblock.__aiter__()


class SQLBlock:
    __slots__ = ('_conn', '_sqltext', '_cursor', '_row_type',
                 '_executed', '_autocommit', '_parent')

    def __init__(self, conn, autocommit=False, parent=None):
        self._conn = conn
        self._autocommit = autocommit
        self._parent = parent

        self._cursor = None
        self._row_type = None
        self._sqltext = SQLText()
        self._executed = False

    def join(self, sqltext, frame=sys._getframe(1)):
        if self._executed:
            self._sqltext.clear() ## next a new SQL statement
            self._executed = False

        self._sqltext._join(sqltext, frame=frame)

        return self

    async def fetchfirst(self, **params):
        """Execute the statement and return the first row.

        :param params: Query arguments
        :return: The first row as a :class:`Rec` instance.
        """

        sql_stmt, sql_vals = self._sqltext.get_statment(params=params)
        if not sql_stmt:
            return

        self._executed = True

        stmt = await self._conn.prepare(sql_stmt)
        record = await stmt.fetchrow(*sql_vals)
        if record is None:
            return None

        _row_type = make_dataclass("Rec",
                                   [a.name for a in stmt.get_attributes()])

        return _row_type(**record)

    async def fetch(self, **params):
        sql_stmt, sql_vals = self._sqltext.get_statment(params=params)
        if not sql_stmt:
            return _EmptyAsyncIterator()

        self._executed = True

        stmt = await self._conn.prepare(sql_stmt)

        if self._autocommit:
            # cursor cannot be created outside of a transaction
            records = await stmt.fetch(*sql_vals)
            self._cursor = _AsyncIteratorWrapper(records.__iter__())
        else:
            self._cursor = stmt.cursor(*sql_vals).__aiter__()

        self._row_type = make_dataclass(
            "Rec", [a.name for a in stmt.get_attributes()])

        return self

    async def execute(self, **params):
        """Execute an SQL command (or commands).

        This method can execute many SQL commands at once, when no arguments
        are provided.

        :param params: Query arguments.
        :return str: Status of the last SQL command.
        """

        sql_stmt, sql_vals = self._sqltext.get_statment(params=params)
        if not sql_stmt:
            return

        self._executed = True

        status = await self._conn.execute(sql_stmt, *sql_vals)
        return status

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._cursor is None:
            raise StopAsyncIteration

        try:
            record = await self._cursor.__anext__()
            return self._row_type(**record)
        except StopAsyncIteration:
            self._cursor = None
            self._row_type = None
            raise


class _AsyncIteratorWrapper:
    def __init__(self, _iter):
        self._iter = _iter

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self._iter.__next__()
        except StopIteration:
            raise StopAsyncIteration


class _EmptyAsyncIterator:
    def __aiter__(self):
        return self

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
                transaction.rollback()
                raise
        else:
            return await func(*args, **kwargs)
    finally:
        ctxvar.reset(saved_point)