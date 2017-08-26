# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

import sys
import asyncpg

from .sqltext import SQLText

_conn_pools = {}
_datasources = {}

def set_dsn(dsn='DEFAULT', url=None, min_size=10, max_size=10):
    _datasources[dsn] = dict(dsn=url, min_size=min_size, max_size=max_size)

async def _get_pool(name):
    pool = _conn_pools.get(name)
    if pool is not None:
        return pool

    ds = _datasources.get(name)
    if ds is None:
        raise NameError('No dsn found: ' + name)

    try:
        pool = await asyncpg.create_pool(**ds)
        _conn_pools[name] = pool
        return pool
    except Exception as exc:
        _logger.error(str(exc))


async def close():
    await asyncio.gather(*(p.close() for p in _conn_pools.values()))



from collections import namedtuple

class RecordCursor:

    def __init__(self, sqlblk):
        self._sqlblock = sqlblk

        self._many_params = None
        self._params = None

        self._idx = None
        self._attr_names = None
        self._records = None
        self._n_records = None
        self._record_type = None

    async def execute(self):
        sqltext = self._sqlblock._sqltext
        self._sqlblock._sqltext = SQLText()

        if self._many_params is not None:
            sql_stmt, sql_vals = sqltext.get_statment(many_params=self._many_params)
            if not sql_stmt:
                return

            await self._sqlblock._conn.executemany(sql_stmt, sql_vals)
            self._many_params = None
            self._params = None

            self._idx = -1
            self._attr_names = None
            self._records = None
            self._n_records = None
            self._record_type = None

            return

        sql_stmt, sql_vals = sqltext.get_statment(params=self._params)
        if not sql_stmt:
            return

        try:
            stmt = await self._sqlblock._conn.prepare(sql_stmt)
            records = await stmt.fetch(*sql_vals)
        except Exception as exc:
            _logger.debug(f"{str(exc):}\nSQL: {sql_stmt}\nPARAMS: {sql_vals}")
            raise

        if not records:
            self._idx = -1
            self._attr_names = None
            self._records = None
            self._n_records = None
            self._record_type = None
            return

        self._attr_names = tuple(a.name for a in stmt.get_attributes())
        self._record_type = namedtuple("Record", self._attr_names)

        self._records = records
        self._n_records = len(records)
        self._idx = 0

    async def __anext__(self):
        if self._sqlblock._sqltext:
            await self.execute()
        idx = self._idx
        if idx is None:
            raise StopAsyncIteration()

        if idx < 0 or idx >= self._n_records:
            self._idx = None
            self._records = None
            self._n_records = None
            self._record_type = None
            raise StopAsyncIteration()

        self._idx += 1
        return self._record_type(*self._records[idx])

    def __next__(self):
        if self._idx is None:
            raise StopIteration

        idx = self._idx
        if idx < 0 or idx >= self._n_records:
            self._idx = None
            self._records = None
            self._n_records = None
            self._record_type = None
            raise StopIteration()

        self._idx += 1
        return self._record_type(*self._records[idx])

class BaseSQLBlock:
    __tuple__ = ('dsn', '_sqltext', '_cursor',
                 '_parent_sqlblk', '_func_module', '_func_name')

    def __init__(self, dsn='DEFAULT',
                parent=None, _func_name=None, _func_module=None):

        self.dsn = dsn
        self._parent_sqlblk = parent
        self._func_name = _func_name
        self._func_module = _func_module

        self._cursor = RecordCursor(self)
        self._sqltext = SQLText()


    async def __enter__(self):
        if self._parent_sqlblk:
            self._conn = self._parent_sqlblk._conn
            return self

        pool = await _get_pool(self.dsn)
        if pool:
            self._conn = await pool.acquire()
            self._transaction = self._conn.transaction()
            await self._transaction.start()

        return self

    async def __exit__ (self, etyp, exc_val, tb):
        if self._parent_sqlblk:
            return False

        if exc_val :
            await self._transaction.rollback()
        else:
            await self._transaction.commit()

        if self._conn:
            pool = await _get_pool(self.dsn)
            await pool.release(self._conn)
            self._conn = None

        return False

    def __lshift__(self, sqltext):
        self._sqltext._join(sqltext, frame=sys._getframe(1))
        return self

    def __await__(self):
        return self.__call__().__await__()

    async def __call__(self, *many_params, **params):
        """
        db(param1=1, param2=2, .... )
        executemany:
            db([dict(param1=1, param2=2, ..), dict(), ...])
        """
        if not many_params:
            self._cursor._params = params
            await self._cursor.execute()
            return self
        else:
            assert len(many_params) == 1

            self._cursor._many_params = many_params[0]
            await self._cursor.execute()

    def __aiter__(self):
        return  self._cursor

    def __iter__(self):
        if self._sqltext:
            self._sqltext.clear()
            raise ValueError(f"There is a sqltext need to 'await {self.dsn}'")

        return  self._cursor

    def __dset__(self, item_type):
        if self._cursor:
            return self._cursor.__dset__(item_type)

        return dset(item_type)()

    def __repr__(self):

        if self._func_name:
            func_str = f"against '{self._func_name}' "
            func_str += f"in '{self._func_module}'"
        else:
            func_str = ""

        return f"<SQLBlock dsn='{self.dsn}' " + func_str +  f" at 0x{id(self):x}>"

from .decorator import TransactionDecoratorFactory
transaction = TransactionDecoratorFactory(BaseSQLBlock)
