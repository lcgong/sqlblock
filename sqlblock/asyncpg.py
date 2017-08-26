# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

import sys
import asyncpg

from .sqltext import SQLText
from .cursor import RecordCursor
from .decorator import TransactionDecoratorFactory

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

transaction = TransactionDecoratorFactory(BaseSQLBlock)
