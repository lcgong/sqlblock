import sys
import asyncio
from contextvars import ContextVar
from functools import update_wrapper as update_func_wrapper
from inspect import iscoroutinefunction
from asyncpg import create_pool
# from asyncpg.pool import Pool
from asyncpg.pool import Pool as AsyncPGPool

from ._sqlblock import SQLBlock

from sqlblock.utils import json_loads, json_dumps


async def _init_connection(conn: AsyncPGPool):
    # TODO 允许自定义类型
    await conn.set_type_codec(
        'jsonb',
        encoder=json_dumps,
        decoder=json_loads,
        schema='pg_catalog'
    )


class Listener:

    def __init__(self, pool):
        self._pool = pool
        self._conn = None
        self._queues = None

        def _callback(pid, channel, payload):
            print('listened: ', pid, channel, payload)
            queue = self._queues[channel]
            queue.put_nowait(payload)

        self._callback = _callback

        self._lock = asyncio.Lock()

    async def get(self, channel):
        if self._conn is None:
            await self.open()

        if channel not in self._queues:
            await self.register(channel)

        async with self._lock:
            queue = self._queues[channel]

        return await queue.get()

    async def register(self, channel):
        """ register a channel to listen """

        if channel in self._queues:
            raise ValueError(f"The channel has been registered: '{channel}'")

        async with self._lock:
            await self._conn.add_listener(channel, self._callback)
            self._queues[channel] = asyncio.Queue()
            print('registered ', channel)

    async def unregister(self, channel):
        """ unregister a channel """

        async with self._lock:
            await self._conn.remove_listener(channel, self._callback)
            del self._queues[channel]

    async def open(self):
        if self._conn is not None:
            return

        async with self._lock:
            self._conn = await self._pool.acquire()
            self._queues = {}

    async def close(self):
        if self._conn is None:
            return

        async with self._lock:
            for channel in list(self._queues.keys()):
                await self.unregister(channel)

            await self._pool.release(self._conn)
            self._queues = None
            self._conn = None


class AsyncPostgresSQL:
    __slots__ = ('_ctxvar', '_pool', '_pool_kwargs', '_listener')

    def __init__(self, dsn=None, min_size=10, max_size=10, on_init_conn=None):
        """
        Define settings to establish a connection to a PostgreSQL server.

        The connection parameters may be specified either as a connection
        URI in *dsn*, or as specific keyword arguments, or both.
        If both *dsn* and keyword arguments are specified, the latter
        override the corresponding values parsed from the connection URI.

        :param dsn:
            Connection arguments specified using as a single string in the
            `[https://www.postgresql.org/docs/current/libpq-connect.html
            #id-1.7.3.8.3.6](libpq connection URI format)`_:
            ``postgres://user:password@host:port/database?option=value``.

        """
        if not on_init_conn:
            on_init_conn = _init_connection

        self._pool_kwargs = dict(dsn=dsn,
                                 min_size=min_size,
                                 max_size=max_size,
                                 init=on_init_conn)
        self._ctxvar = ContextVar('connection')

        self._listener = None

    def transaction(self, *d_args, renew=False, autocommit=False):
        """Decorate the function to access datasbase.

        :param renew: Force the function with a new connection.
        :param autocommit: autocommit
        """
        def _sqlblk_decorator(func):

            async def _sqlblock_wrapper(*args, **kwargs):
                ctxvar = self._ctxvar
                pool = self._pool
                if pool is None:
                    raise ValueError('pole is none')

                block = ctxvar.get(None)
                if block is None or renew:
                    try:
                        conn = await pool.acquire()

                        block = SQLBlock(conn, autocommit=autocommit)
                        return await _scoped_invoke(ctxvar, block,
                                                    conn, autocommit,
                                                    func, args, kwargs)
                    finally:
                        if pool:
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
        self._pool = create_pool(**self._pool_kwargs)
        await self._pool.__aenter__()

        self._listener = Listener(self._pool)

        return self

    async def __aexit__(self, etyp, exc_val, tb):
        """ gracefull shutdown the connection pool """

        if self._listener is not None:
            await self._listener.close()
            self._listener = None

        await self._pool.__aexit__()
        self._pool = None

    # def __call__(self, *sqltexts, **params):
    #     if not params:
    #         params = _get_ctx_frame(1).f_locals

    #     sqlblock = self._sqlblock
    #     for sqltext in sqltexts:
    #         sqlblock.join(sqltext, vars=params)

    #     return self

    def sql(self, *sqltexts, **params):
        if not params:
            params = _get_ctx_frame(1).f_locals

        sqlblock = self._sqlblock
        for sqltext in sqltexts:
            sqlblock.join(sqltext, vars=params)

        return self

    def __lshift__(self, sqltext):
        self._sqlblock.join(sqltext, vars=_get_ctx_frame(1).f_locals)

        return self

    async def execute(self, **params):
        return await self._sqlblock.fetch(**params)

    def __await__(self):
        return self._sqlblock.fetch().__await__()

    async def fetch_first(self, **params):
        return await self._sqlblock.fetch_first(**params)

    async def first(self, **params):
        return await self._sqlblock.fetch_first(**params)

    def __aiter__(self):
        return self._sqlblock.__aiter__()

    async def listen(self, channel):
        """ listen for Postgres notifications

        The returned value is the payload of notification

        :param str channel: Channel to listen on.
        """

        return await self._listener.get(channel)

    async def notify(self, channel, payload):
        await self._sqlblock._conn.execute("NOTIFY $1 $2", channel, payload)

    @property
    def _sqlblock(self) -> SQLBlock:
        """Get sqlblock in context"""
        sqlblock = self._ctxvar.get()
        return sqlblock


_get_ctx_frame = sys._getframe


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
