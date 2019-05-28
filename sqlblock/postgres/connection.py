import sys
from contextvars import ContextVar
from functools import update_wrapper as update_func_wrapper
from inspect import iscoroutinefunction
from asyncpg import create_pool

from ._sqlblock import SQLBlock


class AsyncPostgresSQL:
    __slots__ = ('_ctxvar', '_pool', '_pool_kwargs')

    def __init__(self, dsn=None, min_size=10, max_size=10):
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
        self._pool_kwargs = dict(dsn=dsn, min_size=min_size, max_size=max_size)
        self._ctxvar = ContextVar('connection')

    def transaction(self, *d_args, renew=False, autocommit=False):
        """Decorate the function to access datasbase.

        :param renew: Force the function with a new connection.
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
