from dataclasses import make_dataclass
from enum import Enum

from sqlblock.sqltext import SQLText


class BlockState(Enum):
    PENDING = 0
    EXECUTED = 1
    EXHAUSTED = 2


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

    def join(self, sqltext, vars=None):
        if self._state != BlockState.PENDING:
            self._sqltext.clear()  # next a new SQL statement
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
        self._row_type = make_record_type(stmt)

        self._state = BlockState.EXECUTED

        return self

    def get_statusmsg(self):
        return self._statment.get_statusmsg()

    def __aiter__(self):
        if self._state == BlockState.EXHAUSTED:
            self._state = BlockState.PENDING
        return self

    async def __anext__(self):
        if self._state == BlockState.PENDING:
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
