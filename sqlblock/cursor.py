# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

from collections import namedtuple
from .sqltext import SQLText

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
