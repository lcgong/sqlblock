# -*- coding: utf-8 -*-

import pytest

from sqlblock.old.postgres import set_dsn, transaction
from sqlblock import SQL

# def setup_module(module):
#     set_dsn(dsn='db2', url="postgresql://postgres@localhost/test")

@pytest.fixture(scope='session')
def setup_dsn():
    from sqlblock.old.postgres import set_dsn

    set_dsn(dsn='db', url="postgresql://postgres@localhost/test")


@pytest.fixture(scope='session')
def event_loop(request, setup_dsn):
    """
    To avoid the error that a pending task is attached to a different loop,
    create an instance of the default event loop for each test case.
    """
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@transaction.db
async def func1(db):
    await (db << "SELECT 10 as sn")

    await (db << "SELECT 11 as sn")

    # await func2()

    await (db << "SELECT 12 as sn")

@transaction.db
async def func2(db):
    await (db << "SELECT 21 as sn")

    # await func3()

@transaction.db2
async def func3(db2):
    await (db2 << "SELECT 31 as sn")

@pytest.mark.asyncio
async def test_trans():
    pass
    # await func1()
