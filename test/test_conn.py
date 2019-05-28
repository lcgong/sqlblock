
import asyncio

from sqlblock.sqltext import SQL
from sqlblock.postgres.connection import AsyncPostgresSQL

import pytest


@pytest.fixture
async def conn():
    conn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")
    async with conn:
        yield conn


@pytest.mark.asyncio
async def test_simple_1(conn):

    @conn.transaction
    async def func():

        SQL("SELECT 1001::INTEGER AS sn ") >> conn

        sn_list = [r.sn async for r in conn]
        assert sn_list == [1001]

    await func()


@pytest.mark.asyncio
async def test_simple_2(conn):

    @conn.transaction
    async def func():

        SQL("SELECT {sn}::INTEGER AS sn ") >> conn
        await conn.execute(sn=1001)  # 程序上下文中没有定义sn，单独设置sn

        sn_list = [r.sn async for r in conn]
        assert sn_list == [1001]

    await func()


@pytest.mark.asyncio
async def test_context_1(conn):

    @conn.transaction
    async def func():
        for sn in range(1001, 1002):
            SQL("SELECT {sn}::INTEGER AS sn ") >> conn

        # 虽然程序的上下文定义了sn，但单独设置会覆盖原来的sn值
        await conn.execute(sn=1002)

        assert [r.sn async for r in conn] == [1002]

    await func()


@pytest.mark.asyncio
async def test_context_2(conn):

    @conn.transaction
    async def func1():

        SQL("SELECT * FROM (VALUES") >> conn

        n = 4
        for i in range(1, n):
            SQL("({i}::INTEGER),") >> conn  # i是从1到n的值
        else:
            SQL("({n}::INTEGER)") >> conn
        SQL(") AS t(a);") >> conn

        assert [r.a async for r in conn] == [1, 2, 3, 4]

    @conn.transaction
    async def func2():

        SQL("SELECT * FROM (VALUES") >> conn

        n = 4
        for i in range(1, n):
            SQL("({i}::INTEGER),") >> conn  # i是从1到n的值
        else:
            SQL("({n}::INTEGER)") >> conn
        SQL(") AS t(a);") >> conn

        await conn.execute(i=10, n=20)  # 单独设置将覆盖前面的上下文定义的变量

        assert [r.a async for r in conn] == [10, 10, 10, 20]

    await func1()
    await func2()


@pytest.mark.asyncio
async def test_sql_command(conn):

    @conn.transaction
    async def block_func():
        SQL(" CREATE TEMPORARY TABLE test_sn (sn INTEGER) ") >> conn
        await conn
        assert [r async for r in conn] == []

        await (SQL("INSERT INTO test_sn (sn) VALUES (1001)") >> conn)
        assert [r async for r in conn] == []

        await (SQL("INSERT INTO test_sn (sn) VALUES (1002)") >> conn)
        assert [r async for r in conn] == []

        await (SQL("SELECT sn FROM test_sn") >> conn)

        sn_list = [r.sn async for r in conn]
        assert sn_list == [1001, 1002]

        sn_list = [r.sn async for r in conn]
        assert sn_list == [1001, 1002]

        record = await conn.first()
        assert record.sn == 1001

        SQL("SELECT sn + 100 AS sn FROM test_sn") >> conn
        assert [r.sn async for r in conn] == [1101, 1102]

    await block_func()


@pytest.mark.asyncio
async def test_transaction(conn):

    @conn.transaction(autocommit=True)
    async def func():

        sn = 100
        conn << SQL("""
        SELECT * FROM (VALUES
            ({sn} + 1, {no} + 1, 'one'),
            ({sn} + 1, {no} + 2, 'two'),
            ({sn} + 1, {no} + 3, 'three')
        ) AS t(a, b, c);
        """)

        await func1()

        # await func2()
        # r = await conn.first(no=300)
        # assert r.a == 101 and r.b == 301

        await func1()

    @conn.transaction
    async def func1():
        sn = 100
        conn << SQL("select {sn}::INTEGER as a, {no}::INTEGER as b ")
        await conn.execute(no=100)
        rows = [r async for r in conn]
        print(rows)

    async def func2():
        await conn.execute(no=200)
        rows = [r async for r in conn]

        await conn.execute(no=200)
        [r async for r in conn]
        print(rows)

    await func()


@pytest.mark.asyncio
async def test_async_task(conn):
    background_task = None

    @conn.transaction
    async def foreground():
        SQL("SELECT 1 AS sn") >> conn
        assert (await conn.first()).sn == 1
        nonlocal background_task
        background_task = asyncio.create_task(background())

    # 作为后台任务，必须是独立的连接，不能和原来调用共享连接。
    # 当遇到下面错误时，极有可能是这个原因
    # asyncpg.exceptions._base.InterfaceError:
    # cannot use Connection.transaction() in a manually started transaction
    @conn.transaction(renew=True)
    async def background():
        await asyncio.sleep(0.1)  # 模拟func执行后，才继续进行的后台任务

        SQL("SELECT 1234 AS sn") >> conn
        return (await conn.first()).sn

    await foreground()

    await asyncio.gather(background_task)


@pytest.mark.asyncio
async def test_dirty_read(conn):

    @conn.transaction
    async def func():
        SQL("SELECT 1 AS sn") >> conn
        assert (await conn.first()).sn == 1

        looked = await lookup()
        assert looked == 1234

    @conn.transaction(renew=True, autocommit=True)
    async def lookup():
        SQL("SELECT 1234 AS sn") >> conn
        return (await conn.first()).sn

    await func()
