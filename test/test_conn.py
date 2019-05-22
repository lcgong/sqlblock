
import asyncio

from sqlblock.sqltext import SQL
from sqlblock.connection import AsyncPGConnection

import pytest

@pytest.mark.asyncio
async def test_transaction_1():
  conn = AsyncPGConnection(url="postgresql://postgres@localhost/test")

  @conn.transaction(autocommit=True)
  async def func0():
    conn << SQL("select 123 as a ") 
    r = await conn.fetchfirst()
    assert r.a == 123

  async with conn:
    await func0()

@pytest.mark.asyncio
async def test_transaction():
  conn = AsyncPGConnection(url="postgresql://postgres@localhost/test")
  

  @conn.transaction(autocommit=True)
  async def func0():

    sn = 100
    conn << SQL("""
    SELECT * FROM (VALUES 
        ({sn} + 1, {no} + 1, 'one'), 
        ({sn} + 1, {no} + 2, 'two'), 
        ({sn} + 1, {no} + 3, 'three')
    ) AS t(a, b, c);
    """)

    await func1()

    await func2()

    await func1()

    r = await conn.fetchfirst(no=300)
    assert r.a == 101 and r.b == 301
    print(r)

  @conn.transaction
  async def func1():
    sn = 100
    conn << SQL("select {sn}::INTEGER as a, {no}::INTEGER as b ")  
    cursor = await conn.fetch(no=100)
    rows = [r async for r in cursor ]
    print(rows)

  async def func2():
    await conn.fetch(no=200)
    rows = [r async for r in conn] + [r async for r in conn]
    print(rows)

  async with conn:
    await func0()