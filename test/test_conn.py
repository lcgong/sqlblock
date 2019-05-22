
import asyncio

from sqlblock.sqltext import SQL
from sqlblock.connection import AsyncPGConnection

conn = AsyncPGConnection(url="postgresql://postgres@localhost/test")


async def main():
  async with conn:
    await hi()

@conn.transaction(autocommit=True)
async def hi():

  sn = 100
  conn << SQL("""
  SELECT * FROM (
    VALUES 
      ({sn} + 1, {no} + 1, 'one'), 
      ({sn} + 1, {no} + 2, 'two'), 
      ({sn} + 1, {no} + 3, 'three')
  ) AS t(a, b, c);
  """)

  # 
  # conn << SQL("select {sn}::INTEGER as a, {no}::INTEGER as b ")

  await func1()

  print('----------------')

  await func2()

  r = await conn.fetchfirst(no=300)
  assert r.a == 101 and r.b == 301
  print(r)

@conn.transaction
async def func1():
  sn = 2000
  conn << SQL("select {sn}::INTEGER as a, {no}::INTEGER as b ")  
  cursor = await conn.fetch(no=100)
  rows = [r async for r in cursor ]
  print(rows)

async def func2():
  await conn.fetch(no=200)
  rows = [r async for r in conn] + [r async for r in conn]
  print(rows)

asyncio.run(main())