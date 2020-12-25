
[![Build Status](https://travis-ci.org/lcgong/sqlblock.svg?branch=master)](https://travis-ci.org/lcgong/sqlblock)


*Read this in other languages: [English](README.md),  [简体中文](README.zh-cn.md).*


```python
import asyncio
from sqlblock import AsyncPostgresSQL

conn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")

@conn.transaction
async def hello_world():

    await create_table()
    await init_data(start_sn=100)

    conn.sql("SELECT * FROM tmp_tbl")

    assert [r.sn async for r in conn] == [100, 101, 102, 103]

    async for r in conn:
        print(r.sn)

async def create_table():
    await conn.sql("""
    CREATE TEMPORARY TABLE tmp_tbl (
        sn INTEGER
    )
    """)

async def init_data(start_sn):
    for i in range(4):
        await conn.sql("INSERT INTO tmp_tbl (sn) VALUES ({start_sn + i}) ")

async def main():
    async with conn:
        await hello_world()

asyncio.run(main())
```