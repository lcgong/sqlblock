

*其它语言版本: [English](README.md),  [简体中文](README.zh-cn.md).*



```python
import asyncio
from sqlblock import AsyncPostgresSQL, SQL

conn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")

@conn.transaction
async def hello_world():

    await create_table()
    await init_data(start_sn=100)

    SQL("SELECT * FROM tmp_tbl") >> conn

    assert [r.sn async for r in conn] == [100, 101, 102, 103]

    async for r in conn:
        print(r.sn)

async def create_table():
    SQL("""
    CREATE TEMPORARY TABLE tmp_tbl (
        sn INTEGER
    )
    """) >> conn
    await conn

async def init_data(start_sn):
    for i in range(4):
        SQL("INSERT INTO tmp_tbl (sn) VALUES ({start_sn + i}) ") >> conn
        await conn


async def main():
    async with conn:
        await hello_world()

asyncio.run(main())

```