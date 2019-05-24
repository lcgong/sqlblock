
[![Build Status](https://travis-ci.org/lcgong/sqlblock.svg?branch=master)](https://travis-ci.org/lcgong/sqlblock)



```python
import asyncio
from sqlblock import AsyncPostgresSQL, SQL

conn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")

@conn.transaction
async def helloworld():

    SQL("""
    CREATE TEMPORARY TABLE tmp_tbl (
        sn INTEGER
    )
    """) >> conn
    await conn

    await init_data()

    SQL("SELECT * FROM tmp_tbl") >> conn

    assert [r.sn async for r in conn] == [100, 101, 102, 103]
    
    async for r in conn:
        print(r.sn)

async def init_data():
    for i in range(4):
        SQL("INSERT INTO tmp_tbl (sn) VALUES ({100 + i}) ") >> conn
        await conn

async def main():
    async with conn:
        await helloworld()

asyncio.run(main())
```