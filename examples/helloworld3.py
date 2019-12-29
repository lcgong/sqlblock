import asyncio
from sqlblock import AsyncPostgresSQL

db = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")

@db.transaction
async def hello_world():

    await create_table()
    await init_data(start_sn=100)

    db.sql("SELECT * FROM tmp_tbl")

    assert [r.sn async for r in db] == [100, 101, 102, 103]

    async for r in db:
        print(r.sn)

async def create_table():
    db.sql("""
    CREATE TEMPORARY TABLE tmp_tbl (
        sn INTEGER
    )
    """)
    
    await db

async def init_data(start_sn):
    for i in range(4):
        db.sql("INSERT INTO tmp_tbl (sn) VALUES ({start_sn + i}) ")
        await db


async def main():
    async with db:
        await hello_world()

asyncio.run(main())
