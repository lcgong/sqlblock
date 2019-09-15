import asyncio
from sqlblock import AsyncPostgresSQL, SQL

db = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")


@db.transaction
async def hello_world():

    await create_table()
    await init_data(start_sn=100)

    SQL("SELECT * FROM tmp_tbl") >> db

    assert [r.sn async for r in db] == [100, 101, 102, 103]

    async for r in db:
        print(r.sn)

async def create_table():
    SQL("""
    CREATE TEMPORARY TABLE tmp_tbl (
        sn INTEGER
    )
    """) >> db
    await db

async def init_data(start_sn):
    for i in range(4):
        SQL("INSERT INTO tmp_tbl (sn) VALUES ({start_sn + i}) ") >> db
        await db


async def main():
    async with db:
        await hello_world()

asyncio.run(main())
