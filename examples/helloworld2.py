import asyncio
from sqlblock import AsyncPostgresSQL

sqlb = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/test")


@sqlb.transaction
async def hello_world():

    await create_table()
    await init_data(start_sn=100)

    await sqlb("SELECT * FROM tmp_tbl")

    assert [r.sn async for r in sqlb] == [100, 101, 102, 103]

    async for r in sqlb:
        print(r.sn)

async def create_table():
    await sqlb("""
        CREATE TEMPORARY TABLE tmp_tbl (
            sn INTEGER
        )
    """)

async def init_data(start_sn):
    for i in range(4):
        await sqlb("INSERT INTO tmp_tbl (sn) VALUES ({start_sn + i}) ")


async def main():
    async with sqlb:
        await hello_world()

asyncio.run(main())
