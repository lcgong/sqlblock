import asyncio
from sqlblock import AsyncPostgresSQL

dbconn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/postgres")

@dbconn.transaction
async def hello_world():
    dbconn.sql("SELECT 1 as no")
    r = await dbconn.fetch_first()
    print(r)

async def main():
    async with dbconn:
        await hello_world()
        await hello_world()

asyncio.run(main())