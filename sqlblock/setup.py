

import sys

from sqlblock.connection import AsyncPGConnection

def aiohttp_setup_sqlblock(app, conn: AsyncPGConnection):

    async def startup(app):
        await conn.__aenter__()

    async def shutdown(app):
        await conn.__aexit__(*sys.exc_info())
        print('closed sqlblock')

    app.on_startup.append(startup)
    app.on_cleanup.append(shutdown)
