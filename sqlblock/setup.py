

def setup_aiphttp_app(app, conn):

    async def startup(app):
        await conn.__aenter__()

    async def shutdown(app):
        await conn.__aexit__()

    app.on_startup.append(startup)
    app.on_cleanup.append(shutdown)
