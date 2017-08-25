
import pytest

import logging
logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%M:%S", level=logging.DEBUG)

@pytest.fixture(scope='session')
def setup_dsn():
    from sqlblock.asyncpg import set_dsn

    set_dsn(dsn='db', url="postgresql://postgres@localhost/test")


@pytest.fixture(scope='session')
def event_loop(request, setup_dsn):
    """
    To avoid the error that a pending task is attached to a different loop,
    create an instance of the default event loop for each test case.
    """
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
