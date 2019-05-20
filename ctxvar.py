import asyncio
import contextvars
import random

class DBConnection:

    def __init__(self, default):
        self.__conn_var = contextvars.ContextVar('connection')
        self.__default = default
    
    def __call__(self, func):
        async def wrap(*args, **kwargs):
            conn = self.__conn_var.get(None)
            if conn is None:
                conn = self.__default
                self.__conn_var.set(self.__default)
                ret_val = await func(*args, **kwargs)
                self.__conn_var.set(None)
            else:
                print('---')
                ret_val = await func(*args, **kwargs)

            return ret_val

        return wrap

    async def add(self, value):
        conn = self.__conn_var.get(None)
        self.__conn_var.set(conn + value)

    async def get(self):
        return self.__conn_var.get()

conn = DBConnection(1000)

@conn
async def func1():
    await conn.add(1)
    print(f"c1: {await conn.get()}")
    await func2()
    print(f"c2: {await conn.get()}")

    
@conn
async def func2():
    await conn.add(10)
    print(f"d0: {await conn.get()}")
    

async def request(sn):
    # cls = sn % 3
    await func1()


async def main():
    tasks = [asyncio.create_task(request(100 + i)) for i in range(5)]

    await asyncio.gather(*tasks)

asyncio.run(main())