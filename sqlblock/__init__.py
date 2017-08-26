# -*- coding: utf-8 -*-
"""
对函数方法装配sqlblock，实例会增加sql属性，无异常结束提交事务，有异常则回滚.

@transaction

@transaction()

@transaction(auto_commit=True, dsn='other_db')

具体使用，必须提供第一个参数
@transaction()
def do_something(self, arg):
    ...
    return ...

如果该实例已经存在非空的sql属性则会抛出AttributeError

@transaction.sql_a

@transaction(dsn=)

"""

from .sqltext import SQL
