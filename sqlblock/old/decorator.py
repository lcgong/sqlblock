# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

from inspect import Signature
import sys, functools, inspect

class TransactionDecoratorFactory:
    def __init__(self, sqlblock_type):
        self._sqlblock_type = sqlblock_type
        self.dsn = None

    def __getattr__(self, dsn):
        return TransactionDecorator(dsn, self._sqlblock_type)
        # if self.dsn is not None:
        #     raise TypeError(f"tranaction decorator has already "
        #                     f"set a dsn '{self.dsn}'")

    def __call__(self, *args, kwargs):
        raise TypeError(f"usage: such as 'transaction.db', 'db' is datasource name")


class TransactionDecorator:
    __tuple__ = ('dsn', '_sqlblock_type')

    def __init__(self, dsn, sqlblock_type):
        self.dsn = dsn
        self._sqlblock_type = sqlblock_type

    def __call__(self, *d_args):
        if self.dsn is None:
            raise TypeError('The dsn agasint this transaction is required')

        _self_dsn = self.dsn

        def _decorator(target_func):
            func_name, func_module = target_func.__name__, target_func.__module__
            func_sig = inspect.signature(target_func)
            if _self_dsn not in func_sig.parameters:
                raise TypeError(f"The parameter '{_self_dsn}' is required "
                                f" in {func_name} in {func_module}")

            if _self_dsn.startswith('_dsn_'): # The referrence of dsn
                _dsn_param = func_sig.parameters[_self_dsn]
                if (_dsn_param.default is Signature.empty
                    or _dsn_param.default is not None):

                    raise TypeError(f"The parameter '{_self_dsn}' should "
                                    f"declare that '{_self_dsn}=None' "
                                    f"in {func_name} in {func_module} ")
                partial_func = target_func
            else: # The normal dsn
                partial_func = functools.partial(target_func, **{_self_dsn: None})
                _update_wrapper(partial_func, target_func)

            async def _sqlblock_wrapper(*args, **kwargs):

                if _self_dsn.startswith('_dsn_'):
                    _dsn_var = kwargs.get(_self_dsn)
                    if _dsn_var is not None:
                        if isinstance(_dsn_var, self._sqlblock_type):
                            _parent_sqlblk = _dsn_var
                        elif isinstance(_dsn_var, str):
                            _parent_sqlblk = _find_parent_sqlblock(_dsn_var)
                            if _parent_sqlblk is None:
                                raise ValueError(
                                    f"no found the parent sqlblock(dsn='{_dsn_var}')"
                                    f" against '{_self_dsn}' while calling"
                                    f" '{func_name}' of {func_module}")
                        else:
                            raise ValueError(
                                f"Unknown the value of '{_self_dsn}'"
                                f": {str(_dsn_var)}")
                    else:
                        _parent_sqlblk = _find_parent_sqlblock(None)

                    if _parent_sqlblk is None:
                        raise ValueError(f"Cannot find the parent sqlblock"
                                         f"agasint '{_self_dsn}'. ")

                else:
                    _parent_sqlblk = _find_parent_sqlblock(_self_dsn)

                __sqlblk_obj = self._sqlblock_type(dsn=_self_dsn,
                                            parent=_parent_sqlblk,
                                            _func_name=func_name,
                                            _func_module=func_module)
                kwargs[_self_dsn] = __sqlblk_obj

                await __sqlblk_obj.__enter__()
                try:
                    # print(55555, args, kwargs)
                    return await target_func(*args, **kwargs)
                finally:
                    await __sqlblk_obj.__exit__(*sys.exc_info())

            functools.update_wrapper(_sqlblock_wrapper, partial_func)
            return _sqlblock_wrapper

        if len(d_args) == 1 and callable(d_args[0]): # no argument decorator
            return _decorator(d_args[0])
        else:
            return _decorator

def _find_parent_sqlblock(dsn):
    frame = sys._getframe(2)
    while frame:
        sqlblk = frame.f_locals.get('_TransactionDecorator__sqlblk_obj')
        if sqlblk and (not dsn or sqlblk.dsn == dsn):
            return sqlblk

        frame = frame.f_back
    return None


# this copied from functools.update_wrapper but remove the __wrapped__ attribute
_WRAPPER_ASSIGNMENTS = ('__module__', '__name__', '__qualname__', '__doc__',
                       '__annotations__')
_WRAPPER_UPDATES = ('__dict__',)
def _update_wrapper(wrapper,
                   wrapped,
                   assigned = _WRAPPER_ASSIGNMENTS,
                   updated = _WRAPPER_UPDATES):
    for attr in assigned:
        try:
            value = getattr(wrapped, attr)
        except AttributeError:
            pass
        else:
            setattr(wrapper, attr, value)
    for attr in updated:
        getattr(wrapper, attr).update(getattr(wrapped, attr, {}))
    return wrapper
