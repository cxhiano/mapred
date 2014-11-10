""" This module provides functions to serialize and deserialize python dict
which contains function values

Noted that functions to be serialized should not depend on global variable. When
deserializing, only __builtins__ are provided as globals
"""

import types

def loads(dic):
    """ Deserialize

    Transform func_code back into function, only provide __builtins__ as its
    globals.
    """
    ret = {}
    for key in dic:
        value = dic[key]
        if isinstance(value, types.CodeType):
            ret[key] = types.FunctionType(value, {
                '__builtins__': __builtins__ })
        else:
            ret[key] = value

    return ret

def dumps(dic):
    """ Serialize

    Transform function to its func_code
    """
    ret = {}
    for key in dic:
        value = dic[key]
        if isinstance(value, types.FunctionType):
            ret[key] = value.func_code
        else:
            ret[key] = value

    return ret
