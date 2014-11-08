import types

def loads(dic):
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
    ret = {}
    for key in dic:
        value = dic[key]
        if isinstance(value, types.FunctionType):
            ret[key] = value.func_code
        else:
            ret[key] = value

    return ret

def obj_dumps(obj):
    skeleton = {}
    for attr_name in dir(obj):
        if attr_name.startswith('_'):
            continue
        attr = getattr(obj, attr_name)
        if isinstance(attr, types.MethodType):
            continue
        if isinstance(attr, types.FunctionType):
            skeleton[attr_name] = attr.func_code
        else:
            skeleton[attr_name] = attr

    return skeleton
