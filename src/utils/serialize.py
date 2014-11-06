import types

def loads(obj, skeleton):
    for attr in skeleton:
        value = skeleton[attr]
        if isinstance(value, types.CodeType):
            setattr(obj, attr, types.FunctionType(value, {}))
        else:
            setattr(obj, attr, skeleton[attr])

def dumps(obj):
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
