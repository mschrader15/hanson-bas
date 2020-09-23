import time
from functools import wraps


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print('func:{0} took: {1} sec'.format(f.__name__, te - ts))
        return tuple([r for r in result] + [te - ts]) if isinstance(result, tuple) else (result, te - ts)
    return wrap