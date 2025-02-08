import functools

CACHE_SIZES = {}

def cache_function(maxsize=128):
    def decorator(func):
        size = CACHE_SIZES.get(func.__name__, maxsize)
        cached_func = functools.lru_cache(maxsize=size)(func)

        def wrapper(*args, **kwargs):
            return cached_func(*args, **kwargs)

        wrapper.cache_clear = cached_func.cache_clear
        wrapper.cache_info = cached_func.cache_info
        wrapper.__name__ = func.__name__

        return wrapper
    return decorator
