from setup.initialize import logger


def log_func(func, log=logger, level="debug", arguments=True):
    """
    decorator for any function, which logs the function call and the parameters at a level specified.
    use like this:

    @log_func
    def function():
        <function content>

    :param func:
    :param log:
    :param level:
    :return:
    """
    def inner(*args, **kwargs):
        log_string =f"Called {func.__name__}" + (f" (args:{args}, kwargs: {kwargs})" if arguments else '')
        if level == "debug":
            log.debug(log_string)
        elif level == "info":
            log.info(log_string)
        elif level == "warning":
            log.warning(log_string)
        elif level == "error":
            log.error(log_string)
        else:
            pass
        return func(*args, **kwargs)

    return inner
