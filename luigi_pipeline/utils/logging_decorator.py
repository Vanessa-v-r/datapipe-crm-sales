# utils/logging_decorator.py

import functools
import structlog

def log_function(func):
    """
    Decorator to log function entry, exit, and exceptions.
    Assumes that the first argument is 'self' with a 'logger' attribute.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Attempt to retrieve 'self.logger' if 'self' exists
        if args and hasattr(args[0], 'logger'):
            logger = getattr(args[0], 'logger')
        else:
            logger = structlog.get_logger(func.__module__)

        func_name = func.__name__
        logger.info(event="function_entry", message=f"Entering function '{func_name}'.")

        try:
            result = func(*args, **kwargs)
            logger.info(event="function_exit", message=f"Exiting function '{func_name}'.")
            return result
        except Exception as e:
            logger.error(event="function_exception", message=f"Exception in function '{func_name}': {str(e)}", exception=str(e))
            raise
    return wrapper
