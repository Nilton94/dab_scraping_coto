import time
from functools import wraps

try:
    from modules.utils.logger import get_logger
    logger = get_logger()
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

def retry(func=None, retries=3, delay=1):
    """
    Retry decorator to retry a function call a specified number of times.

    Args:
        func (callable): The function to be retried.
        retries (int): The number of times to retry the function. Default is 3.
        delay (int): The delay between retries in seconds. Default is 1.
    Returns:
        callable: The wrapped function that will retry on failure.
    """
    
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    logger.info(f"Attempt {attempt + 1}/{retries} for {f.__name__}")
                    result = f(*args, **kwargs)
                    logger.info(f"{f.__name__} succeeded on attempt {attempt + 1}")
                    return result
                except Exception as e:
                    if attempt < retries - 1:
                        logger.warning(
                            f"{f.__name__} failed on attempt {attempt + 1}/{retries}: {str(e)}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{f.__name__} failed after {retries} attempts: {str(e)}"
                        )
                        raise e
        return wrapper
    
    if func is None:
        return decorator
    else:
        return decorator(func)