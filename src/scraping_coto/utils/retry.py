import time
from functools import wraps

def retry(func, retries=3, delay=1):
    """
    Retry decorator to retry a function call a specified number of times.

    Args:
        func (callable): The function to be retried.
        retries (int): The number of times to retry the function. Default is 3.
        delay (int): The delay between retries in seconds. Default is 1.
    Returns:
        callable: The wrapped function that will retry on failure.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise e

    return wrapper