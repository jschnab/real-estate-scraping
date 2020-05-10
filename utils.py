import errno
import hashlib
import os
import signal

from functools import wraps


class Explored:
    def __init__(self):
        self.explored = set()

    def add(self, *args):
        """
        Add args to the set.
        Note: args should be of type 'string'.
        """
        for a in args:
            if not isinstance(a, str):
                raise TypeError(f"Expected {a} to be str, got {type(a)}")
            self.explored.add(hashlib.md5(a.encode()).hexdigest())

    def contains(self, *args):
        """
        Check if the args are present in the set.
        Returns True if none of the args are in the set, else False.
        Note: args should be of type 'string'.
        """
        for a in args:
            if not isinstance(a, str):
                raise TypeError(f"Expected {a} to be str, got {type(a)}")
            if hashlib.md5(a.encode()).hexdigest() in self.explored:
                return True
        return False


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def cut_url(url):
    """
    If URL is longer than 50 characters, show the last 45.
    Useful for logging.

    :param str url:
    :return str: short URL
    """
    if len(url) > 50:
        return f"...{url[-45:]}"
    return url
