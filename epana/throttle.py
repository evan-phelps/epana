from datetime import datetime, timedelta
from time import sleep
from functools import wraps


# Throttles decorated function to a rate less than per_sec calls per second.
# No consideration is given to multithreading scenarios, which could require
# either global or thread-specific throttling.  Global throttling would be
# required by original use cases.
class throttle(object):
    def __init__(self, per_sec=20):
        self.period = timedelta(microseconds=1000000 / per_sec)
        self.t0 = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            t1 = datetime.now()
            dt = t1 - self.t0
            secs2wait = (self.period - dt).total_seconds()
            if secs2wait > 0:
                sleep(secs2wait)

            self.t0 = datetime.now()
            retval = fn(*args, **kwargs)

            return retval

        return wrapper


def measure_throttle(n=5, per_sec=1):

    @throttle(per_sec=per_sec)
    def get_time():
        return datetime.now()

    times = [get_time() for i in range(0, n)]
    laps = [(t - times[0]).total_seconds() for t in times]
    avg_period = sum(
        [t2 - t1 for (t1, t2) in zip(laps[:-1], laps[1:])]) / (len(laps) - 1)
    return 1.0 / avg_period
