import time
from functools import wraps
import numpy as np
import cProfile
import pstats
from typing import Union


def time_me(end: str = '\n'):
    """
    Convience decorator with arguments to time the decorated method
    Arguments:
        end: The string to end the message with
    """
    message = '{name:} took {timing:.2f}s'

    def __time_me(f):
        @wraps(f)
        def wrap(*args):
            t = time.time()
            result = f(*args)
            print(message.format(name=f.__name__, timing=time.time() - t), end=end)
            return result

        return wrap

    return __time_me


def profile(sort: Union[pstats.SortKey, None] = pstats.SortKey.TIME, subcalls: bool = True, builtins: bool = True):
    """
    Convience decorator with arguments to profile a method
    Arguments:
        sort: How to sort the profile stats
        subcalls: bool, if True, then subcalls are included in the profile
        builtins: bool, if True, then builtins are included in the profile
    """
    def __profile(f):
        @wraps(f)
        def wrap(*args):
            pr = cProfile.Profile(subcalls=subcalls, builtins=builtins)
            pr.enable()
            result = f(*args)
            pr.disable()

            stats = pstats.Stats(pr)
            if sort is not None:
                stats.sort_stats(sort)
            stats.print_stats()
            return result
        return wrap
    return __profile


class ProgressBar:
    def __init__(self, target: int, resolution: int = 100, length: int = 20, sign: str = '\u2588'):
        self.__progress = 0
        self.__time = time.perf_counter()
        self.__step = max(1, target // resolution)
        self.__time_record = [(0, time.perf_counter())]
        self.target = target
        self.resolution = resolution
        self.length = length
        self.sign = sign

    def start(self):
        self.__time = time.perf_counter()

    def __update_time(self):
        self.start()
        self.__time_record.append((self.__progress, self.__time))
        if len(self.__time_record) > np.sqrt(self.resolution):
            self.__time_record.pop(0)

    def get_time(self, update: bool = False) -> float:
        if update:
            self.__update_time()
        return self.__time

    def set_target(self, target: int):
        self.target = target
        self.__step = max(1, target // self.resolution)
        self.__time_record = [(0, time.perf_counter())]

    def remaining_time(self) -> float:
        x, y = list(zip(*self.__time_record))
        means = (np.mean(x), np.mean(y))
        diff_sqs = ([a - means[0] for a in x], [a - means[1] for a in y])
        if sum(np.square(diff_sqs[0])) > 0:
            b = sum([a * b for a, b in zip(*diff_sqs)]) / sum(np.square(diff_sqs[0]))
            return b * (100 - self.__progress)
        else:
            return np.nan

    def __str__(self) -> str:
        empty_space = int((self.resolution - self.__progress) / self.resolution * self.length)
        try:
            remaining_time = time.strftime('%H:%M:%S', time.gmtime(self.remaining_time()))
        except ValueError:
            remaining_time = 'NaN'
        return '[' + self.sign * (self.length - empty_space) + ' ' * empty_space + '] {} % '.format(
            int(np.ceil(self.__progress / self.resolution * 100))) + 'Remaining: ' + remaining_time

    def update(self, count: int, prt: bool = False):
        if ((count % self.__step) == 0) | (count >= self.target - 1):
            self.__update_time()
            self.__progress = count / self.target * self.resolution
            if prt:
                print('\r' + self.__str__(), end='')


if __name__ == '__main__':
    @time_me()
    def test(n):
        time.sleep(n)


    test(3)
    test(1)

    goal = 350
    pb = ProgressBar(0)
    pb.set_target(goal)
    pb.start()
    for i in range(goal + 1):
        pb.update(i)
        print(f'\rProgress {pb}', end='')
        time.sleep(0.02)
    print()
    goal = 100
    pb.set_target(goal)
    pb.start()
    for i in range(goal + 1):
        pb.update(i)
        print(f'\rProgress {pb}', end='')
        time.sleep(0.1)

