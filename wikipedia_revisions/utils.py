from typing import Iterable, TypeVar, Callable
from concurrent.futures import Executor, ThreadPoolExecutor
import datetime
from functools import partial
from queue import Queue, Empty

import dill

dill.settings["recurse"] = True

T = TypeVar("T")
T2 = TypeVar("T2")


class Exhausted:
    ...


def peek_ahead(executor: Executor, iterable: Iterable[T]) -> Iterable[T]:
    """
    computes the next value of an iterable in a different thread

    :param executor:
    :param iterable:
    :return:
    """
    is_exhausted = False
    exhausted = Exhausted()
    iterator = iter(iterable)
    next_future = executor.submit(partial(next, iterator, exhausted))
    while not is_exhausted:
        next_value = next_future.result()
        if next_value is exhausted:
            is_exhausted = True
        else:
            next_future = executor.submit(partial(next, iterator, exhausted))
            yield next_value


def test_peek_ahead_iter():
    with ThreadPoolExecutor() as ex:
        assert list(range(10)) == list(peek_ahead(ex, iter(range(10))))


def test_peek_ahead_range():
    with ThreadPoolExecutor() as ex:
        assert list(range(10)) == list(peek_ahead(ex, range(10)))


def timestr() -> str:
    return datetime.datetime.now().isoformat()
