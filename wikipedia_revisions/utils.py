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
    computes the next value of an iterable in a different threads

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


def queue_to_iterator(
    q: Queue, timeout: float = 10, should_continue: Callable[[], bool] = lambda: True
):
    try:
        while should_continue():
            yield q.get(timeout=timeout)
            q.task_done()
    except Empty:
        pass


def test_queue_to_iterator():
    q = Queue()
    q.put(1)
    q.put(2)
    q.put(3)
    assert list(queue_to_iterator(q)) == [1, 2, 3]


def test_queue_to_iterator_continue_param():
    q = Queue()
    for i in range(10):
        q.put(i)

    return list(queue_to_iterator(q, should_continue=lambda: not q.empty())) == list(
        range(10)
    )


def timestr() -> str:
    return datetime.datetime.now().isoformat()
