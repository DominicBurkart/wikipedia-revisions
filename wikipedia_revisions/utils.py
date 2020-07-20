from typing import Iterable, Callable, Generator, TypeVar, Any
import threading
import multiprocessing
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
    Future,
    wait,
    FIRST_COMPLETED,
    ALL_COMPLETED,
)
from enum import Enum
import datetime
from functools import partial
import os
from queue import SimpleQueue, Empty

import dill

dill.settings["recurse"] = True


class Exhausted:
    pass


class _Waiter:
    """
        based on _Waiter class in concurrent.futures._base
    """

    def __init__(self):
        self.event = threading.Event()
        self.finished_futures = []
        self.lock = threading.RLock()
        self.n_pending: int = 0

    def add_result(self, future):
        with self.lock:
            self.finished_futures.append(future)
            self.n_pending -= 1
        self.event.set()

    def add_exception(self, future):
        with self.lock:
            self.finished_futures.append(future)
            self.n_pending -= 1
        self.event.set()

    def add_cancelled(self, future):
        with self.lock:
            self.finished_futures.append(future)
            self.n_pending -= 1
        self.event.set()

    def collect_finished(self):
        with self.lock:
            finished = self.finished_futures
            self.finished_futures = []
            self.event.clear()
        return finished


class Waiter:
    """
        works like concurrent.futures.as_completed, but accepts additional futures during iteration.
        output ordering is arbitrary.
    """

    def __init__(self, futures: Iterable[Future] = []):
        self._waiter = _Waiter()
        self.prior_completed = set()
        self.completion_lock = threading.Lock()
        # ^ when acquired, prevents as_completed from stopping even if there are no running tasks.
        for future in futures:
            self.add(future)

    def add(self, future: Future):
        with self._waiter.lock:
            if future.done():
                self.prior_completed.add(future)
            else:
                with future._condition:
                    future._waiters.append(self._waiter)
                    self._waiter.n_pending += 1

    def as_completed(self) -> Generator[Any, None, None]:
        def process_future(future: Future):
            with future._condition:
                future._waiters.remove(self._waiter)
                return future.result()

        while not self.done():
            while len(self.prior_completed) > 0:
                yield self.prior_completed.pop().result()
            if not self.done():
                self._waiter.event.wait(timeout=5)
                finished_futures = self._waiter.collect_finished()
                while len(finished_futures) > 0:
                    yield process_future(finished_futures.pop())

        stragglers = self._waiter.collect_finished()
        for future in stragglers:
            yield process_future(future)

    def done(self) -> bool:
        if self.completion_lock.locked():
            return False
        return self._waiter.n_pending == 0 and len(self.prior_completed) == 0

    def n_uncollected(self) -> int:
        return (
            self._waiter.n_pending
            + len(self.prior_completed)
            + len(self._waiter.finished_futures)
        )


def test_waiter():
    with ThreadPoolExecutor() as e:
        futures = [e.submit(lambda x: x, i) for i in range(10)]
        waiter = Waiter(futures)
        for i in range(10, 20):
            waiter.add(e.submit(lambda x: x, i))
        assert set(waiter.as_completed()) == set(range(20))

    with ThreadPoolExecutor() as e:
        futures = [e.submit(lambda x: x, i) for i in range(33)]
        waiter = Waiter(futures)
        for i in range(33, 66):
            waiter.add(e.submit(lambda x: x, i))
        assert set(waiter.as_completed()) == set(range(66))


class PoolExecutor(Enum):
    Process = "Process"
    Thread = "Thread"


T = TypeVar("T")


def _iter_spanner(serialized_iterator_function: bytes, queue: multiprocessing.Queue):
    iterator_function = dill.loads(serialized_iterator_function)
    for value in iterator_function():
        queue.put(value)


def _loader(
    executor: ThreadPoolExecutor,
    queue: multiprocessing.Queue,
    iterator_functions: Iterable[Iterable[T]],
    chunk_size: int = -1,
):
    executor_kwargs = {}
    if chunk_size != -1:
        executor_kwargs["max_workers"] = chunk_size

    active_futures = set()
    for iterator_function in iterator_functions:
        serialized_iterator = dill.dumps(iterator_function)
        future = executor.submit(_iter_spanner, serialized_iterator, queue)
        if chunk_size != -1:
            active_futures.add(future)
            while len(active_futures) >= chunk_size:
                completed_futures, active_futures = wait(
                    active_futures,
                    return_when=FIRST_COMPLETED,
                    timeout=(0.1 * len(active_futures)) + 10,
                )
                for future in completed_futures:
                    future.result()
    while len(active_futures) > 0:
        completed_futures, active_futures = wait(
            active_futures,
            return_when=ALL_COMPLETED,
            timeout=(0.1 * len(active_futures)) + 45,
        )
        for future in completed_futures:
            future.result()


T = TypeVar("T")


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


def queue_to_iterator(q: SimpleQueue, timeout: float = 10):
    try:
        while True:
            yield q.get(timeout=timeout)
    except Empty:
        pass


FnInputType = TypeVar("FnInputType")
FnOutputType = TypeVar("FnOutputType")


def unordered_incremental_executor_map(
    executor: ThreadPoolExecutor,
    function: Callable[[FnInputType], FnOutputType],
    function_inputs: Iterable[FnInputType],
    max_parallel: int = os.cpu_count() or 4,
    max_backlog: int = -1,
) -> Generator[FnOutputType, None, None]:
    """
    Works like executor.map, but sacrifices efficient, grouped thread assignment for eagerness.
    Runs max_parallel jobs or fewer simultaneously. Input order is NOT preserved.

    :param max_parallel: Number of parallel jobs to run. Should be greater than zero.
    :return: A generator of the unordered results of the mapping.
    """
    if max_parallel < 1:
        raise ValueError(
            f"max_parallel is not greater than or equal to one: {max_parallel}"
        )

    exhausted = Exhausted()
    waiter = Waiter()

    def load(
        executor: Executor,
        waiter: Waiter,
        function: Callable[[FnInputType], FnOutputType],
        function_inputs: Iterable[FnInputType],
        max_parallel: int,
        max_backlog: int,
        exhausted: Exhausted,
        acquired_lock: threading.Lock,
    ):
        load_complete = False
        while not load_complete:
            while waiter._waiter.n_pending < max_parallel and (
                (max_backlog == -1) or (waiter.n_uncollected() < max_backlog)
            ):
                next_input = next(function_inputs, exhausted)
                if next_input is not exhausted:
                    new_future = executor.submit(function, next_input)
                    waiter.add(new_future)
                else:
                    load_complete = True
                    break
            if not load_complete:
                waiter._waiter.event.wait(timeout=30)
        acquired_lock.release()

    waiter.completion_lock.acquire()
    loader = executor.submit(
        lambda t: load(*t),
        (
            executor,
            waiter,
            function,
            iter(function_inputs),
            max_parallel,
            max_backlog,
            exhausted,
            waiter.completion_lock,
        ),
    )
    for result in waiter.as_completed():
        yield result

    assert loader.done()


def test_unordered_incremental_executor_map():
    with ThreadPoolExecutor() as ex:
        out = unordered_incremental_executor_map(ex, lambda x: x, range(5))
        assert set(out) == set(range(5))


def timestr() -> str:
    return datetime.datetime.now().isoformat()
