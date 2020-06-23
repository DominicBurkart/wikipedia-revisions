from typing import Tuple, Iterable, Callable, Generator, TypeVar, Any
import threading
import queue
import multiprocessing
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, Future
from enum import Enum
import datetime
from functools import partial
from collections import deque
import os

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


def merge_iterators(
    iterator_functions: Iterable[Callable[..., Iterable[T]]],
    chunk_size: int = -1,
    buffer: int = 10,
    parallelization_strategy: PoolExecutor = PoolExecutor.Process,
) -> Generator[T, None, None]:
    """
    Combines the output of multiple iterables into a single iterator. Since regular a iterator cannot
    be polled from multiple threads concurrently, the number of concurrent tasks submitted by this function is at most
    the number of iterators. Note: a separate thread is generated to poll the iterator functions and call them to
    begin populating the output, regardless of the parallelization strategy. Results are returned eagerly. Order is
    not guaranteed.

    :param executor: executor used to increment the generators.
    :param iterator_functions: zero-arity functions that return iterators to combine results from.
    :param chunk_size: number of generators to poll from at once. If greater than the number of generators, ignored.
    if -1, max concurrency is set by the underlying PoolExecutor (ProcessPoolExecutor or ThreadPoolExecutor from
    concurrent.futures).
    :param buffer: max number of backlogged values.
    :param parallelization_strategy: kind of PoolExecutor to use.
    :return: a generator over the combined outputs of all input generators.
    """
    if chunk_size < 1 and chunk_size != -1:
        raise ValueError("chunk_size must be greater than zero or equal to negative 1.")
    if buffer < 1:
        raise ValueError("buffer must be greater than 0.")

    def _loader(
        queue: multiprocessing.Queue,
        iterator_functions: Iterable[Iterable[T]],
        chunk_size: int = -1,
        executor_type: PoolExecutor = PoolExecutor.Process,
    ):
        Executor = (
            ProcessPoolExecutor
            if executor_type == PoolExecutor.Process
            else ThreadPoolExecutor
        )
        executor_kwargs = {}
        if chunk_size != -1:
            executor_kwargs["max_workers"] = chunk_size

        with Executor(**executor_kwargs) as executor:
            active_futures = deque()
            for iterator_function in iterator_functions:
                serialized_iterator = dill.dumps(iterator_function)
                future = executor.submit(_iter_spanner, serialized_iterator, queue)
                if chunk_size != -1:
                    active_futures.append(future)
                    while len(active_futures) >= chunk_size:
                        active_futures.popleft().result()
            if len(active_futures) > 0:
                active_futures.popleft().result()

    results_queue = multiprocessing.Manager().Queue(maxsize=buffer)
    with ThreadPoolExecutor(max_workers=1) as executor:
        loader_future = executor.submit(
            _loader,
            results_queue,
            iterator_functions,
            chunk_size,
            parallelization_strategy,
        )
        future_done = False
        is_empty = False
        while not (future_done and is_empty):
            try:
                yield results_queue.get(timeout=0.1)
            except queue.Empty:
                if future_done and results_queue.empty():
                    is_empty = True
                else:
                    future_done = loader_future.done()

        loader_future.result()  # rethrow any error that killed the loader


def test_merge_iterators():
    def gen1():
        for x in range(10):
            yield x

    def gen2():
        for y in range(10, 20):
            yield y

    assert set(merge_iterators((gen1, gen2), 1, 1, PoolExecutor.Process)) == set(
        range(20)
    )
    assert len(list(merge_iterators((gen1, gen2), 1, 1, PoolExecutor.Process))) == 20

    assert set(merge_iterators([gen1, gen2], 1, 1, PoolExecutor.Thread)) == set(
        range(20)
    )
    assert len(list(merge_iterators([gen1, gen2], 1, 1, PoolExecutor.Thread))) == 20


class LazyList:
    """
    memorizes the results of an iterable, allowing for lazy list construction. Not thread-safe. Behavior on this is
    not well protected.

    Be careful about mutating the contents of the list.
    """

    def __init__(self, iterable: Iterable):
        self.all = []
        self.iterator = iter(iterable)
        self.appended = []

    def __iter__(self) -> Generator:
        for value in self.all:
            yield value
        for value in self.iterator:
            self.all.append(value)
            yield value
        for value in self.appended:
            self.all.append(value)
            yield value
        self.appended.clear()

    def __getitem__(self, item):
        if isinstance(item, int):
            if len(self.all) > item:
                return self.all[item]
            i = len(self.all)
            for value in self.iterator:
                self.all.append(value)
                if i == item:
                    return value
                i += 1
            for value in self.appended:
                self.all.append(value)
                if i == item:
                    return value
            self.appended.clear()
            raise IndexError
        elif isinstance(item, slice):
            if item.start < 0:
                raise IndexError
            if item.stop < item.start:
                raise IndexError
            if len(self.all) < item.stop:
                self[item.stop - 1]  # memorize necessary values
            return self.all[item]
        raise NotImplementedError

    def append(self, value) -> None:
        self.appended.append(value)


def _test_fn_iden(x):
    return x


def _test_fn_append(appendable, v):
    appendable.append(v)


def test_lazy_list():
    li = LazyList(iter([1, 2, 3]))
    assert list(li) == [1, 2, 3]
    assert list(li) == [1, 2, 3]

    l2 = LazyList(range(1, 4))
    for v in range(4, 7):
        l2.append(v)
    assert list(l2) == list(range(1, 7))
    assert list(l2) == list(range(1, 7))

    l3 = LazyList(range(10))
    l4 = LazyList(l3)
    assert list(l3) == list(l4)
    assert list(l3) == list(l4)
    l4.append(10)
    assert list(l3) + [10] == list(l4)

    l5 = LazyList(range(10))
    for _ in l5:
        assert list(l5) == list(range(10))

    l6 = LazyList(range(2))
    it1 = iter(l6)
    it2 = iter(l6)
    next(it2)
    next(it2)  # it2 is now exhausted, but hasn't raised StopIteration yet.
    l6.append("nice")
    assert next(it1) == 0
    assert next(it1) == 1
    assert next(it1) == "nice"
    assert next(it2) == "nice"
    l6.append("final")
    assert next(it1) == "final"
    assert next(it2) == "final"


def test_lazy_list_slicing():
    x = LazyList(range(10))
    assert x[4:6] == [4, 5]


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


def lazy_dezip(it: Iterable[Tuple]) -> Iterable[Iterable]:
    """
    assumes that each tuple in the iterable has the same length. Stores the whole input in memory until the last
    iterator is released.
    """
    try:
        container = LazyList(it)
        first = next(iter(container))
        n_zipped = len(first)
        return map(lambda i: (row[i] for row in container), range(n_zipped))
    except StopIteration:
        raise RuntimeError("lazy_dezip received an empty iterator")


def test_lazy_dezip():
    abc, cde, efg, hij = lazy_dezip(zip("abc", "cde", "efg", "hij"))
    assert "".join(abc) == "abc"
    assert "".join(cde) == "cde"
    assert "".join(efg) == "efg"
    assert "".join(hij) == "hij"


def timestr() -> str:
    return datetime.datetime.now().isoformat()
