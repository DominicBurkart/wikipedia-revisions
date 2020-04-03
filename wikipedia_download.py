import bz2
import csv
import datetime
import os
import re
import xml.etree.ElementTree as ET
import time
import errno
import traceback
from concurrent.futures import ThreadPoolExecutor, Executor, Future
from functools import partial
from typing import (
    Optional,
    Callable,
    Dict,
    Generator,
    Iterable,
    Tuple,
    TypeVar,
    Any,
)
import hashlib
import threading

import requests

DUMP_DATE = "20200101"
DUMP_PAGE_URL = f"https://dumps.wikimedia.org/enwiki/{DUMP_DATE}/"
MD5_HASHES = f"https://dumps.wikimedia.org/enwiki/{DUMP_DATE}/enwiki-{DUMP_DATE}-md5sums.txt"
DELETE = False  # if true, deletes intermediary files
USE_LOCAL = True  # if true, get directory and prefer local files if they exist
MAX_WORKERS = (os.cpu_count() or 4) * 10  # number of concurrent threads


def strtime() -> str:
    return datetime.datetime.now().isoformat()


def download_update_file(session: requests.Session, url: str) -> str:
    def _download():
        resp = session.get(url, stream=True, timeout=60)
        assert resp.status_code == 200
        print(f"{strtime()} response for {url}: {resp.status_code}. 🕺")
        with open(filename, "wb") as file:
            i = 0
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                file.write(chunk)
                i += 1
                if i % 25000 == 0:
                    print(
                        f"{strtime()} {filename}: received chunk #{i} "
                        f"({round(i * CHUNK_SIZE / (1024 * 1024 * 1024), 3)} GB downloaded)... ⚡"
                    )

    CHUNK_SIZE = 2048
    filename = url.split("/")[-1]
    print(f"{strtime()} downloading {url}. saving to {filename}. 📁")
    retries = 0
    while True:
        try:
            if USE_LOCAL and os.path.exists(filename):
                print(f"{strtime()} using local file {filename} 👩‍🌾")
                break
            _download()
            break
        except requests.exceptions.Timeout:
            retries += 1
            print(
                f"{strtime()} timeout for {url}: restarting download... (retry #{retries}) ↩️"
            )
    return filename


def generate_revisions(file) -> Generator[Dict, None, None]:
    for _end, element in ET.iterparse(file):
        if element.tag.endswith("revision"):
            parent_id = None
            for revision_element in element.iter():
                if revision_element.tag.endswith("revision"):
                    continue  # iter() passes the element and its children
                elif revision_element.tag.endswith("timestamp"):
                    timestamp = revision_element.text
                elif revision_element.tag.endswith("text"):
                    text = revision_element.text
                elif revision_element.tag.endswith("parentid"):
                    parent_id = revision_element.text
                elif revision_element.tag.endswith("}id"):
                    revision_id = revision_element.text

            yield {
                "id": revision_id,
                "parent": parent_id,
                "timestamp": timestamp,
                "text": text,
            }
            element.clear()


def extract_one_file(filename: str) -> Generator[Dict, None, None]:
    print(f"{strtime()} extracting revisions from update file {filename}... 🧛")
    with bz2.open(filename, "rt", newline="") as uncompressed:
        for revision in generate_revisions(uncompressed):
            yield revision

    print(f"{strtime()} exhausted file: {filename} 😴")
    if DELETE:
        print(f"{strtime()} Deleting {filename}... ✅")
        os.remove(filename)


def parse_downloads(
    download_file_and_url: Iterable[Tuple[str, str]],
    append_bad_urls,
    executor: Executor,
) -> Generator[Dict, None, None]:
    # perform checksum
    filenames, urls = lazy_dezip(download_file_and_url)
    filenames_and_urls = zip(
        lazy_executor_map(
            executor,
            partial(check_hash, VerifiedFilesRecord()),
            filenames,
            max_parallel=MAX_WORKERS,
        ),
        urls,
    )

    # yield revisions for valid-checksum files
    files_to_process = []
    for filename, url in filenames_and_urls:
        if filename:
            files_to_process.append(filename)
            if len(files_to_process) == MAX_WORKERS:
                for case in merge_generators(
                    executor,
                    (
                        extract_one_file(filename)
                        for filename in files_to_process
                    ),
                ):
                    yield case
                files_to_process.clear()
        else:
            append_bad_urls.append(
                url
            )  # if checksum fails, add incomplete / misformatted file to the retry pile
    for case in merge_generators(
        executor, (extract_one_file(filename) for filename in files_to_process)
    ):
        yield case


class VerifiedFilesRecord:
    """
    retain the hash and basename for each downloaded file. downloads the
    canonical hashes from wikipedia if they are not stored locally.
    """

    def __init__(self):
        self.canonical_record = "canonical_hashes.txt"
        self.lock = threading.Lock()
        while not os.path.exists(self.canonical_record):
            resp = requests.get(MD5_HASHES)
            if resp.status_code != 200:
                print(
                    f"{strtime()} unable to get md5 hashes from wikipedia. "
                    "Sleeping for five minutes then retrying..."
                )
                time.sleep(5 * 60)
            else:
                with open(self.canonical_record, "w") as local_record:
                    local_record.write(resp.text)

        self.canonical_hashes = {
            line.split("  ")[1].strip(): line.split("  ")[0]
            for line in open(self.canonical_record).readlines()
        }

        self.record_in_storage = "verified_files_record.txt"
        if os.path.exists(self.record_in_storage):
            self.files = set(
                map(
                    lambda s: s.strip(),
                    open(self.record_in_storage).readlines(),
                )
            )
        else:
            open(self.record_in_storage, "a").close()
            self.files = set()

    def __contains__(self, filename):
        with self.lock:
            return filename in self.files

    def add(self, filename):
        with self.lock:
            base = os.path.basename(filename)
            with open(self.record_in_storage, "a") as store:
                store.write(base + "\n")
            self.files.add(base)

    def canonical_hash(self, filename) -> str:
        base = os.path.basename(filename)
        return self.canonical_hashes[base]


def get_hash(filename: str) -> str:
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(1000000)
            if not chunk:
                break
            hash.update(chunk)
    return hash.hexdigest()


def check_hash(
    verified_files: VerifiedFilesRecord, filename: str
) -> Optional[Dict]:
    if filename not in verified_files:
        print(f"{strtime()} checking hash for {filename}... 📋")
        hash = get_hash(filename)
        if hash != verified_files.canonical_hash(filename):
            print(f"{strtime()} Hash mismatch with {filename}. Deleting file.")
            os.remove(filename)
            return None
        elif not DELETE:
            verified_files.add(filename)

    return filename


class Exhausted:
    pass


class _Waiter:
    """
        based on _Waiter class in concurrent.futures._base
    """

    def __init__(self):
        self.event = threading.Event()
        self.finished_futures = []
        self.lock = threading.Lock()
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

    def __init__(self, futures: Iterable[Future]):
        self._waiter = _Waiter()
        self.prior_completed = set()
        for future in futures:
            self.add(future)

    def add(self, future: Future):
        with future._condition:
            if future.done():
                self.prior_completed.add(future)
            else:
                future._waiters.append(self._waiter)
                with self._waiter.lock:
                    self._waiter.n_pending += 1

    def as_completed(self) -> Generator[Any, None, None]:
        while not self.done():
            while len(self.prior_completed) > 0:
                yield self.prior_completed.pop().result()
            self._waiter.event.wait()
            finished = self._waiter.collect_finished()
            while len(finished) > 0:
                future = finished.pop()
                with future._condition:
                    future._waiters.remove(self._waiter)
                    result = future.result()
                del future
                yield result

    def done(self) -> bool:
        with self._waiter.lock:
            return (
                self._waiter.n_pending == 0 and len(self.prior_completed) == 0
            )


T = TypeVar("T")


def merge_generators(
    executor: Executor, generators: Iterable[Generator[T, None, None]]
) -> Generator[T, None, None]:
    """
    Combines the output of multiple generators into a single generator. Since regular generators cannot
    be polled concurrently, the number of concurrent tasks submitted by this function is at most the number
    of generators.

    :param executor: executor used to increment the generators.
    :param generators: generators to combine results from.
    :return: a generator over the combined outputs of all input generators.
    """
    exhausted = Exhausted()
    waiter = Waiter(
        executor.submit(
            lambda generator: (next(generator, exhausted), generator),
            generator,
        )
        for generator in generators
    )
    for (value, generator) in waiter.as_completed():
        if value is not exhausted:
            yield value
            waiter.add(
                executor.submit(
                    lambda generator: (next(generator, exhausted), generator),
                    generator,
                )
            )


def test_merge_generators():
    def gen1():
        for x in range(10):
            yield x

    def gen2():
        for y in range(10, 20):
            yield y

    with ThreadPoolExecutor() as e:
        assert set(merge_generators(e, (gen1(), gen2()))) == set(range(20))
        assert len(list(merge_generators(e, (gen1(), gen2())))) == 20


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


def _test_fn_append(l, v):
    l.append(v)


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


FnInputType = TypeVar("FnInputType")
FnOutputType = TypeVar("FnOutputType")


def lazy_executor_map(
    executor: Executor,
    function: Callable[[FnInputType], FnOutputType],
    function_inputs: Iterable[FnInputType],
    max_parallel=os.cpu_count() or 4,
) -> Generator[FnOutputType, None, None]:
    """
    Works like executor.map, but sacrifices efficient, grouped thread assignment for eagerness.
    Runs max_parallel jobs or fewer simultaneously. Input order is preserved.

    :param max_parallel: Number of parallel jobs to run. Should be greater than zero.
    :return: A generator of the ordered results of the mapping.
    """
    if max_parallel < 1:
        raise ValueError(
            f"max_parallel is not greater than or equal to one: {max_parallel}"
        )

    function_inputs_iter = iter(function_inputs)
    futures = []

    try:
        while True:
            while len(futures) < max_parallel:
                futures.append(
                    executor.submit(function, next(function_inputs_iter))
                )
            old_futures = futures
            futures = []
            for future_i in range(len(old_futures)):
                yield old_futures[future_i].result()
                num_current_tasks = (
                    len(old_futures) - (future_i + 1) + len(futures)
                )
                if num_current_tasks < max_parallel:
                    # start next task immediately, unless we're at max_parallel open jobs.
                    try:
                        futures.append(
                            executor.submit(
                                function, next(function_inputs_iter)
                            )
                        )
                    except StopIteration as stop:  # no new tasks! clean up old_futures and then re-raise StopIteration.
                        for remaining_i in range(
                            future_i + 1, len(old_futures)
                        ):
                            yield old_futures[remaining_i].result()
                        raise stop
    except StopIteration:
        pass
    for future in futures:
        yield future.result()


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


def download_and_parse_files(
    executor: Executor
) -> Generator[Dict, None, None]:
    # todo automatically find the last completed bz2 history job
    print(f"{strtime()} program started. 👋")
    print(f"{strtime()} requesting dump directory... 📚")
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,"
            "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        }
    )
    dump_page = session.get(DUMP_PAGE_URL)

    assert dump_page.status_code == 200
    print(f"{strtime()} parsing dump directory...  🗺️🗺️")

    # read history file links in dump summary
    updates_urls = LazyList(
        map(
            lambda partial_url: "https://dumps.wikimedia.org" + partial_url,
            filter(
                lambda url: "pages-meta-history" in url
                and url.endswith(".bz2"),
                re.findall('href="(.+?)"', dump_page.text),
            ),
        )
    )

    # download & process the history files
    file_and_url = zip(
        lazy_executor_map(
            executor,
            partial(download_update_file, session),
            updates_urls,
            max_parallel=2,
        ),
        updates_urls,
    )

    for revision in parse_downloads(
        file_and_url, append_bad_urls=updates_urls, executor=executor
    ):
        yield revision


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        complete = False
        while not complete:
            try:
                # download XML files from wikipedia and collect revisions
                revisions = download_and_parse_files(executor)

                # write collected revisions to output csv.
                with bz2.open(
                    "revisions.csv.bz2", "wt", newline=""
                ) as output_file:
                    writer = csv.DictWriter(
                        output_file, ["id", "parent", "timestamp", "text"]
                    )

                    i = 0
                    for case in revisions:
                        writer.writerow(case)
                        i += 1
                        if i % 1000000 == 0 or i == 1:
                            print(f"{strtime()} wrote revision #{i}")

                print(f"{strtime()} program complete. 💐")
                complete = True
            except Exception as e:
                if getattr(e, "errno", None) == errno.ENOSPC:
                    print(
                        f"{strtime()} no space left on device. Ending program. 😲"
                    )
                    raise e
                SLEEP_SECONDS = 5 * 60
                print(traceback.format_exc())
                print(
                    f"{strtime()} caught exception ({e}). Sleeping {SLEEP_SECONDS/60} minutes..."
                )
                time.sleep(SLEEP_SECONDS)
                print(f"{strtime()} Restarting...")
