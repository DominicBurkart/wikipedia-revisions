import bz2
import csv
import datetime
import os
import sys
import re
import xml.etree.ElementTree as ET
import time
import errno
import traceback
from concurrent.futures import ThreadPoolExecutor, Executor, Future, TimeoutError
from functools import partial
from typing import Optional, Callable, Dict, Generator, Iterable, Tuple, TypeVar, Any
import hashlib
import threading
import platform

import requests
import click

config = dict()


def timestr() -> str:
    return datetime.datetime.now().isoformat()


def download_update_file(session: requests.Session, url: str) -> str:
    CHUNK_SIZE = 1024 * 1024 * 5
    filename = url.split("/")[-1]
    retries = 0
    while True:
        try:
            if os.path.exists(filename):
                print(f"{timestr()} using local file {filename} 👩‍🌾")
                break
            print(f"{timestr()} downloading {url}. saving to {filename}. 📁")
            resp = session.get(url, stream=True, timeout=60)
            assert resp.status_code == 200
            print(f"{timestr()} response for {url}: {resp.status_code}. 🕺")
            with open(filename, "wb") as file:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    file.write(chunk)
            break
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            TimeoutError,
        ):
            retries += 1
            print(
                f"{timestr()} timeout for {url}: sleeping 60 seconds and restarting download... (retry #{retries}) ↩️"
            )
            time.sleep(60)
    return filename


class MalformattedInput(Exception):
    ...


def generate_revisions(file) -> Generator[Dict, None, None]:
    def prefixed(s: str) -> str:
        """element names have the following string prepended to them."""
        return "{http://www.mediawiki.org/xml/export-0.10/}" + s

    ID_STR = prefixed("id")
    NS_STR = prefixed("ns")
    TITLE_STR = prefixed("title")
    PAGE_STR = prefixed("page")
    REVISION_STR = prefixed("revision")
    PARENT_ID_STR = prefixed("parent_id")
    TIMESTAMP_STR = prefixed("timestamp")
    CONTRIBUTOR_STR = prefixed("contributor")
    IP_STR = prefixed("ip")
    USERNAME_STR = prefixed("username")
    COMMENT_STR = prefixed("comment")
    TEXT_STR = prefixed("text")

    page_id = None
    page_ns = None
    page_title = None
    for event, element in ET.iterparse(file, events=["start", "end"]):
        if event == "end" and element.tag == PAGE_STR:
            page_id = None
            page_ns = None
            page_title = None
            element.clear()
        elif event == "end":
            # hack: assume that the id, ns, and title for a revision all precede the first revision.
            # if this is not the case, a MalformattedInput exception is thrown.
            if page_id is None and element.tag == ID_STR:
                page_id = element.text
            elif page_ns is None and element.tag == NS_STR:
                page_ns = element.text
            elif page_title is None and element.tag == TITLE_STR:
                page_title = element.text
            elif element.tag == REVISION_STR:
                revision_id = element.find(ID_STR).text
                parent_id_element = element.find(PARENT_ID_STR)
                parent_id = (
                    parent_id_element.text if parent_id_element is not None else None
                )
                timestamp = element.find(TIMESTAMP_STR).text
                contributor_element = element.find(CONTRIBUTOR_STR)
                ip_element = contributor_element.find(IP_STR)
                contributor_ip = ip_element.text if ip_element is not None else None
                contributor_id_element = contributor_element.find(ID_STR)
                contributor_id = (
                    contributor_id_element.text
                    if contributor_id_element is not None
                    else None
                )
                contributor_name_element = contributor_element.find(USERNAME_STR)
                contributor_name = (
                    contributor_name_element.text
                    if contributor_name_element is not None
                    else None
                )
                comment_element = element.find(COMMENT_STR)
                comment = comment_element.text if comment_element is not None else None
                text = element.find(TEXT_STR).text
                if any(v is None for v in (page_id, page_ns, page_title)):
                    raise MalformattedInput
                yield {
                    "id": revision_id,
                    "parent_id": parent_id,
                    "timestamp": timestamp,
                    "page_id": page_id,
                    "page_title": page_title,
                    "page_ns": page_ns,
                    "contributor_id": contributor_id,
                    "contributor_name": contributor_name,
                    "contributor_ip": contributor_ip,
                    "comment": comment,
                    "text": text,
                }
                element.clear()


def extract_one_file(filename: str) -> Generator[Dict, None, None]:
    print(f"{timestr()} extracting revisions from update file {filename}... 🧛")
    with bz2.open(filename, "rt", newline="") as uncompressed:
        for revision in generate_revisions(uncompressed):
            yield revision
    print(f"{timestr()} exhausted file: {filename} 😴")
    if config["low_storage"]:
        print(f"{timestr()} Deleting {filename}... ✅")
        os.remove(filename)


def make_extractors(
    filenames_and_urls, append_bad_urls
) -> Generator[Generator[Dict, None, None], None, None]:
    for filename, url in filenames_and_urls:
        if filename:
            yield extract_one_file(filename)
        else:
            append_bad_urls.append(url)
            # ^ if there is no filename, add url to the retry pile


def parse_downloads(
    download_file_and_url: Iterable[Tuple[str, str]],
    append_bad_urls,
    executor: Executor,
) -> Generator[Dict, None, None]:
    verified_files = VerifiedFilesRecord()

    # perform checksum
    if config["low_storage"]:
        print(
            f"{timestr()} [low storage mode] "
            f"deleting all records of previously verified files. 🔥"
        )
        verified_files.remove_local_file_verification()
        filenames_and_urls = peek_ahead(
            executor,
            map(
                lambda tup: (check_hash(verified_files, tup[0]), tup[1]),
                download_file_and_url,
            ),
        )
    else:
        filenames_and_urls = incremental_executor_map(
            executor,
            lambda tup: (check_hash(verified_files, tup[0]), tup[1]),
            download_file_and_url,
            max_parallel=config["max_workers"],
        )

    # extract files with valid checksums
    file_extractors = make_extractors(filenames_and_urls, append_bad_urls)
    chunk_size = config["concurrent_reads"]
    for case in merge_generators(executor, file_extractors, chunk_size=chunk_size):
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
            resp = requests.get(config["md5_hashes_url"])
            if resp.status_code != 200:
                print(
                    f"{timestr()} unable to get md5 hashes from wikipedia. "
                    "Sleeping for five minutes then retrying... 🛌"
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
                map(lambda s: s.strip(), open(self.record_in_storage).readlines())
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

    def remove_local_file_verification(self):
        with self.lock:
            with open(self.record_in_storage, "w") as store:
                store.write("")
            self.files.clear()


def get_hash(filename: str) -> str:
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(1000000)
            if not chunk:
                break
            hash.update(chunk)
    return hash.hexdigest()


def check_hash(verified_files: VerifiedFilesRecord, filename: str) -> Optional[Dict]:
    if filename not in verified_files:
        print(f"{timestr()} checking hash for {filename}... 📋")
        file_hash = get_hash(filename)
        if file_hash == verified_files.canonical_hash(filename):
            if not config["low_storage"]:
                # ^ hack in low_storage mode the files are deleted when exhausted
                verified_files.add(filename)
        else:
            print(f"{timestr()} hash mismatch with {filename}. Deleting file.🗑️ ")
            os.remove(filename)
            return None

    print(f"{timestr()} {filename} hash verified 💁")
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

    def __init__(self, futures: Iterable[Future] = []):
        self._waiter = _Waiter()
        self.prior_completed = set()
        self.completion_lock = threading.Lock()
        # ^ when acquired, prevents as_completed from stopping even if there are no running tasks.
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
        def process_future(future: Future):
            with future._condition:
                future._waiters.remove(self._waiter)
                return future.result()

        while not self.done():
            while len(self.prior_completed) > 0:
                yield self.prior_completed.pop().result()
            self._waiter.event.wait(20 * 60)
            finished = self._waiter.collect_finished()
            while len(finished) > 0:
                yield process_future(finished.pop())

        stragglers = self._waiter.collect_finished()
        while len(stragglers) > 0:
            yield process_future(stragglers.pop())

    def done(self) -> bool:
        if self.completion_lock.locked():
            return False
        with self._waiter.lock:
            return self._waiter.n_pending == 0 and len(self.prior_completed) == 0


def test_waiter():
    with ThreadPoolExecutor() as e:
        futures = [e.submit(lambda x: x, i) for i in range(10)]
        waiter = Waiter(futures)
        for i in range(10, 20):
            waiter.add(e.submit(lambda x: x, i))
        assert set(waiter.as_completed()) == set(range(20))


T = TypeVar("T")


def merge_generators(
    executor: Executor,
    generators: Iterable[Generator[T, None, None]],
    chunk_size: int = -1,
) -> Generator[T, None, None]:
    """
    Combines the output of multiple generators into a single generator. Since regular generators cannot
    be polled concurrently, the number of concurrent tasks submitted by this function is at most the number
    of generators.
    :param executor: executor used to increment the generators.
    :param generators: generators to combine results from.
    :param chunk_size: number of generators to poll from at once. If greater than the number of generators, ignored.
    if -1, ignored. If zero or a negative number other than -1, raises ValueError.
    :return: a generator over the combined outputs of all input generators.
    """
    if chunk_size < 1 and chunk_size != -1:
        raise ValueError("chunk_size must be greater than zero or equal to negative 1.")

    state = {"n_generators": 0, "n_exhausted": 0, "exhaustion_event": threading.Event()}

    def async_load_generators(
        waiter: Waiter,
        generators: Iterable[Generator[Any, None, None]],
        acquired_lock: threading.Lock,
    ) -> None:
        for generator in generators:
            future = executor.submit(
                lambda generator: (next(generator, exhausted), generator), generator
            )
            waiter.add(future)
            state["n_generators"] += 1
            if chunk_size != -1 and (
                chunk_size <= (state["n_generators"] - state["n_exhausted"])
            ):
                state["exhaustion_event"].wait()
                state["exhaustion_event"].clear()
        acquired_lock.release()

    exhausted = Exhausted()
    waiter = Waiter()

    # asynchronously load generators
    waiter.completion_lock.acquire()
    async_load_future = executor.submit(
        lambda tup: async_load_generators(*tup),
        (waiter, iter(generators), waiter.completion_lock),
    )

    # yield values as they are completed & request next generator value
    for (value, generator) in waiter.as_completed():
        if value is not exhausted:
            yield value
            waiter.add(
                executor.submit(lambda gen: (next(gen, exhausted), gen), generator)
            )
        else:
            state["n_exhausted"] += 1
            state["exhaustion_event"].set()
            # ^ tell the loader it can add more generators if any are available.

    assert async_load_future.done()


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
    asynchronously computes the next value of an iterable

    :param executor:
    :param iterable:
    :return:
    """
    is_exhausted = False
    exhausted = Exhausted()
    next_future = executor.submit(partial(next, iterable, exhausted))
    while not is_exhausted:
        next_value = next_future.result()
        if next_value is exhausted:
            is_exhausted = True
        else:
            next_future = executor.submit(partial(next, iterable, exhausted))
            yield next_value
    del next_future


FnInputType = TypeVar("FnInputType")
FnOutputType = TypeVar("FnOutputType")


def incremental_executor_map(
    executor: Executor,
    function: Callable[[FnInputType], FnOutputType],
    function_inputs: Iterable[FnInputType],
    max_parallel=os.cpu_count() or 4,
) -> Generator[FnOutputType, None, None]:
    """
    Works like executor.map, but sacrifices efficient, grouped thread assignment for eagerness.
    Runs max_parallel jobs or fewer simultaneously. Input order is NOT preserved.

    :param max_parallel: Number of parallel jobs to run. Should be greater than zero.
    :return: A generator of the unordered results of the mapping.
    """

    def load(
        executor: Executor,
        waiter: Waiter,
        function: Callable[[FnInputType], FnOutputType],
        function_inputs: Iterable[FnInputType],
        max_parallel: int,
        exhausted: Exhausted,
        acquired_lock: threading.Lock,
    ):
        load_complete = False
        while not load_complete:
            while waiter._waiter.n_pending < max_parallel:
                next_input = next(function_inputs, exhausted)
                if next_input is not exhausted:
                    new_future = executor.submit(function, next_input)
                    waiter.add(new_future)
                else:
                    load_complete = True
                    break
            if not load_complete:
                waiter._waiter.event.wait(20 * 60)
        acquired_lock.release()

    if max_parallel < 1:
        raise ValueError(
            f"max_parallel is not greater than or equal to one: {max_parallel}"
        )

    exhausted = Exhausted()
    waiter = Waiter()

    waiter.completion_lock.acquire()
    loader = executor.submit(
        lambda t: load(*t),
        (
            executor,
            waiter,
            function,
            iter(function_inputs),
            max_parallel,
            exhausted,
            waiter.completion_lock,
        ),
    )
    for result in waiter.as_completed():
        yield result

    assert loader.done()


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


def full_dump_url_from_partial(partial: str):
    if config["date"] != "latest" and partial.startswith("/"):
        return "https://dumps.wikimedia.org" + partial
    elif config["date"] == "latest" and not partial.startswith("/"):
        return "https://dumps.wikimedia.org/enwiki/latest/" + partial
    else:
        raise ValueError("dump page format has been updated.")


def download_and_parse_files(executor: Executor,) -> Generator[Dict, None, None]:
    # todo automatically find the last completed bz2 history job
    print(f"{timestr()} requesting dump directory... 📚")
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,"
            "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        }
    )
    dump_page = session.get(config["dump_page_url"])

    assert dump_page.status_code == 200
    print(f"{timestr()} parsing dump directory...  🗺️🗺️")

    # read history file links in dump summary
    updates_urls = LazyList(
        map(
            full_dump_url_from_partial,
            filter(
                lambda url: "pages-meta-history" in url and url.endswith(".bz2"),
                re.findall('href="(.+?)"', dump_page.text),
            ),
        )
    )

    # download & process the history files
    download_update_file_using_session = partial(download_update_file, session)
    if config["low_storage"]:
        print(f"{timestr()} low storage mode active. 🐈📦")
        file_and_url = peek_ahead(
            executor,
            map(
                lambda update_url: (
                    download_update_file_using_session(update_url),
                    update_url,
                ),
                updates_urls,
            ),
        )
    else:
        file_and_url = incremental_executor_map(
            executor,
            lambda update_url: (
                download_update_file_using_session(update_url),
                update_url,
            ),
            updates_urls,
            max_parallel=2,
        )

    for revision in parse_downloads(
        file_and_url, append_bad_urls=updates_urls, executor=executor
    ):
        yield revision


def write_to_csv(revisions: Iterable[Dict]) -> None:
    with bz2.open("revisions.csv.bz2", "wt", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            [
                "id",
                "parent_id",
                "page_title",
                "contributor_id",
                "contributor_name",
                "contributor_ip",
                "timestamp",
                "text",
                "comment",
                "page_id",
                "page_ns",
            ],
        )
        writer.writeheader()
        i = 0
        for case in revisions:
            writer.writerow(case)
            i += 1
            if i % 1000000 == 0 or i == 1:
                print(f"{timestr()} wrote revision #{i}")


class DatabaseAlreadyExists(Exception):
    pass


def write_to_database(executor: Executor, revisions: Iterable[Dict]) -> None:
    from dateutil.parser import parse as parse_timestamp
    from sqlalchemy import create_engine, Column, Integer, Text, DateTime
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy_utils import database_exists, create_database, drop_database
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Revision(Base):
        __tablename__ = "revisions"
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer)
        timestamp = Column(DateTime, nullable=False)
        text = Column(Text)
        comment = Column(Text)
        page_id = Column(Integer, nullable=False)
        page_title = Column(Text, nullable=False)
        page_ns = Column(Integer, nullable=False)
        contributor_id = Column(Integer)
        contributor_name = Column(Text)
        contributor_ip = Column(Text)

    def retype_revision(revision: Dict) -> Dict:
        parent_id_str = revision["parent_id"]
        contributor_id_str = revision["contributor_id"]
        return {
            **revision,
            "id": int(revision["id"]),
            "parent_id": int(parent_id_str) if parent_id_str is not None else None,
            "timestamp": parse_timestamp(revision["timestamp"]),
            "contributor_id": int(contributor_id_str) if contributor_id_str else None,
        }

    def retype_and_size_revision(raw_revision: Dict) -> Tuple[Dict, int]:
        """
        This function calls the revision retype function and estimates the size of
        a revision in memory based on the size of its text and comment.
        """
        revision = retype_revision(raw_revision)
        size = (
            len((revision.get("text") or "").encode("utf-8"))
            + len((revision.get("comment") or "").encode("utf-8"))
            + 300  # estimate for the remaining small fields
        )
        # ^ sys.getsizeof doesn't work in pypy
        return revision, size

    def commit_batch(batch):
        session = Session()
        session.bulk_insert_mappings(Revision, batch)
        session.commit()
        session.close()

    retyped_revisions_and_sizes = incremental_executor_map(
        executor,
        lambda revision: retype_and_size_revision(revision),
        revisions,
        max_parallel=config["max_workers"] * (3 / 4),
    )

    print(f"{timestr()} structuring database... 📐")
    engine = create_engine(config["database_url"])
    if database_exists(engine.url):
        if config["delete_database"]:
            drop_database(engine.url)
        else:
            raise DatabaseAlreadyExists

    try:
        create_database(engine.url)
        assert database_exists(engine.url)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        print(f"{timestr()} adding revisions to session... 📖")
        i = 0
        batch_size = 0
        max_batch_size = (
            1024 * 1024 * 100 if config["low_memory"] else 1024 * 1024 * 1024 * 2
        )
        # ^ 100 MB if low memory else 2 GB
        last_commit = None
        batch = []
        for revision, revision_size in retyped_revisions_and_sizes:
            batch.append(revision)
            batch_size += revision_size
            if batch_size >= max_batch_size:
                # max two batch writes open at a time
                next_commit = executor.submit(commit_batch, batch)
                if last_commit is not None:
                    last_commit.result()
                batch = []
                batch_size = 0
                last_commit = next_commit
            i += 1
            if i % 1000000 == 0 or i == 1:
                print(f"{timestr()} processed revision #{i}")
        print(f"{timestr()} committing session with {i} revisions... 🤝")
        if last_commit is not None:
            last_commit.result()
        print(
            f"{timestr()} revisions written to database at: {config['database_url']} 🌈"
        )
    except Exception as e:
        print(
            f"{timestr()} exception while writing. deleting partial database & re-raising exception. 🌋"
        )
        drop_database(engine.url)
        raise e


@click.command()
@click.option(
    "--date",
    "date",
    default="latest",
    help="Wikipedia dump page in YYYYMMDD format (like 20200101). "
    "Find valid dates by checking which entries on "
    "https://dumps.wikimedia.org/enwiki/ have .bz2 files that "
    'contain the include "pages-meta-history" in the name and '
    "have been successfully written.",
)
@click.option(
    "--low-storage/--large-storage",
    "low_storage",
    default=True,
    help="Cut performance to decrease storage requirements. Deletes "
    "files when they are exhausted and keeps a limited number of "
    ".xml.bz2 files on disk at any time. If --large-storage, eagerly "
    "downloads all xml.bz2 and never deletes intermediary xml.bz2 "
    "files.",
)
@click.option(
    "--database/--csv",
    "use_database",
    default=False,
    help="Write output into a database instead of a CSV. "
    "Requires additional installations (run pip install -r "
    "database_requirements.txt) and for the database URL (see "
    "--database-url) to be available.",
)
@click.option(
    "--database-url",
    default="postgres:///wikipedia_revisions"
    if platform.python_implementation() == "CPython"
    else "postgresql+psycopg2cffi:///wikipedia_revisions",
    help="Database URL to use. Defines database dialect used (any "
    "database dialect supported by SQLAlchemy should work). Ignored"
    "if --database is not set. Default is postgres:///wikipedia_revisions on CPython, and "
    "postgresql+psycopg2cffi:///wikipedia_revisions on all other implementations (e.g. PyPy).",
)
@click.option(
    "--low-memory/--large-memory",
    "low_memory",
    default=True,
    help="Optimize for low-memory systems. Limits the number of "
    "dump files concurrently processed to 3, instead of "
    "the number of CPU cores. If writing to a database, "
    "also commits every megabyte instead of gigabyte to limit "
    "memory usage.",
)
@click.option(
    "--delete-database/--do-not-delete-database",
    "delete_database",
    default=False,
    help="drop everything in the passed database and overwrite it with "
    "the wikipedia revisions data.",
)
@click.option(
    "--concurrent-reads",
    "concurrent_reads",
    default=2,
    type=int,
    help="number of concurrently processed .xml.bz2 files. Default is 2. When using storage media "
         "with fast concurrent reads and high throughput (SSDs), higher values are better."
)
def run(date, low_storage, use_database, database_url, low_memory, delete_database, concurrent_reads):
    config["date"] = date
    config["dump_page_url"] = f"https://dumps.wikimedia.org/enwiki/{date}/"
    config[
        "md5_hashes_url"
    ] = f"https://dumps.wikimedia.org/enwiki/{date}/enwiki-{date}-md5sums.txt"
    config["max_workers"] = (os.cpu_count() or 4) * 2
    config["low_storage"] = low_storage
    config["database_url"] = database_url
    config["low_memory"] = low_memory
    config["delete_database"] = delete_database
    config["concurrent_reads"] = concurrent_reads
    if concurrent_reads < 1:
        raise ValueError("concurrent_reads must be at least 1.")

    print(f"{timestr()} program started. 👋")

    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        complete = False
        while not complete:
            try:
                # download XML files from wikipedia and collect revisions
                revisions = download_and_parse_files(executor)

                # write collected revisions to output.
                if use_database:
                    write_to_database(executor, revisions)
                else:
                    write_to_csv(revisions)
                print(f"{timestr()} program complete. 💐")
                complete = True
            except Exception as e:
                if getattr(e, "errno", None) == errno.ENOSPC:
                    print(f"{timestr()} no space left on device. Ending program. 😲")
                    raise e
                elif isinstance(e, DatabaseAlreadyExists):
                    print(
                        f"{timestr()} there is already a local version of the database. Doing nothing. HELP: to "
                        f"overwrite database, use --delete-database flag. 🌅"
                    )
                    raise e
                SLEEP_SECONDS = 5 * 60
                print(traceback.format_exc())
                print(
                    f"{timestr()} caught exception ({e}). Sleeping {SLEEP_SECONDS/60} minutes..."
                )
                time.sleep(SLEEP_SECONDS)
                print(f"{timestr()} Restarting...")
            finally:
                for fname in ["verified_files.txt", "canonical_hashes.txt"]:
                    if os.path.exists(fname):
                        os.remove(fname)


if __name__ == "__main__":
    run()
