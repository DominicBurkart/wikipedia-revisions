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
import click

config = dict()


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
            if os.path.exists(filename):
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
    def prefixed(s: str) -> str:
        """element names have the following string prepended to them."""
        return "{http://www.mediawiki.org/xml/export-0.10/}" + s

    ID_STR = prefixed("id")
    NS_STR = prefixed("ns")
    TITLE_STR = prefixed("title")
    REVISION_STR = prefixed("revision")
    PARENT_ID_STR = prefixed("parent_id")
    TIMESTAMP_STR = prefixed("timestamp")
    CONTRIBUTOR_STR = prefixed("contributor")
    IP_STR = prefixed("ip")
    USERNAME_STR = prefixed("username")
    COMMENT_STR = prefixed("comment")
    TEXT_STR = prefixed("text")

    for _end, element in ET.iterparse(file):
        if element.tag.endswith("page"):
            page_id = element.find(ID_STR).text
            page_ns = element.find(NS_STR).text
            page_title = element.find(TITLE_STR).text
            for revision_element in element.iterfind(REVISION_STR):
                revision_id = revision_element.find(ID_STR).text
                parent_id_element = revision_element.find(PARENT_ID_STR)
                parent_id = (
                    parent_id_element.text
                    if parent_id_element is not None
                    else None
                )
                timestamp = revision_element.find(TIMESTAMP_STR).text
                contributor_element = revision_element.find(CONTRIBUTOR_STR)
                ip_element = contributor_element.find(IP_STR)
                contributor_ip = (
                    ip_element.text if ip_element is not None else None
                )
                contributor_id_element = contributor_element.find(ID_STR)
                contributor_id = (
                    contributor_id_element.text
                    if contributor_id_element is not None
                    else None
                )
                contributor_name_element = contributor_element.find(
                    USERNAME_STR
                )
                contributor_name = (
                    contributor_name_element.text
                    if contributor_name_element is not None
                    else None
                )
                comment_element = revision_element.find(COMMENT_STR)
                comment = (
                    comment_element.text
                    if comment_element is not None
                    else None
                )
                text = revision_element.find(TEXT_STR).text
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
                revision_element.clear()
            element.clear()


def extract_one_file(filename: str) -> Generator[Dict, None, None]:
    print(f"{strtime()} extracting revisions from update file {filename}... 🧛")
    with bz2.open(filename, "rt", newline="") as uncompressed:
        for revision in generate_revisions(uncompressed):
            yield revision
    print(f"{strtime()} exhausted file: {filename} 😴")
    if config["delete"]:
        print(f"{strtime()} Deleting {filename}... ✅")
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
    # perform checksum
    verified_files = VerifiedFilesRecord()
    filenames_and_urls = incremental_executor_map(
        executor,
        lambda tup: (check_hash(verified_files, tup[0]), tup[1]),
        download_file_and_url,
        max_parallel=config["max_workers"],
    )

    # extract files with valid checksums
    file_extractors = make_extractors(filenames_and_urls, append_bad_urls)
    for file_extractor in file_extractors:
        for case in file_extractor:
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
        file_hash = get_hash(filename)
        if file_hash == verified_files.canonical_hash(filename):
            verified_files.add(filename)
        else:
            print(
                f"{strtime()} hash mismatch with {filename}. Deleting file.🗑️ "
            )
            os.remove(filename)
            return None

    print(f"{strtime()} {filename} hash verified 💁")
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
            return (
                self._waiter.n_pending == 0 and len(self.prior_completed) == 0
            )


def test_waiter():
    with ThreadPoolExecutor() as e:
        futures = [e.submit(lambda x: x, i) for i in range(10)]
        waiter = Waiter(futures)
        for i in range(10, 20):
            waiter.add(e.submit(lambda x: x, i))
        assert set(waiter.as_completed()) == set(range(20))


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


def download_and_parse_files(
    executor: Executor,
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
    dump_page = session.get(config["dump_page_url"])

    assert dump_page.status_code == 200
    print(f"{strtime()} parsing dump directory...  🗺️🗺️")

    # read history file links in dump summary
    updates_urls = LazyList(
        map(
            full_dump_url_from_partial,
            filter(
                lambda url: "pages-meta-history" in url
                and url.endswith(".bz2"),
                re.findall('href="(.+?)"', dump_page.text),
            ),
        )
    )

    # download & process the history files
    download_update_file_using_session = partial(download_update_file, session)
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
                print(f"{strtime()} wrote revision #{i}")


class DatabaseAlreadyExists(Exception):
    pass


def write_to_database(revisions: Iterable[Dict]) -> None:
    from dateutil.parser import parse as parse_timestamp
    from sqlalchemy import create_engine, Column, Integer, Text, DateTime
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy_utils import (
        database_exists,
        create_database,
        drop_database,
    )
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
            "parent_id": int(parent_id_str)
            if parent_id_str is not None
            else None,
            "timestamp": parse_timestamp(revision["timestamp"]),
            "contributor_id": int(contributor_id_str)
            if contributor_id_str
            else None,
        }

    print(f"{strtime()} structuring database... 📐")
    engine = create_engine(config["database_url"])
    if database_exists(engine.url):
        raise DatabaseAlreadyExists

    try:
        create_database(engine.url)
        assert database_exists(engine.url)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        print(f"{strtime()} adding revisions to session... 📖")
        i = 0
        for revision in revisions:
            session.add(Revision(**retype_revision(revision)))
            i += 1
            if i % 100 == 0:
                session.flush()
            if i % 1000000 == 0 or i == 1:
                print(f"{strtime()} wrote revision #{i}")
        print(f"{strtime()} committing session with {i} revisions... 🤝")
        session.commit()
        print(
            f"{strtime()} revisions written to database at: {config['database_url']} 🌈"
        )
    except Exception as e:
        print(
            f"{strtime()} exception while writing. deleting partial database & re-raising exception. 🌋"
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
    "--delete",
    "delete",
    default=False,
    help="Delete downloaded xml files as they are exhausted to save "
    "space. False by default to avoid having to "
    "re-downloading the same files if the program is "
    "interrupted.",
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
    default="postgres://postgres@localhost:5342/wikipedia-revisions",
    help="Database URL to use. Defines database dialect used (any "
    "database dialect supported by SQLAlchemy should work). Default is: "
    "postgres://postgres@localhost:5342/wikipedia-revisions",
)
def run(date, delete, use_database, database_url):
    config["date"] = date
    config["dump_page_url"] = f"https://dumps.wikimedia.org/enwiki/{date}/"
    config[
        "md5_hashes_url"
    ] = f"https://dumps.wikimedia.org/enwiki/{date}/enwiki-{date}-md5sums.txt"
    config["max_workers"] = (
        os.cpu_count() or 4
    ) * 2  # number of concurrent threads
    config["delete"] = delete
    config["database_url"] = database_url

    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        complete = False
        while not complete:
            try:
                # download XML files from wikipedia and collect revisions
                revisions = download_and_parse_files(executor)

                # write collected revisions to output.
                if use_database:
                    write_to_database(revisions)
                else:
                    write_to_csv(revisions)
                print(f"{strtime()} program complete. 💐")
                complete = True
            except Exception as e:
                if getattr(e, "errno", None) == errno.ENOSPC:
                    print(
                        f"{strtime()} no space left on device. Ending program. 😲"
                    )
                    raise e
                elif isinstance(e, DatabaseAlreadyExists):
                    print(
                        f"{strtime()} there is already a local version of the database. Doing nothing. 🌅"
                    )
                    raise e
                SLEEP_SECONDS = 5 * 60
                print(traceback.format_exc())
                print(
                    f"{strtime()} caught exception ({e}). Sleeping {SLEEP_SECONDS/60} minutes..."
                )
                time.sleep(SLEEP_SECONDS)
                print(f"{strtime()} Restarting...")


if __name__ == "__main__":
    run()
