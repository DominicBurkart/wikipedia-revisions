import bz2
import csv
import datetime
import difflib
import os
import re
import xml.etree.ElementTree as ET
import time
import errno
import traceback
from concurrent.futures import ThreadPoolExecutor, Executor, as_completed
from dataclasses import dataclass, FrozenInstanceError, asdict
from functools import partial
from itertools import chain
from typing import (
    Optional,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Iterator,
    Tuple,
    TypeVar,
    Any
)
import json
import tempfile
import hashlib
import threading
from random import choice

import nltk
import requests

DUMP_DATE = "20200101"
DUMP_PAGE_URL = f"https://dumps.wikimedia.org/enwiki/{DUMP_DATE}/"
MD5_HASHES = f"https://dumps.wikimedia.org/enwiki/{DUMP_DATE}/enwiki-{DUMP_DATE}-md5sums.txt"
DELETE = False  # if true, deletes intermediary files
USE_LOCAL = True  # if true, get directory and prefer local files if they exist
try:
    SENTENCE_SPLITTER = nltk.data.load("tokenizers/punkt/english.pickle")
except LookupError:
    nltk.download("punkt")
    SENTENCE_SPLITTER = nltk.data.load("tokenizers/punkt/english.pickle")


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


@dataclass(init=False, frozen=True)
class Revision:
    timestamp: str
    text: str
    parent: Optional[str]
    id: str

    def __init__(
        self, timestamp: str, text: str, parent: Optional[str], id: str
    ):
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "text", text or "")
        object.__setattr__(self, "parent", int(parent) if parent else None)
        object.__setattr__(self, "id", int(id))

    @classmethod
    def fields(cls) -> List[str]:
        return list(cls.__annotations__.keys())


def test_revision():
    r = Revision("2002-02-25 15:51:15+00:00", "this is a text", None, "10")

    # test __init__
    assert r.timestamp == "2002-02-25 15:51:15+00:00"
    assert r.text == "this is a text"
    assert r.parent is None
    assert r.id == 10

    # test frozen
    try:
        r.text = "fields should not be reassignable"
        raise AssertionError("Field reassign permitted.")
    except FrozenInstanceError:
        pass


def generate_revisions(file) -> Generator[Revision, None, None]:
    for _end, element in ET.iterparse(file):
        if element.tag.endswith("revision"):
            parent = None
            for revision_element in element.iter():
                if revision_element.tag.endswith("revision"):
                    continue  # iter() passes the element and its children
                elif revision_element.tag.endswith("timestamp"):
                    timestamp = revision_element.text
                elif revision_element.tag.endswith("text"):
                    text = revision_element.text
                elif revision_element.tag.endswith("parentid"):
                    parent = revision_element.text
                elif revision_element.tag.endswith("}id"):
                    id = revision_element.text

            yield Revision(timestamp, text, parent, id)
            element.clear()


def extract_one_file(filename: str):
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
):
    # perform checksum
    filenames, urls = lazy_dezip(download_file_and_url)
    filenames_and_urls = zip(
        lazy_executor_map(
            executor, check_hash, filenames, max_parallel=os.cpu_count() * 3
        ),
        urls,
    )

    # yield revisions for valid-checksum files
    chunk_size = int((os.cpu_count() or 4) * 3)
    files_to_process = []
    for filename, url in filenames_and_urls:
        if filename:
            files_to_process.append(filename)
            if len(files_to_process) == chunk_size:
                for case in merge_generators(
                    executor,
                    (
                        extract_one_file(filename)
                        for filename in files_to_process
                    ),
                    10,  # read some revisions sequentially to avoid needle moving cost
                ):
                    yield case
                files_to_process.clear()
        else:
            append_bad_urls.append(
                url
            )  # if checksum fails, add bad file to retry file
    for case in merge_generators(
        executor,
        (extract_one_file(filename) for filename in files_to_process),
        100,
    ):
        yield case


class VerifiedFilesRecord:
    """
    retain the hash and basename for each downloaded file. downloads the
    canonical hashes from wikipedia if they are not stored locally.
    """

    def __init__(self):
        self.canonical_record = "canonical_hashes.txt"
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
            self.files = set(map(lambda s: s.strip(), open(self.record_in_storage).readlines()))
        else:
            open(self.record_in_storage, "a").close()
            self.files = set()

    def __contains__(self, filename):
        return filename in self.files

    def add(self, filename):
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


def check_hash(filename: str) -> Optional[Dict]:
    verified_files = VerifiedFilesRecord()
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


def diff(old: str, new: str) -> Generator[str, None, None]:
    END_PUNCTUATION = ".!?"
    JUNK_CHUNKS = {"", *END_PUNCTUATION}

    def preceding_sentence_end(pretty_diff, i):
        for i2 in range(i - 1, 0, -1):
            if (
                pretty_diff[i2] in " +"
                and pretty_diff[i2][1] in END_PUNCTUATION
            ):  # todo check for ellipsis here
                return i2 + 1
        return 0

    def parse_range(pretty_diff, r):
        return "".join(
            pretty_diff[i][1]
            for i in r
            if pretty_diff[i][0] in " +"  # don't include "-" (removed values)
        )

    def map_parsed_to_diff(pretty_diff):
        parsed_to_diff_indices = {}
        parsed_list = []
        for diff_index in range(len(pretty_diff)):
            if pretty_diff[diff_index][0] in " +":
                parsed_to_diff_indices[len(parsed_list)] = diff_index
                parsed_list.append(pretty_diff[diff_index][1])
        return "".join(parsed_list), parsed_to_diff_indices

    def sentence_ranges(pretty_diff) -> Iterable[Tuple[int, int]]:
        parsed, parsed_to_diff_mapping = map_parsed_to_diff(pretty_diff)
        return (
            (
                parsed_to_diff_mapping[parsed_start],
                parsed_to_diff_mapping.get(parsed_end, len(pretty_diff)),
            )
            for (parsed_start, parsed_end) in SENTENCE_SPLITTER.span_tokenize(
                parsed
            )
        )

    def range_includes_added_or_edited_section(diff_range):
        return any(
            diff_item[0] in "-+" for diff_item in diff_range
        ) and not all(diff_item[0] == "-" for diff_item in diff_range)

    pretty_diff = list(
        filter(
            lambda ds: not any(
                ["+++" in ds, "---" in ds, "@@" in ds]
            ),  # diff string headers (not useful for us)
            difflib.unified_diff(old, new, n=max(len(old), len(new))),
        )
    )
    sentences_update_mapping = {
        (starti, endi): range_includes_added_or_edited_section(
            pretty_diff[starti:endi]
        )
        for starti, endi in sentence_ranges(pretty_diff)
    }
    sorted_sentence_ranges = sorted(sentences_update_mapping.keys())
    window_start = 0
    while window_start < len(sorted_sentence_ranges):
        # check if the current window starts with a changed or added sentence.
        if sentences_update_mapping[sorted_sentence_ranges[window_start]]:
            # if the current window starts with a changed or added sentence, set the window end.

            # if multiple consecutive sentences are updated or added, include them in the window.
            for window_end in range(window_start, len(sorted_sentence_ranges)):
                if not sentences_update_mapping[
                    sorted_sentence_ranges[window_end]
                ]:
                    window_end -= 1
                    break

            # find the text contained in the window.
            parsed = parse_range(
                pretty_diff,
                range(
                    sorted_sentence_ranges[window_start][0],
                    sorted_sentence_ranges[window_end][1],
                ),
            )

            # if the text contains actual content, yield it.
            if parsed.strip() not in JUNK_CHUNKS:
                yield parsed

            # advance the window and loop.
            window_start = max(window_end + 1, window_start + 1)
        else:
            window_start += 1


def test_diff():
    diff_maps = {
        "no diff": (
            [],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a dark and stormy night. A second sentence. A third.",
            ),
        ),
        "creation": (
            ["it was a dark and stormy night. A second sentence. A third."],
            (
                "",
                "it was a dark and stormy night. A second sentence. A third.",
            ),
        ),
        "deletion": (
            [],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "",
            ),
        ),
        "first sentence: one word change": (
            ["it was a light and stormy day."],
            (
                "it was a dark and stormy day. A second sentence. A third.",
                "it was a light and stormy day. A second sentence. A third.",
            ),
        ),
        "first sentence: two non-continuous words changed in same sentence": (
            ["it was a light and bright day."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a light and bright day. A second sentence. A third.",
            ),
        ),
        "middle sentence changed": (
            ["Another sentence."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a dark and stormy night. Another sentence. A third.",
            ),
        ),
        "last sentence changed": (
            ["A fourth."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a dark and stormy night. A second sentence. A fourth.",
            ),
        ),
        "two continuous sentences changed": (
            ["Another sentence. A fourth."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a dark and stormy night. Another sentence. A fourth.",
            ),
        ),
        "two non-continuous sentences changed": (
            ["it was a light and bright day.", "A fourth."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a light and bright day. A second sentence.  A fourth.",
            ),
        ),
        "sentence prepended": (
            ["A fourth."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "A fourth. it was a dark and stormy night. A second sentence. A third.",
            ),
        ),
        "sentence imputed": (
            ["And a fourth."],
            (
                "it was a dark and stormy night. A second sentence. Then, a third.",
                "it was a dark and stormy night. A second sentence. And a fourth. Then, a third.",
            ),
        ),
        "sentence appended": (
            ["And a fourth."],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "it was a dark and stormy night. A second sentence. A third. And a fourth.",
            ),
        ),
        "sentence prepended, imputed, and appended": (
            [
                "The fourth sentence is prepended.",
                "another fourth sentence is imputed.",
                "A final fourth appends itself.",
            ],
            (
                "it was a dark and stormy night. A second sentence. A third.",
                "The fourth sentence is prepended. it was a dark and stormy night. another "
                "fourth sentence is imputed. A second sentence. A third. A final fourth appends itself.",
            ),
        ),
        "special case: ellipses (...)": (
            ["A single sentence was changed... with an ellipsis!"],
            (
                "A sentence. A single sentence was changed... I don't know which one, though!",
                "A sentence. A single sentence was changed... with an ellipsis!",
            ),
        ),
        "special case: abbreviations": (
            ["The President of the U.S., J.F.K, is here."],
            (
                "The President of the US, J.F.K, is here.",
                "The President of the U.S., J.F.K, is here.",
            ),
        ),
        "special case: filename with dot (hello_world.py)": (
            ["The file was titled hello_world.py."],
            (
                "The file was titled incorrect.docx. A sentence.",
                "The file was titled hello_world.py. A sentence.",
            ),
        ),
    }

    for case, (correct_diffs, (old, new)) in diff_maps.items():
        diffs = list(diff(old, new))
        assert len(diffs) == len(correct_diffs)
        for i in range(len(correct_diffs)):
            assert diffs[i] == correct_diffs[i]


def process_one_revision(
    case: Dict, parents, map_parent_id_to_lost_kids
) -> List[Dict]:
    _parent_id = case.get("parent", None)
    parent_id = int(_parent_id) if _parent_id else None
    parent = parents.pop(parent_id, None) if parent_id else None
    id = int(case["id"])
    text = case["text"]
    if parent_id is None:
        return [case]
    elif parent:
        parents[id] = case
        return [{**case, "text": text} for text in diff(parent["text"], text)]
    else:
        if parent_id not in map_parent_id_to_lost_kids:
            map_parent_id_to_lost_kids[parent_id] = case

    kid = map_parent_id_to_lost_kids.pop(id, None)
    if kid:
        return [{**case, "text": text} for text in diff(text, kid["text"])]
    return []


T = TypeVar("T")


def merge_generators(
    executor: Executor,
    generators: Iterable[Generator[T, None, None]],
    batchsize: int = 1,
) -> Generator[T, None, None]:
    """
    Combines the output of multiple generators into a single generator. Uses a `concurrent.futures.Executor` to
    concurrently exhaust input generators. Since regular generators cannot be polled concurrently,
    the number of concurrent tasks submitted by this function is at most the number of generators.
    Output order is not guaranteed.

    If the output of this function is not exhausted, each input generators may be polled up to batchsize times
    without the results being returned.

    Larger batchsize values are especially useful when dealing with generators that return quickly.

    The number of input generators is considered finite and reasonably small.

    :param executor: executor in which generators are run.
    :param generators: generators to combine results from.
    :param batchsize: number of batches polled from each generator sequentially. Default is 1.
    :return: a generator over the combined outputs of all input generators.
    """

    if batchsize < 1:
        raise ValueError("chunksize must be ≥ 1")

    def next_chunk(generator):
        output = []
        for value in generator:
            output.append(value)
            if len(output) == batchsize:
                return False, output, generator
        return True, output, None

    running_generators = [
        executor.submit(next_chunk, generator) for generator in generators
    ]
    while len(running_generators) > 0:
        incomplete = []
        for future in as_completed(running_generators):
            (is_exhausted, values, generator) = future.result()
            for value in values:
                yield value
            if not is_exhausted:
                incomplete.append(executor.submit(next_chunk, generator))
        running_generators = incomplete


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

def rescan(executor, map_parent_id_to_lost_kids, parents) -> Generator[Dict, None, None]:
    orphan_cases = list(map_parent_id_to_lost_kids.values())
    orphan_tups = (
        (orphan_case, parents, map_parent_id_to_lost_kids)
        for orphan_case in orphan_cases
    )
    complete_cases = [case for il in executor.map(lambda tup: process_one_revision(*tup), orphan_tups) for case in il]
    print(f"rescanned found: {complete_cases} n cases: {len(orphan_cases)}")
    return complete_cases

def all_happy_cases(
    executor: Executor, revisions: Iterable[Revision]
) -> Generator[Dict, None, None]:
    """
    notes: this function consumes its input. Its output is not ordered.

    Finding all valid cases has significant memory and storage requirements, as we must retain the most recent known
    revision for
    every known article in memory as we iterate through all revisions. If input is not ordered, we may also be required
    to retain multiple revisions of articles until the intermediary ones have been found.
    """
    print(f"{strtime()} de-chaining revisions... 🔗")
    chunk_size = (os.cpu_count() or 4) * 20
    rescan_after = 100

    revisions_handled = 0
    map_parent_id_to_lost_kids = dict()
    parents = dict()
    rescan_futures = []
    # with StorageDict() as map_parent_id_to_lost_kids:
    #     with StorageDict() as parents:
    for intermediary_list in lazy_executor_map(
        executor,
        lambda tup: process_one_revision(asdict(tup[0]), tup[1], tup[2]),
        (
            (revision, parents, map_parent_id_to_lost_kids)
            for revision in revisions
        ),
        max_parallel=chunk_size,
    ):
        for case in intermediary_list:
            yield case
        revisions_handled += 1
        if revisions_handled % rescan_after == 0:
            if len(rescan_futures) < 0.9 ** len(map_parent_id_to_lost_kids):
                rescan_futures.append(
                    executor.submit(lambda tup: rescan(*tup), (executor, map_parent_id_to_lost_kids, parents))
                )
            else:
                for future in as_completed(rescan_futures):
                    for case in future.result():
                        yield case

    if not len(map_parent_id_to_lost_kids) == 0:
        orphans_out = "orphans_out.csv.bz2"
        print(
            f"orphan revisions identified. Saving in a second output file: {orphans_out}"
        )
        with bz2.open(orphans_out, "wt", newline="") as output_file:
            writer = csv.DictWriter(output_file, Revision.fields())
            for _id, orphan_case in map_parent_id_to_lost_kids.items():
                writer.writerow(orphan_case)


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


class StorageDict:
    """
        Dict-like interface where values are stored on disk (one per temporary file).
        Values must be able to be encoded as JSONs using the python standard json library.
        Values on disk are bz2-compressed.

        The files are made in a new temporary directory. The parent for this directory can be set using the path
        argument. If not set, the working directory is used.

        Filenames are the hash of the key. Files are deleted when a key is deleted, close() is called, or __exit__() is
        called, as when the end of the `with StorageDict() as d:` indented block is
        reached, or if an exception occurs within the block).

        Unless you have a really strong reason not to do so, using StorageDict with the context api (`with _ as _:`) is
        usually the best way to avoid accidentally leaving temporary files on your disk in the case of an uncaught
        error/exception.

        Basic concurrency is supported (added and deleting
        entries within unique threads). Multiple simultaneous edits on the same key are *not* currently supported–
        no lock is implemented.

    """

    keys_to_files: Dict
    directory: tempfile.TemporaryDirectory
    num_subdirs: int

    def __init__(self, path=".", num_subdirs:int =1000, memory_cap: int=0):
        self.keys_to_files: Dict[Any, Tuple(str, threading.Lock)] = dict()
        self.directory = tempfile.TemporaryDirectory(dir=path)
        self.num_subdirs = num_subdirs
        self.memory_cap = memory_cap
        self.key_lock = threading.Lock()

    def _read_path(self, path):
        with bz2.open(path, "rt") as f:
            return json.load(f)

    def __getitem__(self, item):
        with self.key_lock:
            path, file_lock = self.keys_to_files[
                item
            ]  # throws KeyError if key not found.
        with file_lock:
            try:
                return self._read_path(path)
            except EOFError:
                raise KeyError # entry deleted

    def __setitem__(self, key, value):
        with self.key_lock:
            if key in self.keys_to_files:
                path, file_lock = self.keys_to_files[key]
                with file_lock:
                    with bz2.open(path, "wt") as f:
                        json.dump(value, f)
                self.keys_to_files[key] = path, file_lock
                return
            else:
                key_hash = hash(key)
                subdir = os.path.join(
                    self.directory.name, str(key_hash % self.num_subdirs)
                )
                if not os.path.exists(subdir):
                    try:
                        os.mkdir(subdir)
                    except FileExistsError:
                        pass
                path = os.path.join(subdir, str(key_hash))
                with bz2.open(path, "wt") as f:
                    json.dump(value, f)
                self.keys_to_files[key] = path, threading.Lock()

    def __contains__(self, item):
        with self.key_lock:
            return item in self.keys_to_files

    def __delitem__(self, key):
        with self.key_lock:
            path, file_lock = self.keys_to_files.pop(key)
        with file_lock:
            os.remove(path)

    def __len__(self):
        with self.key_lock:
            return len(self.keys_to_files)

    def __repr__(self):
        with self.key_lock:
            return f"<StorageDict object at {id(self)} with {len(self.keys_to_files)} entries>"

    def items(self):
        with self.key_lock:
            for k in self.keys_to_files:
                path, file_lock = self.keys_to_files[k]
                with file_lock:
                    try:
                        yield k, self._read_path(path)
                    except EOFError:
                        pass

    def pop(self, *args):
        try:
            with self.key_lock:
                path, file_lock = self.keys_to_files.pop(args[0])
            with file_lock:
                try:
                    return self._read_path(path)
                except EOFError:
                    raise KeyError # entry deleted
        except KeyError:
            if len(args) == 1:
                raise KeyError
            elif len(args) == 2:
                return args[1]
            else:
                raise TypeError(
                    f"pop takes one or two arguments. {len(args)} arguments passed."
                )

    def close(self):
        self.directory.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def test_storage_dict_context():
    with StorageDict() as d:
        d["nice"] = "cool"
        d["d"] = {"a": "dictionary"}
        d["n"] = -1

        assert d["nice"] == "cool"
        assert d["d"] == {"a": "dictionary"}
        assert d["n"] == -1
        directory = d.directory.name

    assert not os.path.exists(directory)


def test_storage_dict_delete():
    with StorageDict() as d:
        d["n"] = -1
        del d["n"]
        try:
            d["n"]
        except KeyError:
            pass
        else:
            raise RuntimeError


def test_storage_dict_pop():
    with StorageDict() as d:
        d["n"] = -1
        assert d.pop("n", 1) == -1
        assert d.pop("n", 1) == 1
        d["n"] = -2
        assert d.pop("n", 2) == -2


def test_storage_dict_set_path():  # hack side effects here are not great
    test_path = ".test_storage_dict_set_path"
    if os.path.exists(test_path):
        os.rmdir(test_path)
    os.mkdir(test_path)
    with StorageDict(path=test_path) as d:
        assert os.path.dirname(d.directory.name) == test_path
    os.rmdir(test_path)


def test_multithreading():
    def add_value(d: StorageDict, i: int) -> None:
        d[i] = i

    def delete_value(d: StorageDict, i: int) -> None:
        del d[i]

    for num_workers in range(1, os.cpu_count() * 5):
        with StorageDict() as d:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                list(executor.map(lambda i: add_value(d, i), range(10)))
                print(
                    f"test add value: num workers: {num_workers} storage dict: {d}"
                )
                for i in range(10):
                    assert d[i] == i
                list(executor.map(lambda i: delete_value(d, i), range(10)))
                print(
                    f"test delete value: num workers: {num_workers} storage dict: {d}"
                )
                assert len(d) == 0


def test_with_lazy_executor_map():
    def add_value(d: StorageDict, i: int) -> None:
        d[i] = i

    def delete_value(d: StorageDict, i: int) -> None:
        del d[i]

    for num_workers in range(1, os.cpu_count() * 5):
        with StorageDict() as d:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                list(
                    lazy_executor_map(
                        executor,
                        lambda i: add_value(d, i),
                        range(10),
                        max_parallel=num_workers,
                    )
                )
                print(
                    f"test add value: num workers: {num_workers} storage dict: {d}"
                )
                for i in range(10):
                    assert d[i] == i

            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                list(
                    lazy_executor_map(
                        executor,
                        lambda i: delete_value(d, i),
                        range(10),
                        max_parallel=num_workers,
                    )
                )
                print(
                    f"test delete value: num workers: {num_workers} storage dict: {d}"
                )
                assert len(d) == 0


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
    Runs max_parallel jobs or fewer simultaneously. This is ideal when you have few, long-running tasks instead of
    many short-running tasks. Input order is preserved.

    :param max_parallel: Number of parallel jobs to run. Should be greater than zero.
    :return: A generator that eagerly returns input values. If the generator is not
    run to exhaustion, the function will not be run for all inputs.
    """
    if max_parallel < 1:
        raise ValueError(
            f"max_parallel is not greater than or equal to one: {max_parallel}"
        )

    chunk = []
    for inp in function_inputs:
        chunk.append(inp)
        if len(chunk) == max_parallel:
            for v in executor.map(function, chunk):
                yield v
            chunk = []
    if len(chunk) > 0:
        for v in executor.map(function, chunk):
            yield v


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
) -> Generator[Revision, None, None]:
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


def write_diffs_from_revisions(
    executor: Executor, revisions: Iterable[Revision]
):
    with bz2.open("revisions.csv.bz2", "wt", newline="") as output_file:
        writer = csv.DictWriter(output_file, Revision.fields())

        i = 0
        for case in all_happy_cases(executor, revisions):
            writer.writerow(case)
            i += 1
            if i % 1000 == 0 or i == 1:
                print(f"{strtime()} writing revision #{i}")

    print(f"{strtime()} program complete. 💐")


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 20) as executor:
        complete = False
        while not complete:
            try:
                revisions = download_and_parse_files(executor)
                write_diffs_from_revisions(executor, revisions)
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
