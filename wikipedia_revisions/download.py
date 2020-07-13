import bz2
import csv
import errno
import hashlib
import os
import platform
import re
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait,
    FIRST_COMPLETED,
)
from typing import Optional, Dict, Generator, Iterable, Tuple, Callable, List
import fcntl
import math


import click
import requests

from wikipedia_revisions.utils import (
    timestr,
    peek_ahead,
    unordered_incremental_executor_map,
    LazyList,
)

config = dict()

FIELDS = [
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
]


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
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
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


def parse_one_file(filename: str) -> Generator[Dict, None, None]:
    pid = os.getpid()
    print(
        f"{timestr()} extracting revisions from update file {filename} in process #{pid}... 🧛"
    )
    with bz2.open(filename, "rt", newline="") as uncompressed:
        for revision in generate_revisions(uncompressed):
            yield revision
    print(f"{timestr()} exhausted file: {filename} 😴")
    if config["low_storage"]:
        print(f"{timestr()} Deleting {filename}... ✅")
        os.remove(filename)


def verify_files(
    download_file_and_url: Iterable[Tuple[str, str]],
    append_bad_urls,
    executor: ThreadPoolExecutor,
) -> Generator[str, None, None]:
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
        filenames_and_urls = unordered_incremental_executor_map(
            executor,
            lambda tup: (check_hash(verified_files, tup[0]), tup[1]),
            download_file_and_url,
            max_parallel=os.cpu_count() or 4,
            max_backlog=4 * (os.cpu_count() or 4),
        )

    # yield filenames that passed checksum
    for filename, url in filenames_and_urls:
        if filename:
            yield filename
        else:
            append_bad_urls.append(url)


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
            print(f"{timestr()} hash mismatch with {filename}. Deleting file.🗑️")
            os.remove(filename)
            return None

    print(f"{timestr()} {filename} hash verified 💁")
    return filename


def full_dump_url_from_partial(partial: str):
    if config["date"] != "latest" and partial.startswith("/"):
        return "https://dumps.wikimedia.org" + partial
    elif config["date"] == "latest" and not partial.startswith("/"):
        return "https://dumps.wikimedia.org/enwiki/latest/" + partial
    else:
        raise ValueError("dump page format has been updated.")


def download_and_parse_files() -> Iterable[Callable[..., Generator[Dict, None, None]]]:
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

    with ThreadPoolExecutor() as executor:
        # download & process the history files
        file_and_url = unordered_incremental_executor_map(
            executor,
            lambda url: (download_update_file(session, url), url),
            updates_urls,
            max_parallel=2,
            max_backlog=2 if config["low_storage"] else -1,
        )

        # verify files were correctly downloaded
        verified_files = verify_files(
            file_and_url, append_bad_urls=updates_urls, executor=executor
        )

        # create functions that read and parse valid files
        for filename in verified_files:
            yield lambda: parse_one_file(filename)


def _write_rows_to_pipe(
    pipe_dir: str, revision_maker: Callable[..., Iterable[Dict]], fields: List[str]
):
    pipe_name = f"revisions-{os.getpid()}-{str(time.time()).replace('.', '')}.pipe"
    os.mkfifo(pipe_name)
    print(f"{timestr()} writing out to {pipe_name}... 🖋️")
    i = 0
    with open(os.path.join(pipe_dir, pipe_name), "w") as out:
        writer = csv.DictWriter(out, fields)
        writer.writeheader()
        for revision in revision_maker():
            writer.writerow(revision)
            i += 1
        return i


def _write_rows_to_bzipped_csv(
    revision_maker: Callable[..., Iterable[Dict]],
    filename: Optional[str],
    fields: List[str],
    process_buffer_length: int,
):
    i = 0
    with bz2.open(filename, "at", newline="") as out:
        writer = csv.DictWriter(out, fields)
        buffer = []
        for revision in revision_maker():
            buffer.append(revision)
            if len(buffer) >= process_buffer_length:
                fcntl.flock(out, fcntl.LOCK_EX)
                for rev in buffer:
                    writer.writerow(rev)
                out.flush()
                fcntl.flock(out, fcntl.LOCK_UN)
                buffer = []
            i += 1
        return i


def write_to_csv(
    output_filename: Optional[str],
    revision_iterator_functions: Iterable[Callable[..., Iterable[Dict]]],
) -> None:
    def _write():
        i = 0
        active_readers = set()
        with ProcessPoolExecutor(max_workers=config["concurrent_reads"]) as executor:
            for revision_maker in revision_iterator_functions:
                while len(active_readers) >= config["concurrent_reads"]:
                    completed_readers, active_readers = wait(
                        active_readers, return_when=FIRST_COMPLETED, timeout=60
                    )
                    for future in completed_readers:
                        i += future.result()
                        print(f"{timestr()} wrote revision #{i}")
                if output_filename is None:
                    active_readers.add(
                        executor.submit(
                            _write_rows_to_pipe,
                            config["pipe_dir"],
                            revision_maker,
                            FIELDS,
                        )
                    )
                else:
                    active_readers.add(
                        executor.submit(
                            _write_rows_to_bzipped_csv,
                            revision_maker,
                            output_filename,
                            FIELDS,
                            int(
                                math.floor(
                                    config["backlog"] / config["concurrent_reads"]
                                )
                            ),
                        )
                    )
            while len(active_readers) > 0:
                completed_readers, active_readers = wait(
                    active_readers, return_when=FIRST_COMPLETED, timeout=120
                )
                for future in completed_readers:
                    i += future.result()
                    print(f"{timestr()} wrote revision #{i}")

    if output_filename is None:
        _write()
    else:
        with bz2.open(output_filename, "wt", newline="") as out:
            writer = csv.DictWriter(out, FIELDS)
            writer.writeheader()
            out.flush()
        _write()


def write_to_database(
    revision_iterator_functions: Iterable[Callable[..., Iterable[Dict]]]
) -> None:
    from wikipedia_revisions.write_to_database import write

    write(config, revision_iterator_functions)


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
    "-o",
    "--output",
    "output_file",
    default=os.path.join(os.path.curdir, "revisions.csv.bz2"),
    help="set output path for csv.bz2. Defaults to revisions.csv.bz2 in current directory.",
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
    "--num-subprocesses",
    "concurrent_reads",
    default=2,
    type=int,
    help="number of concurrent processes, each reading one .xml.bz2 file. Default is 2. When using storage media "
    "with fast concurrent reads and high throughput (SSDs), higher values (e.g. the number of "
    "cpu cores) are better.",
)
@click.option(
    "--insert-multiple-values/--batch-insert",
    "insert_multiple_values",
    default=False,
    help="if writing to a database, insert multiple values within a single statement. Not supported for all "
    "SQLAlchemy-covered databases. For more information on multi-value inserts in SQLAlchemy, see: "
    "http://docs.sqlalchemy.org/en/latest/core/dml.html#sqlalchemy.sql.expression.Insert.values.params.*args",
)
@click.option(
    "--db-connections-per-process",
    "num_db_connections",
    default=4,
    type=int,
    help="number of DB connections per process. Default is 4. Must be > 0.",
)
@click.option(
    "-p",
    "--pipe-dir",
    "pipe_dir",
    default=False,
    help="write revisions as uncompressed csvs to a series of named pipes. Pipes are named "
    "revisions-<process number>-<numeric time string>.pipe, and are placed in the passed directory.",
)
def run(
    date,
    low_storage,
    use_database,
    output_file,
    database_url,
    low_memory,
    delete_database,
    concurrent_reads,
    insert_multiple_values,
    num_db_connections,
    pipe_dir,
):
    config["date"] = date
    config["dump_page_url"] = f"https://dumps.wikimedia.org/enwiki/{date}/"
    config[
        "md5_hashes_url"
    ] = f"https://dumps.wikimedia.org/enwiki/{date}/enwiki-{date}-md5sums.txt"
    config["low_storage"] = low_storage
    config["database_url"] = database_url
    config["low_memory"] = low_memory
    config["delete_database"] = delete_database
    config["concurrent_reads"] = concurrent_reads
    config["insert_multiple_values"] = insert_multiple_values
    config["num_db_connections"] = num_db_connections
    config["backlog"] = 300 if low_memory else 5000
    config["pipe_dir"] = pipe_dir

    if concurrent_reads < 1:
        raise ValueError("concurrent_reads must be at least 1.")
    if num_db_connections < 1:
        raise ValueError("num_db_connections must be at least 1.")

    print(f"{timestr()} program started. 👋")
    if config["low_storage"]:
        print(f"{timestr()} low storage mode active. 🐈 📦")

    complete = False
    while not complete:
        try:
            # download XML files from wikipedia and collect revisions
            revision_iterator_functions = download_and_parse_files()

            # write collected revisions to output.
            if pipe_dir is not None:
                write_to_csv(None, revision_iterator_functions)
            elif use_database:
                write_to_database(revision_iterator_functions)
            else:
                if os.path.exists(output_file):
                    print(f"{timestr()} overwriting file {output_file}... 🥛")
                write_to_csv(output_file, revision_iterator_functions)
            print(f"{timestr()} program complete. 💐")
            complete = True
        except Exception as e:
            if getattr(e, "errno", None) == errno.ENOSPC:
                print(f"{timestr()} no space left on device. Ending program. 😲")
                raise e
            SLEEP_SECONDS = 5 * 60
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
