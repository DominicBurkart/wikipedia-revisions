import concurrent.futures
import time
import traceback
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
    FIRST_COMPLETED,
)
from typing import Dict, Tuple, Iterable, Callable

import dill
from dateutil.parser import parse as parse_timestamp
from sqlalchemy import create_engine, Column, Integer, Text, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database, drop_database

from utils import timestr

dill.settings["recurse"] = True

SMALLBATCH = 50 * 1024 * 1024
BIGBATCH = 500 * 1024 * 1024
COMMIT_RETRIES = 6


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


class DatabaseAlreadyExists(Exception):
    ...


def _retype_revision(revision: Dict) -> Dict:
    parent_id_str = revision["parent_id"]
    contributor_id_str = revision["contributor_id"]
    return {
        **revision,
        "id": int(revision["id"]),
        "parent_id": int(parent_id_str) if parent_id_str is not None else None,
        "timestamp": parse_timestamp(revision["timestamp"]),
        "contributor_id": int(contributor_id_str) if contributor_id_str else None,
    }


def _retype_and_size_revision(raw_revision: Dict) -> Tuple[Dict, int]:
    """
    This function calls the revision retype function and estimates the size of
    a revision in memory based on the size of its text and comment.
    """
    revision = _retype_revision(raw_revision)
    size = (
        len((revision.get("text") or "").encode("utf-8"))
        + len((revision.get("comment") or "").encode("utf-8"))
        + 300  # estimate for the remaining small fields
    )
    # ^ sys.getsizeof doesn't work in pypy
    return revision, size


def _commit_wrapper(fn: Callable[[], None], n_attempt: int = 1):
    try:
        fn()
    except SQLAlchemyError as e:
        if n_attempt < COMMIT_RETRIES:
            sleep_seconds = n_attempt * 15
            print(
                f"{timestr()} error while committing. "
                f"Sleeping {sleep_seconds} seconds and retrying (attempt #{n_attempt}). "
                f"Error: {e} (printing stack trace below)"
            )
            traceback.print_exc()
            time.sleep(sleep_seconds)
            _commit_wrapper(fn, n_attempt + 1)
        else:
            raise e


def _commit_batch(Session, batch):
    def _commit():
        Session.bulk_insert_mappings(Revision, batch)
        Session.commit()
        Session.remove()

    _commit_wrapper(_commit)


def _multi_insert(Session, batch):
    def _commit():
        Session.execute(Revision.__table__.insert(), batch)
        Session.commit()
        Session.remove()

    _commit_wrapper(_commit)


def _run_dilled_function(dilled_extractor_function: bytes):
    try:
        f = dill.loads(dilled_extractor_function)
        res = f()
        return res
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e


def _run_one_extractor(extractor, config):
    engine = create_engine(
        config["database_url"], pool_size=config["num_db_connections"]
    )
    Session = scoped_session(sessionmaker(bind=engine))

    revisions = extractor()
    retyped_and_sized_revisions = map(_retype_and_size_revision, revisions)

    n_revisions_processed = 0
    batch_size = 0
    max_batch_size = SMALLBATCH if config["low_memory"] else BIGBATCH
    active_commits = set()
    num_commits = 0
    last_dispose = None
    batch = []
    with ThreadPoolExecutor() as executor:
        for revision, revision_size in retyped_and_sized_revisions:
            batch.append(revision)
            batch_size += revision_size
            if batch_size >= max_batch_size:
                # add new commit task
                if config["insert_multiple_values"]:
                    commit_future = executor.submit(_multi_insert, Session, batch)
                else:
                    commit_future = executor.submit(_commit_batch, Session, batch)

                active_commits.add(commit_future)
                del commit_future

                # label new commit task as previous commit task & increment
                num_commits += 1
                batch_size = 0
                batch = []

                # re-initialize engine to keep memory low
                # see https://github.com/DominicBurkart/wikipedia-revisions/issues/15
                if num_commits % (config["num_db_connections"] * 2) == 0:
                    if last_dispose is not None:
                        last_dispose.result()
                    last_dispose = executor.submit(engine.dispose)

                # if at connection limit, sleep this thread until at least one is finished.
                while len(active_commits) >= config["num_db_connections"]:
                    completed, active_commits = wait(
                        active_commits, return_when=FIRST_COMPLETED, timeout=30
                    )
                    for future in completed:
                        future.result()
            n_revisions_processed += 1

        while len(active_commits) >= config["num_db_connections"]:
            active_commits.pop().result()
        if config["insert_multiple_values"]:
            commit_future = executor.submit(_multi_insert, Session, batch)
        else:
            commit_future = executor.submit(_commit_batch, Session, batch)
        commit_future.result()
        Session.remove()
    return n_revisions_processed


def write(
    config: Dict, revision_iterator_functions: Iterable[Callable[..., Iterable[Dict]]]
) -> None:

    print(f"{timestr()} structuring database... 📐")
    engine = create_engine(
        config["database_url"], pool_size=config["num_db_connections"]
    )
    if database_exists(engine.url):
        if config["delete_database"]:
            drop_database(engine.url)
        else:
            raise DatabaseAlreadyExists

    try:
        create_database(engine.url)
        assert database_exists(engine.url)
        Base.metadata.create_all(engine)
        print(f"{timestr()} adding revisions to session... 📖")
        active_extractor_futures = set()
        num_total_processed = 0
        with ProcessPoolExecutor() as executor:
            for revision_iterator_function in revision_iterator_functions:
                dilled_extractor = dill.dumps(
                    lambda: _run_one_extractor(revision_iterator_function, config)
                )
                active_extractor_futures.add(
                    executor.submit(_run_dilled_function, dilled_extractor)
                )
                while len(active_extractor_futures) >= config["concurrent_reads"]:
                    try:
                        completed, active_extractor_futures = wait(
                            active_extractor_futures,
                            return_when=FIRST_COMPLETED,
                            timeout=30,
                        )
                        if len(completed) > 0:
                            for future in completed:
                                num_total_processed += future.result()
                            print(
                                f"{timestr()} {num_total_processed} total revisions processed 💼"
                            )
                    except concurrent.futures.TimeoutError:
                        pass
            while len(active_extractor_futures) >= config["concurrent_reads"]:
                num_total_processed += active_extractor_futures.pop().result()
        print(
            f"{timestr()} {num_total_processed} revisions written to database at: {config['database_url']} 🌈"
        )
    except Exception as e:
        if isinstance(e, DatabaseAlreadyExists):
            print(
                f"{timestr()} there is already a local version of the database. Doing nothing. HELP: to "
                f"overwrite database, use --delete-database flag. 🌅"
            )
            raise e
        print(
            f"{timestr()} exception while writing. deleting partial database & re-raising exception. 🌋"
        )
        drop_database(engine.url)
        raise e
