import bz2
import csv
import fcntl
import math
import os
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import Callable, Iterable, Dict, List, Optional
import time

import dill

from wikipedia_revisions import config, FIELDS
from wikipedia_revisions.utils import timestr, run_dilled_function


def _write_rows_to_pipe(
    pipe_dir: str,
    revision_maker: Callable[..., Iterable[Dict]],
    fields: List[str],
    process_buffer_length: int,
):
    # instantiate pipe
    pipe_name = f"revisions-{os.getpid()}-{str(time.time()).replace('.', '')}.pipe"
    pipe_path = os.path.join(pipe_dir, pipe_name)
    os.mkfifo(pipe_path)

    print(f"{timestr()} writing out to {pipe_name}... 🖋️")
    revisions_written = 0
    buffer = []
    with open(pipe_path, "w") as out:
        writer = csv.DictWriter(out, fields)
        writer.writeheader()
        for revision in revision_maker():
            buffer.append(revision)
            if len(buffer) >= process_buffer_length:
                for rev in buffer:
                    writer.writerow(rev)
                revisions_written += len(buffer)
                buffer = []
    return revisions_written


def _write_rows_to_bzipped_csv(
    revision_maker: Callable[..., Iterable[Dict]],
    filename: Optional[str],
    fields: List[str],
    process_buffer_length: int,
):
    revisions_written = 0
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
            revisions_written += 1
    return revisions_written


def write_to_csv(
    single_output_filename: Optional[str],
    revision_iterator_functions: Iterable[Callable[..., Iterable[Dict]]],
) -> None:
    if single_output_filename is not None:
        with bz2.open(single_output_filename, "wt", newline="") as out:
            writer = csv.DictWriter(out, FIELDS)
            writer.writeheader()

    revisions_written = 0
    active_readers = set()
    backlog_per_process = int(
        math.floor(config["backlog"] / config["concurrent_reads"])
    )
    with ProcessPoolExecutor(max_workers=config["concurrent_reads"]) as executor:
        for revision_maker in revision_iterator_functions:
            if single_output_filename is not None:
                dilled_function = dill.dumps(
                    lambda: _write_rows_to_bzipped_csv(
                        revision_maker,
                        single_output_filename,
                        FIELDS,
                        backlog_per_process,
                    )
                )
            else:
                dilled_function = dill.dumps(
                    lambda: _write_rows_to_pipe(
                        config["pipe_dir"], revision_maker, FIELDS, backlog_per_process
                    )
                )
            active_readers.add(executor.submit(run_dilled_function, dilled_function))

            # when at capacity, wait for current readers to finish
            while len(active_readers) >= config["concurrent_reads"]:
                completed_readers, active_readers = wait(
                    active_readers, return_when=FIRST_COMPLETED, timeout=60
                )
                for future in completed_readers:
                    revisions_written += future.result()
                    print(f"{timestr()} wrote revision #{revisions_written}")

        # wait for last readers to finish
        while len(active_readers) > 0:
            completed_readers, active_readers = wait(
                active_readers, return_when=FIRST_COMPLETED, timeout=120
            )
            for future in completed_readers:
                revisions_written += future.result()
                print(f"{timestr()} wrote revision #{revisions_written}")
            completed_readers = {}

        # count any remaining completed readers
        for future in completed_readers:
            revisions_written += future.result()
            print(f"{timestr()} wrote revision #{revisions_written}")
