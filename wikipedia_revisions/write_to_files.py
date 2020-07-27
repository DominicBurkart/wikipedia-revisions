import bz2
import csv
import fcntl
import math
import os
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import Callable, Iterable, Dict, List, Optional

from wikipedia_revisions.download import config, FIELDS


def _write_rows_to_pipe(
    pipe_dir: str, revision_maker: Callable[..., Iterable[Dict]], fields: List[str]
):
    pipe_name = f"revisions-{os.getpid()}-{str(time.time()).replace('.', '')}.pipe"
    pipe_path = os.path.join(pipe_dir, pipe_name)
    os.mkfifo(pipe_path)
    print(f"{timestr()} writing out to {pipe_name}... 🖋️")
    i = 0
    with open(pipe_path, "w") as out:
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
        _write()