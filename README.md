# wikipedia-revisions
download every wikipedia edit

[![status](https://github.com/DominicBurkart/wikipedia-revisions/workflows/Python%20application/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions/actions?query=is%3Acompleted+branch%3Amaster)


This script downloads every revision to wikipedia. It can output the revisions into a [sqlalchemy-supported database](https://docs.sqlalchemy.org/en/13/dialects/) or into single bz2-zipped csv file.

Revisions are output with the following fields:
- `id`: the revision id, numeric
- `parent_id`: parent revision id (if it exists), numeric
- `page_id`: id of page, numeric
- `page_title`: name of page
- `page_ns`: page namespace, numeric
- `timestamp`: timestamp with timezone
- `contributor_id`: id of contributor, numeric
- `contributor_name`: screen name of contributor
- `contributor_ip`: ip addresss of contributor
- `text`: complete article text after the revision is applied, string in wikipedia markdown
- `comment`: comment

The id, timestamp, page_id, page_title, and page_ns cannot be null. All other fields may be null.

System requirements:
- 4gb memory
- ~3tb storage
- python 3 & pip pre-installed

I wrote a [blog post](https://dominicburkart.com/blog/2020/big_data_and_small_computers.html) on some of the project 
goals and technical choices.

## Install

Installation requires pip, the python package manager. Installation 
works with either CPython or PyPy.

### CPython

CPython is the standard Python distribution.

```shell
python3 -m pip install git+https://github.com/DominicBurkart/wikipedia-revisions.git
```

### PyPy

PyPy is an alternate, faster Python distribution.

```shell
pypy3 -m pip install git+https://github.com/DominicBurkart/wikipedia-revisions.git
```

## Use

Use `--help` to see the available options:
```shell
python3 -m wikipedia_revisions.download --help
```

Note: if using PyPy, you can just substitute `pypy3` for `python3` 
for any of these commands, for example:
```shell
pypy3 -m wikipedia_revisions.download --help
```

Output all revisions into a giant bz2-zipped csv:
```shell 
python3 -u -m wikipedia_revisions.download
```

Use a wikipedia dump from a specific date:
```shell
python3 -u -m wikipedia_revisions.download --date 20200101
```

Output to a series of named pipes (posix-based systems only):
```shell
python3 -u -m wikipedia_revisions.download --pipe-dir /path/to/dir
```

Output to postgres database named "wikipedia_revisions" waiting at localhost port 5432:
```shell
python3 -u -m wikipedia_revisions.download --database
```

To set the database url:
```shell
python3 -u -m wikipedia_revisions.download --database --database-url postgres://postgres@localhost:5432/wikipedia_revisions
```

Note: If using PyPy to write to a database, currently only postgres is 
supported. With PyPy, any custom database url must point to a postgres 
database and start with `postgresql+psycopg2cffi`, as in 
`postgresql+psycopg2cffi:///wikipedia_revisions`.

## Configuration Notes
The above information is sufficient for you to run the program. The information below is useful for tuning performance.

- if you're using an SSD, set `--num-subprocesses` to a higher number (e.g. the number of CPU cores).
- this program is I/O heavy and relies on the OS's [page cache](https://en.wikipedia.org/wiki/Page_cache). Having a few gigabytes of free memory for the cache to use will improve I/O throughput.
- using an SSD provides substantial benefits for this program, by increasing I/O speed and eliminating needle-moving cost.
- if writing to a database stored on an external drive, run the program in a directory on a different drive than the database (and ideally the OS). The wikidump is downloaded into the current directory, so putting them on a different disk than the output database avoids throughput and needle-moving issues.