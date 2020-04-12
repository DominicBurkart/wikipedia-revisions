# wikipedia-revisions
download every wikipedia edit

[![status](https://github.com/DominicBurkart/wikipedia-revisions/workflows/Python%20application/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions/actions?query=is%3Acompleted+branch%3Amaster)


This script downloads every revision to wikipedia. It can output the revisions into a [sqlalchemy-supported database](https://docs.sqlalchemy.org/en/13/dialects/) or into single bz2-zipped csv file.

Revisions are output with the following fields:
- `id`: the revision id, numeric
- `parent_id`: parent revision id (if it exists), numeric
- `page_id`: id of page, numeric
- `page_name`: name of page
- `page_ns`: page namespace, numeric
- `timestamp`: timestamp with timezone
- `contributor_id`: id of contributor, numeric
- `contributor_name`: screen name of contributor
- `contributor_ip`: ip addresss of contributor
- `text`: complete article text after the revision is applied, string in wikipedia markdown
- `comment`: comment

All fields except id and timestamp may be null.

System requirements:
- 4gb memory
- ~3tb storage
- python 3 & pip pre-installed

I wrote a [blog post](https://dominicburkart.com/blog/2020/big_data_and_small_computers.html) on some of the project 
goals and technical choices. PyPy is supported while outputting to a csv,
but not while outputting to a database.

## Installation

For writing to a bz2-compressed csv file:
```sh
git clone https://github.com/DominicBurkart/wikipedia-revisions
cd wikipedia-revisions
pip3 install -r requirements.txt
```

For writing to a database:
```sh
git clone https://github.com/DominicBurkart/wikipedia-revisions
cd wikipedia-revisions
pip3 install -r database_requirements.txt
```

## Use

Use `--help` to see the available options:
```sh
python3 wikipedia_download.py --help
```

Output all revisions into a giant bz2-zipped csv:
```sh 
python3 wikipedia_download.py
```

Use a wikipedia dump from a specific date:
```sh
python3 wikipedia_download.py --date 20200101
```

Output to postgres database named "wikipedia-revisions" waiting at localhost port 5432:
```sh
python3 wikipedia_download.py --database
```

To set the database url:
```sh
python3 wikipedia_download.py --database --database-url postgres://postgres@localhost:5432/wikipedia-revisions
```
