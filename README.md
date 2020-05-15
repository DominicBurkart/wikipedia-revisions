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
goals and technical choices. PyPy is supported while outputting to a csv,
but not while outputting to a database.

## Install

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

## Configuration Notes
The above information is sufficient for you to run the program. The information below is useful for optimization.

- this program is I/O heavy and relies on the OS's [page cache](https://en.wikipedia.org/wiki/Page_cache). Having a few gigabytes of free memory for the cache to use will improve I/O throughput.
- using an SSD provides substantial benefits for this program, by increasing I/O speed and eliminating needle-moving cost.
- if writing to a database stored on an external drive, run the program in a directory on a different drive than the database (and ideally the OS). The wikidump is downloaded into the current directory, so putting them on a different disk than the output database avoids throughput and needle-moving issues. As an example configuration, here is the command that I used to process the revisions into a local postgres database using an raspberry pi 4 with two external drives (a 240gb SSD, and a 6tb spinning disk that holds the output database). The `nohup` command prevents the command from stopping if the terminal process that spawned it is closed, and the output is saved in nohup.out. The tail program outputs the contents of nohup.out to the screen for monitoring. 
```sh
cd /path/to/ssd/without/db && > nohup.out && nohup time python3 -u  /home/dominic/scripts/wikipedia-revisions-scraper/wikipedia_download.py --database --date 20200401 --low-storage --low-memory --delete-database & tail -f nohup.out 
```
- the `--low-memory` option more closely couples file reading and database I/O. It also limits the number of files actively processed to 3, which might be valuable if you are hitting your I/O constraints.
