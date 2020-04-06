# wikipedia-revisions
download every wikipedia edit

[![status](https://github.com/DominicBurkart/wikipedia-revisions/workflows/Python%20application/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions/actions?query=is%3Acompleted+branch%3Amaster)


This script downloads every revision to wikipedia and outputs them into a single bz2-zipped csv file.

Revisions are output with the following fields:
- "id": the revision id, numeric
- "parent": parent revision id (if it exists), numeric
- "timestamp": timestamp with timezone
- "text": complete article text after the revision is applied, in wikipedia markdown

System requirements:
- 4gb memory
- ~3tb storage
- python 3 & pip pre-installed

I wrote a [blog post](https://dominicburkart.com/blog/2020/big_data_and_small_computers.html) on some of the project 
goals and technical choices.


Example use:
```sh 
git clone https://github.com/DominicBurkart/wikipedia-revisions
cd wikipedia-revisions
pip3 install -r requirements.txt
python3 wikipedia_download.py
```

You can also pass some parameters to the script, visible on its --help:
```sh
python3 wikipedia_download.py --help
```