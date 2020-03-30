# wikipedia-revisions
download every wikipedia edit

[![status](https://github.com/DominicBurkart/wikipedia-revisions/workflows/Python%20application/badge.svg)](https://github.com/DominicBurkart/wikipedia-revisions/actions?query=is%3Acompleted+branch%3Amaster)


This script downloads every revision to wikipedia. By default, it outputs each changed sentence (or continuous group of sentences) from each edit in the wikipedia revision history as a bz2-zipped csv file. Further documentation forthcoming.

System requirements:
- 4gb memory
- ~3tb storage
- python 3 & pip pre-installed

I wrote a [blog post](https://dominicburkart.com/blog/2020/big_data_and_small_computers.html) on some of the project 
goals and technical choices.

example use:
```sh 
git clone https://github.com/DominicBurkart/wikipedia-revisions
cd wikipedia-revisions
pip3 install -r requirements.txt
python3 wikipedia_download.py
```

