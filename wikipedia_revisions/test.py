import os
import functools
import bz2

import wikipedia_revisions
import wikipedia_revisions.download as download


def populate_config():
    date = "20200601"
    wikipedia_revisions.config["date"] = date
    wikipedia_revisions.config[
        "dump_page_url"
    ] = f"https://dumps.wikimedia.org/enwiki/{date}/"
    wikipedia_revisions.config[
        "md5_hashes_url"
    ] = f"https://dumps.wikimedia.org/enwiki/{date}/enwiki-{date}-md5sums.txt"
    wikipedia_revisions.config["low_storage"] = True
    wikipedia_revisions.config["database_url"] = "postgres:///wikipedia_revisions"
    wikipedia_revisions.config["low_memory"] = True
    wikipedia_revisions.config["delete_database"] = True
    wikipedia_revisions.config["concurrent_reads"] = 4
    wikipedia_revisions.config["insert_multiple_values"] = True
    wikipedia_revisions.config["num_db_connections"] = 4
    wikipedia_revisions.config["backlog"] = 300
    wikipedia_revisions.config["pipe_dir"] = "/pipes"


def write_test_xml(tmp_path) -> str:
    compressed_xml_path = functools.reduce(os.path.join, [tmp_path, "test.xml.bz2"])
    with bz2.open(compressed_xml_path, mode="wt") as f:
        f.write(
            """
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.35.0-wmf.34</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <namespace key="3" case="first-letter">User talk</namespace>
      <namespace key="4" case="first-letter">Wikipedia</namespace>
      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
      <namespace key="6" case="first-letter">File</namespace>
      <namespace key="7" case="first-letter">File talk</namespace>
      <namespace key="8" case="first-letter">MediaWiki</namespace>
      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
      <namespace key="10" case="first-letter">Template</namespace>
      <namespace key="11" case="first-letter">Template talk</namespace>
      <namespace key="12" case="first-letter">Help</namespace>
      <namespace key="13" case="first-letter">Help talk</namespace>
      <namespace key="14" case="first-letter">Category</namespace>
      <namespace key="15" case="first-letter">Category talk</namespace>
      <namespace key="100" case="first-letter">Portal</namespace>
      <namespace key="101" case="first-letter">Portal talk</namespace>
      <namespace key="108" case="first-letter">Book</namespace>
      <namespace key="109" case="first-letter">Book talk</namespace>
      <namespace key="118" case="first-letter">Draft</namespace>
      <namespace key="119" case="first-letter">Draft talk</namespace>
      <namespace key="446" case="first-letter">Education Program</namespace>
      <namespace key="447" case="first-letter">Education Program talk</namespace>
      <namespace key="710" case="first-letter">TimedText</namespace>
      <namespace key="711" case="first-letter">TimedText talk</namespace>
      <namespace key="828" case="first-letter">Module</namespace>
      <namespace key="829" case="first-letter">Module talk</namespace>
      <namespace key="2300" case="first-letter">Gadget</namespace>
      <namespace key="2301" case="first-letter">Gadget talk</namespace>
      <namespace key="2302" case="case-sensitive">Gadget definition</namespace>
      <namespace key="2303" case="case-sensitive">Gadget definition talk</namespace>
    </namespaces>
  </siteinfo>
  <page>
    <title>nice</title>
    <ns>1</ns>
    <id>1</id>
    <redirect title="Talk:Chroniques du pays des mères" />
    <revision>
      <id>1</id>
      <timestamp>2017-03-19T04:23:23Z</timestamp>
      <contributor>
        <ip>192.168.0.1</ip>
      </contributor>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes="65" xml:space="preserve">hi</text>
      <sha1>8pfm5bgclg41gvdiv2ro69rsin2xpku</sha1>
    </revision>
    <revision>
      <id>2</id>
      <parentid>1</parentid>
      <timestamp>2017-03-19T04:24:23Z</timestamp>
      <contributor>
        <username>person</username>
        <id>1</id>
      </contributor>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes="65" xml:space="preserve">hi hi</text>
      <sha1>8pfm5bgclg41gvdiv2ro69rsin2xpku</sha1>
    </revision>
  </page>
  <page>
    <title>also nice</title>
    <ns>2</ns>
    <id>2</id>
    <redirect title="Talk:Chroniques du pays des mères" />
    <revision>
      <id>3</id>
      <timestamp>2017-03-19T04:25:23Z</timestamp>
      <contributor>
        <username>another_person</username>
        <id>2</id>
      </contributor>
      <comment>sometimes there are comments</comment>
      <text bytes="65" xml:space="preserve"></text>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <sha1>8pfm5bgclg41gvdiv2ro69rsin2xpku</sha1>
    </revision>
  </page>
</mediawiki>
        """
        )

    print("fsldkfj")
    return compressed_xml_path


def test_parse_one_file(tmp_path):
    # generate sample data
    populate_config()
    compressed_xml_path = write_test_xml(tmp_path)

    # test parsing
    revisions_generator = download.parse_one_file(compressed_xml_path)
    assert list(revisions_generator) == [
        {
            "id": "1",
            "parent_id": None,
            "timestamp": "2017-03-19T04:23:23Z",
            "page_id": "1",
            "page_title": "nice",
            "page_ns": "1",
            "contributor_id": None,
            "contributor_name": None,
            "contributor_ip": "192.168.0.1",
            "comment": None,
            "text": "hi",
        },
        {
            "id": "2",
            "parent_id": "1",
            "timestamp": "2017-03-19T04:24:23Z",
            "page_id": "1",
            "page_title": "nice",
            "page_ns": "1",
            "contributor_id": "1",
            "contributor_name": "person",
            "contributor_ip": None,
            "comment": None,
            "text": "hi hi",
        },
        {
            "id": "3",
            "parent_id": None,
            "timestamp": "2017-03-19T04:25:23Z",
            "page_id": "2",
            "page_title": "also nice",
            "page_ns": "2",
            "contributor_id": "2",
            "contributor_name": "another_person",
            "contributor_ip": None,
            "comment": "sometimes there are comments",
            "text": None,
        },
    ]
