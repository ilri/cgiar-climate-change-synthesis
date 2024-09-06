#!/usr/bin/env python3
#
# SPDX-License-Identifier: GPL-3.0-only
#

import csv
import logging
import re
import sys
from datetime import timedelta

from requests_cache import CachedSession
from tqdm import tqdm

from util import clean_string, normalize_doi

# Create a local logger instance for this module. We don't do any configuration
# because this module might be used elsewhere that will have its own logging
# configuration.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")

session = CachedSession(
    "requests-cache", expire_after=timedelta(days=30), allowable_codes=(200, 404)
)

# prune old cache entries
session.cache.delete(expired=True)

# IFPRI CONTENTdm API base URL
url = "https://ebrary.ifpri.org/digital/bl/dmwebservices/index.php"

# Query syntax is:
#
#   dmQuery/alias/searchstrings/fields/sortby/maxrecs/start/suppress/docptr/suggest/facets/showunpub/denormalizeFacets/format
#
# I'm only using a basic query here because I find the API strange. For example,
# we can only use six search strings, it is not possible to search date ranges,
# and we can only return five metadata fields in the response. Instead, I will
# get the pointers for each result and fetch the items directly.
#
# See: https://help.oclc.org/Metadata_Services/CONTENTdm/Advanced_website_customization/API_Reference/CONTENTdm_API/CONTENTdm_Server_API_Functions_dmwebservices
params = {
    "q": "dmQuery/p15738coll5/^climate+change^all^and!type^journal+article^all^and//title/0/1/0/0/0/0/json"
}
r = session.get(url, params=params)

# Find out how many records matched and get the first ten
if r.ok:
    pager_start = int(r.json()["pager"]["start"])
    pager_total = int(r.json()["pager"]["total"])

    record_pointers = [record["pointer"] for record in r.json()["records"]]
else:
    sys.exit()

while True:
    # Get the next ten records
    pager_start = pager_start + 10
    params = {
        "q": f"dmQuery/p15738coll5/^climate+change^all^and!type^journal+article^all^and//title/0/{pager_start}/0/0/0/0/json"
    }
    r = session.get(url, params=params)

    # Parse this page of results
    if r.ok:
        for record in r.json()["records"]:
            record_pointers.append(record["pointer"])

    if len(r.json()["records"]) < 10:
        break

fieldnames = [
    "Title",
    "Authors",
    "Abstract",
    "Funder",
    "Language",
    "DOI",
    "Access rights",
    "Usage rights",
    "Repository link",
    "Publication date",
    "Journal",
    "ISSN",
    "Publisher",
    "Pages",
    "Subjects",
    "Type",
]

with open("/tmp/ifpri.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for record_pointer in tqdm(record_pointers, desc="Harvesting"):
        params = {"q": f"dmGetItemInfo/p15738coll5/{record_pointer}/json"}
        r = session.get(url, params=params)

        if r.ok:
            record = r.json()
        else:
            continue

        # All items should have titles right?
        item_title = clean_string(record["title"])

        item_authors = record["creato"]

        if isinstance(record["fundin"], str):
            item_funders = re.sub(
                r"http://dx.doi.org/10.13039/\d+ ", "", record["fundin"]
            )
        else:
            item_funders = ""

        if isinstance(record["descri"], str):
            item_abstract = record["descri"]
        else:
            item_abstract = ""

        # In CONTENTdm "doi" is the DOI URI and "doia" is the standalone DOI
        if isinstance(record["doia"], str):
            # Fix wrong DOI here: https://ebrary.ifpri.org/digital/collection/p15738coll5/id/4288
            if record["doia"] == "00000034/00000004/art00015":
                item_doi = "10.1177/156482651303400415"
            else:
                item_doi = normalize_doi(record["doia"])
        else:
            item_doi = ""

        if isinstance(record["access"], str):
            item_access_rights = record["access"]
        else:
            item_access_rights = ""

        if isinstance(record["cclice"], str):
            item_usage_rights = record["cclice"]
        else:
            item_usage_rights = ""

        item_handle = f"https://ebrary.ifpri.org/digital/collection/p15738coll5/id/{record_pointer}"

        item_date_issued = record["date"]

        if isinstance(record["series"], str):
            item_journal = clean_string(record["series"])
        else:
            item_journal = ""

        if isinstance(record["issn"], str):
            item_issn = clean_string(record["issn"])
        else:
            item_issn = ""

        if isinstance(record["publis"], str):
            item_publisher = clean_string(record["publis"])
        else:
            item_publisher = ""

        # IFPRI doesn't separate pages
        if isinstance(record["source"], str):
            item_extent = clean_string(record["source"])
        else:
            item_extent = ""

        # Lazily combine all subjects. CONTENTdm seems to have a mix of strings
        # and dicts, and some with extra semicolons...
        if isinstance(record["loc"], dict):
            item_subjects = "; ".join(record["loc"])
        else:
            item_subjects = record["loc"].rstrip("; ")

        if isinstance(record["subjea"], dict):
            item_subjects = item_subjects + "; ".join(record["subjea"])
        else:
            item_subjects = item_subjects + "; " + record["subjea"]

        item_language = record["langua"]

        item_type = record["type"]

        writer.writerow(
            {
                "Title": item_title,
                "Authors": item_authors,
                "Abstract": item_abstract,
                "Language": item_language,
                "DOI": item_doi,
                "Access rights": item_access_rights,
                "Usage rights": item_usage_rights,
                "Repository link": item_handle,
                "Publication date": item_date_issued,
                "Journal": item_journal,
                "ISSN": item_issn,
                "Publisher": item_publisher,
                "Pages": item_extent,
                "Funder": item_funders,
                "Subjects": item_subjects.rstrip("; ").lower(),
                "Type": item_type,
            }
        )

logger.info("Wrote /tmp/ifpri.csv")
