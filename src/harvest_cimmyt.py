#!/usr/bin/env python3
#
# SPDX-License-Identifier: GPL-3.0-only
#

import csv
import logging
from datetime import timedelta

from requests_cache import CachedSession

# Create a local logger instance for this module. We don't do any configuration
# because this module might be used elsewhere that will have its own logging
# configuration.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")

session = CachedSession(
    "harvest-cache", expire_after=timedelta(days=30), allowable_codes=(200, 404)
)

# prune old cache entries
session.cache.delete(expired=True)


url = "https://repository.cimmyt.org/server/api/discover/search/objects"
params = {
    "query": 'dc.date.issued:[2012 TO 2023] AND dc.type:Article AND (dc.title:"climate change" OR dc.subject.agrovoc:"climate change" OR dc.subject.keywords:"climate change" OR dc.description:"climate change" OR dc.description.abstract:"climate change" OR dcterms.description:"climate change") AND (dcterms.language:English OR dc.language:English)'
}
r = session.get(url, params=params)

# Create empty list for items
items = []

if r.ok:
    for item in r.json()["_embedded"]["searchResult"]["_embedded"]["objects"]:
        items.append(item["_embedded"]["indexableObject"])
else:
    exit

# Get link to next page of results
url = r.json()["_embedded"]["searchResult"]["_links"]["next"]["href"]

while True:
    r = session.get(url)

    # Parse this page of results
    if r.ok:
        for item in r.json()["_embedded"]["searchResult"]["_embedded"]["objects"]:
            items.append(item["_embedded"]["indexableObject"])

    # Try to set the URL for the next page
    try:
        url = r.json()["_embedded"]["searchResult"]["_links"]["next"]["href"]
    except Exception:
        break

fieldnames = [
    "Title",
    "Authors",
    "Abstract",
    "Funders",
    "Language",
    "DOI",
    "Repository link",
    "Publication date",
    "Journal",
    "ISSN",
    "Publisher",
    "Pages",
    "Subjects",
    "Countries",
]

with open("/tmp/cimmyt.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for item in items:
        # All items should have titles right?
        item_title = item["metadata"]["dc.title"][0]["value"]

        item_authors = []
        try:
            for item_author in item["metadata"]["dc.creator"]:
                item_authors.append(item_author["value"])
        except KeyError:
            pass

        item_funders = []
        try:
            for item_funder in item["metadata"]["dc.relation.funderName"]:
                item_funders.append(item_funder["value"])
        except KeyError:
            pass

        # Get abstracts in order of most likelihood in CIMMYT's repository:
        # dc.description: 1512
        # dc.description.abstract: 1261
        # dcterms.description: 844
        try:
            item_abstract = item["metadata"]["dc.description"][0]["value"]
        except KeyError:
            try:
                item_abstract = item["metadata"]["dc.description.abstract"][0]["value"]
            except KeyError:
                try:
                    item_abstract = item["metadata"]["dcterms.description"][0]["value"]
                except KeyError:
                    item_abstract = ""

        try:
            item_doi = item["metadata"]["dc.identifier.doi"][0]["value"]
        except KeyError:
            item_doi = ""

        try:
            item_handle = item["metadata"]["dc.identifier.uri"][0]["value"]

            # Normalize some URIs since CIMMYT seems to have mixed HTTP/HTTPS
            if "http://hdl.handle.net" in item_handle:
                item_handle = item_handle.replace(
                    "http://hdl.handle.net", "https://hdl.handle.net"
                )
        except KeyError:
            item_handle = ""

        try:
            item_date_issued = item["metadata"]["dc.date.issued"][0]["value"]
        except KeyError:
            item_date_issued = ""

        try:
            item_journal = item["metadata"]["dc.source.journal"][0]["value"]
        except KeyError:
            item_journal = ""

        try:
            item_issn = item["metadata"]["dc.source.issn"][0]["value"]
        except KeyError:
            item_issn = ""

        try:
            item_publisher = item["metadata"]["dc.publisher"][0]["value"]
        except KeyError:
            try:
                item_publisher = item["metadata"]["dcterms.publisher"][0]["value"]
            except KeyError:
                item_publisher = ""

        try:
            item_extent = item["metadata"]["dc.description.pages"][0]["value"]
        except KeyError:
            item_extent = ""

        # Append all subjects
        item_subjects = []
        try:
            for item_subject in item["metadata"]["dc.subject.agrovoc"]:
                if item_subject["value"].lower() not in item_subjects:
                    item_subjects.append(item_subject["value"].lower())
        except KeyError:
            pass

        try:
            for item_subject in item["metadata"]["dc.subject.keywords"]:
                if item_subject["value"].lower() not in item_subjects:
                    item_subjects.append(item_subject["value"].lower())
        except KeyError:
            pass

        item_countries = []
        try:
            for item_country in item["metadata"]["dc.coverage.countryfocus"]:
                if item_country["value"] not in item_countries:
                    item_countries.append(item_country["value"])
        except KeyError:
            pass

        # Get language in order of most likelihood in CIMMYT's repository. For
        # Articles it seems they mostly use dc.language:
        # dc.language: 2234
        # dcterms.language: 870
        try:
            item_language = item["metadata"]["dc.language"][0]["value"]
        except KeyError:
            try:
                item_language = item["metadata"]["dcterms.language"][0]["value"]
            except KeyError:
                item_language = ""

        writer.writerow(
            {
                "Title": item_title,
                "Authors": "; ".join(item_authors),
                "Abstract": item_abstract,
                "Language": item_language,
                "DOI": item_doi,
                "Repository link": item_handle,
                "Publication date": item_date_issued,
                "Journal": item_journal,
                "ISSN": item_issn,
                "Publisher": item_publisher,
                "Pages": item_extent,
                "Funders": "; ".join(item_funders),
                "Subjects": "; ".join(item_subjects),
                "Countries": "; ".join(item_countries),
            }
        )

logger.info("Wrote /tmp/cimmyt.csv")
