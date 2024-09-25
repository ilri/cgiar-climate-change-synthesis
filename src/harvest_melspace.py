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

url = "https://repo.mel.cgiar.org/server/api/discover/search/objects"
params = {
    "query": '(dcterms.issued:[2012 TO 2023] OR dcterms.available:[2012 TO 2023]) AND dc.type:"Journal Article" AND (dc.title:"climate change" OR dc.subject:"climate change" OR cg.subject.agrovoc:"climate change" OR dc.description.abstract:"climate change") AND dc.language:en'
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
    "Author affiliations",
    "Abstract",
    "Funders",
    "Language",
    "DOI",
    "Access rights",
    "Usage rights",
    "Repository link",
    "Publication date",
    "Publication date (Online)",
    "Journal",
    "ISSN",
    "Publisher",
    "Volume",
    "Issue",
    "Pages",
    "Subjects",
]

with open("/tmp/melspace.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for item in items:
        # All items should have titles right?
        item_title = item["metadata"]["dc.title"][0]["value"]

        # Concatenate authors. MELSpace uses dc.creator as the first author, and
        # dc.contributor for the other authors.
        item_authors = []
        try:
            for item_author in item["metadata"]["dc.creator"]:
                item_authors.append(item_author["value"])
        except KeyError:
            pass

        try:
            for item_author in item["metadata"]["dc.contributor"]:
                item_authors.append(item_author["value"])
        except KeyError:
            pass

        item_affiliations = []
        try:
            for item_affiliation in item["metadata"]["cg.contributor.center"]:
                item_affiliations.append(item_affiliation["value"])
        except KeyError:
            pass

        item_funders = []
        try:
            for item_funder in item["metadata"]["cg.contributor.funder"]:
                item_funders.append(item_funder["value"])
        except KeyError:
            pass

        try:
            item_abstract = item["metadata"]["dc.description.abstract"][0]["value"]
        except KeyError:
            item_abstract = ""

        try:
            item_doi = item["metadata"]["cg.identifier.doi"][0]["value"]
        except KeyError:
            item_doi = ""

        try:
            item_access_rights = item["metadata"]["dc.identifier.status"][0]["value"]
        except KeyError:
            item_access_rights = ""

        try:
            item_usage_rights = item["metadata"]["dc.rights"][0]["value"]
        except KeyError:
            item_usage_rights = ""

        try:
            item_handle = item["metadata"]["dc.identifier.uri"][0]["value"]
        except KeyError:
            item_handle = ""

        try:
            item_date_issued = item["metadata"]["dcterms.issued"][0]["value"]
        except KeyError:
            item_date_issued = ""

        try:
            item_date_online = item["metadata"]["dcterms.available"][0]["value"]
        except KeyError:
            item_date_online = ""

        try:
            item_journal = item["metadata"]["cg.journal"][0]["value"]
        except KeyError:
            item_journal = ""

        try:
            item_issn = item["metadata"]["cg.issn"][0]["value"]
        except KeyError:
            item_issn = ""

        try:
            item_publisher = item["metadata"]["dc.publisher"][0]["value"]
        except KeyError:
            item_publisher = ""

        try:
            item_volume = item["metadata"]["cg.volume"][0]["value"]
        except KeyError:
            item_volume = ""

        try:
            item_issue = item["metadata"]["cg.issue"][0]["value"]
        except KeyError:
            item_issue = ""

        try:
            item_extent = item["metadata"]["dcterms.extent"][0]["value"]
        except KeyError:
            item_extent = ""

        # Append all subjects, first AGROVOC, then other subjects
        item_subjects = []
        try:
            for item_subject in item["metadata"]["cg.subject.agrovoc"]:
                if item_subject["value"].lower() not in item_subjects:
                    item_subjects.append(item_subject["value"].lower())
        except KeyError:
            pass

        try:
            for item_subject in item["metadata"]["dc.subject"]:
                if item_subject["value"].lower() not in item_subjects:
                    item_subjects.append(item_subject["value"].lower())
        except KeyError:
            pass

        try:
            item_language = item["metadata"]["dc.language"][0]["value"]
        except KeyError:
            item_language = ""

        writer.writerow(
            {
                "Title": item_title,
                "Authors": "; ".join(item_authors),
                "Author affiliations": "; ".join(item_affiliations),
                "Abstract": item_abstract,
                "Language": item_language,
                "DOI": item_doi,
                "Access rights": item_access_rights,
                "Usage rights": item_usage_rights,
                "Repository link": item_handle,
                "Publication date": item_date_issued,
                "Publication date (Online)": item_date_online,
                "Journal": item_journal,
                "ISSN": item_issn,
                "Publisher": item_publisher,
                "Volume": item_volume,
                "Issue": item_issue,
                "Pages": item_extent,
                "Funders": "; ".join(item_funders),
                "Subjects": "; ".join(item_subjects),
            }
        )

logger.info("Wrote /tmp/melspace.csv")
