#!/usr/bin/env python3

# icrisat_json_to_csv.py v0.0.3
#
# Copyright Alan Orth.
#
# SPDX-License-Identifier: GPL-3.0-only
#
# ---
#
# Helper script to parse the JSON file exported from ICRISAT EPrints.
#
# Tested on Python 3.12.

import csv
import json
import logging

from util import clean_string, normalize_doi

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")


def export_row(row):
    try:
        title = clean_string(row["title"])
    except KeyError:
        title = None

    authors = list()
    try:
        for author in row["creators"]:
            # The family or given name for this author may be blank
            try:
                family_name = author["name"]["family"]
                given_name = author["name"]["given"]
                author_name = f"{family_name}, {given_name}"

                if author_name not in authors:
                    authors.append(author_name)
            except KeyError:
                continue
    except KeyError:
        authors = None

    affiliations = list()
    try:
        for affiliation in row["affiliation"]:
            if affiliation not in affiliations:
                affiliations.append(affiliation)
    except KeyError:
        affiliations = ""

    funders = list()
    try:
        for funder in row["funders"]:
            if funder not in funders:
                funders.append(funder)
    except KeyError:
        funders = ""

    try:
        abstract = row["abstract"]
    except KeyError:
        abstract = None

    try:
        # Make sure this is something like a DOI or a URL...
        if "http" in row["id_number"] or "10." in row["id_number"]:
            doi = normalize_doi(row["id_number"])
        else:
            doi = None
    except KeyError:
        doi = None

    # Try to capture the official_url as the DOI if we don't have a DOI already.
    # We have to try/except here because not all items have official_url.
    try:
        if not doi and len(row["official_url"]) > 0:
            doi = normalize_doi(row["official_url"])
    except KeyError:
        pass

    # Repository URI
    try:
        uri = row["uri"]
    except KeyError:
        uri = None

    try:
        date = row["date"]
    except KeyError:
        date = None

    try:
        publication = row["publication"]
    except KeyError:
        publication = None

    try:
        issn = row["issn"]
    except KeyError:
        issn = None

    try:
        publisher = row["publisher"]
    except KeyError:
        publisher = None

    # Extract keywords as subjects for now. In EPrints they are apparently one
    # long string, and I see a lot of "\r\n" and whitespace in them so we need
    # to split and clean them.
    subjects = list()
    try:
        # Oh my gosh, there are keyword strings separating multiple values with
        # semi-colons!
        row["keywords"] = row["keywords"].replace(";", ",")

        for subject in row["keywords"].lower().split(","):
            if clean_string(subject) not in subjects:
                subjects.append(clean_string(subject))
    except KeyError:
        # Use an empty list here so we can append later
        subjects = list()

    try:
        extent = row["pagerange"]
    except KeyError:
        extent = ""

    # Append "climate change" to subjects if we see the corresponding code.
    # I want to make sure this is here so that we don't wonder why an item
    # has matched later
    if "s2.8" in row["subjects"] and "climate change" not in subjects:
        if len(subjects) > 0:
            subjects.append("climate change")
        else:
            subjects.append("climate change")

    writer.writerow(
        {
            "Title": title,
            "Authors": "; ".join(authors),
            "Author affiliations": "; ".join(affiliations),
            "Abstract": abstract,
            "DOI": doi,
            "Repository link": uri,
            "Publication date": date,
            "Journal": publication,
            "ISSN": issn,
            "Publisher": publisher,
            "Subjects": "; ".join(subjects),
            "Funders": "; ".join(funders),
            "Pages": extent,
        }
    )


input_file = "/tmp/icrisat.json"
output_file = "data/icrisat-filtered.csv"

with open(input_file, "r") as f:
    icrisat_json = json.load(f)
    logger.info(f"Opened {input_file}")

# Open the output file and keep it open so we can use the CSV writer in
# the export_row function
output_file_handle = open(output_file, "w")

fieldnames = [
    "Title",
    "Authors",
    "Author affiliations",
    "Abstract",
    "DOI",
    "Repository link",
    "Publication date",
    "Journal",
    "ISSN",
    "Publisher",
    "Subjects",
    "Funders",
    "Pages",
]
writer = csv.DictWriter(output_file_handle, fieldnames=fieldnames)
writer.writeheader()
logger.info(f"Wrote {output_file}")

# Iterate over rows looking for matches of "climate change" in the title and
# subjects
for row in icrisat_json:
    try:
        if "climate change" in row["title"].lower():
            export_row(row)
            continue
    except KeyError:
        pass

    try:
        # s2.8 is the code for "climate change" in the controlled subjects
        if "s2.8" in row["subjects"]:
            export_row(row)
            continue
    except KeyError:
        pass

    try:
        if "climate change" in row["keywords"].lower():
            export_row(row)
            continue
    except KeyError:
        pass

    try:
        if "climate change" in row["abstract"].lower():
            export_row(row)
            continue
    except KeyError:
        pass

output_file_handle.close()
