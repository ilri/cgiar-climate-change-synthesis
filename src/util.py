#
# Copyright Alan Orth.
#
# SPDX-License-Identifier: GPL-3.0-only
#
# ---
#
# Various helper functions for Python scripts.
#

import gzip
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timedelta

import country_converter as coco
import pandas as pd
from requests_cache import CachedSession

# Create a local logger instance for this module. We don't do any configuration
# because this module might be used elsewhere that will have its own logging
# configuration.
logger = logging.getLogger(__name__)

session = CachedSession(
    "util-cache", expire_after=timedelta(days=30), allowable_codes=(200, 404)
)
# prune old cache entries
session.cache.delete(expired=True)


def get_access_rights(doi: str):
    access_rights = pd.NA

    if pd.isna(doi):
        return access_rights

    if doi.startswith("https://doi.org/10."):
        # Opportunistically use an email address from the environment to make
        # sure we get better access to the API.
        try:
            request_params = {"email": os.environ["EMAIL"]}
        except KeyError:
            request_params = {}

        url = f"https://api.unpaywall.org/v2/{doi}"

        r = session.get(url, params=request_params)
    else:
        return access_rights

    if not r.ok:
        return access_rights

    data = r.json()

    if data["is_oa"]:
        match data["oa_status"]:
            case "gold":
                access_rights = "Gold Open Access"
            case "green":
                access_rights = "Green Open Access"
            case "hybrid":
                access_rights = "Hybrid Open Access"
            case "bronze":
                access_rights = "Bronze Open Access"
            case _:
                access_rights = "Open Access"
    else:
        access_rights = "Limited Access"

    return access_rights


def get_license(doi: str):
    license = pd.NA

    if pd.isna(doi):
        return license

    if doi.startswith("https://doi.org/10."):
        # Opportunistically use an email address from the environment to make
        # sure we get better access to the API.
        try:
            request_params = {"mailto": os.environ["EMAIL"]}
        except KeyError:
            request_params = {}

        url = f"https://api.crossref.org/works/{doi}"

        r = session.get(url, params=request_params)
    else:
        return license

    # HTTP 404 here means the DOI is not registered at Crossref
    if not r.ok:
        return license

    data = r.json()

    # Extract license strings from Crossref in the order we prefer them
    doi_licenses = {}
    try:
        for doi_license in data["message"]["license"]:
            content_version = doi_license["content-version"]
            doi_licenses[content_version] = doi_license["URL"]

        if "am" in doi_licenses:
            license = doi_licenses["am"]
        elif "vor" in doi_licenses:
            license = doi_licenses["vor"]
        elif "tdm" in doi_licenses:
            license = doi_licenses["tdm"]
        else:
            license = doi_licenses["unspecified"]

        # Try to parse various license versions
        if "creativecommons.org" in license:
            if "publicdomain/zero/1.0" in license:
                license = "CC0-1.0"
            else:
                license = license.replace("/legalcode", "")
                license = license.replace("/deed.en_GB", "")
                # Remove trailing slash so we can split on slashes reliably
                license = license.rstrip("/")

                # Special handling for IGO corner case
                if not "igo" in license:
                    # Split on slash and get last two matches from the end
                    license, version = license.split("/")[-2:]
                    # Put it all together
                    license = f"CC-{license}-{version}"
                else:
                    license, version, variation = license.split("/")[-3:]
                    # Put it all together
                    license = f"CC-{license}-{version}-{variation}"

                license = license.upper()
    except KeyError:
        pass

    # Infer copyright for some big publishers. By inspection, this is going to
    # be correct for the majority, but will be incorrect for some corner cases.
    if pd.notna(license):
        license = license.replace(
            "https://www.elsevier.com/tdm/userlicense/1.0/",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "http://www.springer.com/tdm", "Copyrighted; all rights reserved"
        )
        license = license.replace(
            "http://onlinelibrary.wiley.com/termsAndConditions#vor",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "http://www.elsevier.com/open-access/userlicense/1.0/",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "https://www.springer.com/tdm", "Copyrighted; all rights reserved"
        )
        license = license.replace(
            "https://www.springernature.com/gp/researchers/text-and-data-mining",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "https://www.cambridge.org/core/terms", "Copyrighted; all rights reserved"
        )
        license = license.replace(
            "https://academic.oup.com/pages/standard-publication-reuse-rights",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "https://www.elsevier.com/legal/tdmrep-license",
            "Copyrighted; all rights reserved",
        )
        license = license.replace(
            "http://doi.wiley.com/10.1002/tdm_license_1.1",
            "Copyrighted; all rights reserved",
        )

    # Reset some licenses back to pd.NA since we can't determine, and hopefully
    # we can fill in the missing information from repository metadata.
    if pd.notna(license) and "http" in license:
        license = pd.NA

    return license


def pdf_exists(doi: str):
    if pd.isna(doi):
        return pd.NA

    # Strip URI prefix
    doi = doi.replace("https://doi.org/", "")

    doi_pdf_file = f'{doi.replace("/", "-")}.pdf'
    pdf_path = f"data/pdf/{doi_pdf_file}"

    # Check if we have the PDF for this DOI locally
    if os.path.isfile(pdf_path):
        return doi_pdf_file
    else:
        return pd.NA


# Try to see which DSpace version this is
def detect_dspace_version(dspace_root: str) -> str:
    # Maybe it's DSpace 7.x
    r = session.get(f"{dspace_root}/server/api", headers={"Accept": "application/json"})
    if r.ok:
        try:
            # Could be 7.6 or 7.6.2, etc
            dspace_version = re.search(
                r"7\.\d+(\.\d+)?", r.json()["dspaceVersion"]
            ).group(0)

            return dspace_version
        except KeyError:
            pass

    # Maybe it's DSpace 6.x and we can get the version from the REST API?
    r = session.get(
        f"{dspace_root}/rest/status", headers={"Accept": "application/json"}
    )
    if r.ok:
        try:
            dspace_version = r.json()["sourceVersion"]

            return dspace_version
        except KeyError:
            pass

    # Nope! Guess we have to parse the HTML Generator meta tag
    r = session.get(dspace_root)

    if r.ok:
        # Search for the Generator meta tag, which could be something like:
        #   <meta name="Generator" content="DSpace 5.4">
        #   <meta name="Generator" content="CIFOR-DSpace 5.10">
        generator = re.findall(
            r'<meta name="Generator" content=".*?DSpace 5.\d+">', r.text
        )[0]

        dspace_version = re.search(r"5\.\d+", generator).group(0)

        return dspace_version
    else:
        logger.error(f"Cannot detect DSpace version")

        sys.exit(1)


# Deduplicate subject string by splitting on "; " and re-building in a set. This
# is guaranteed to be unique in Python and should preserve the order as well.
def deduplicate_subjects(subjects: str) -> str:
    if pd.isna(subjects):
        return pd.NA

    seen = set()
    deduped_list = [
        x for x in subjects.split("; ") if x not in seen and not seen.add(x)
    ]

    return "; ".join(deduped_list)


# Determine the publication date as the earlier of the issue date and the online
# date. This is what Crossref does and allows us to have one "Publication date".
# For this to work we need to assume every item has *at least* one of the issue
# or online dates, and they are in YYYY, YYYY-MM, or YYYY-MM-DD format.
def get_publication_date(row: pd.Series) -> str:
    if pd.isna(row["Publication date"]):
        issue_date_dt = False
    else:
        issue_date = row["Publication date"]
        issue_date_dt = False

        # Split the item date on "-" to see what format we need to
        # use to create the datetime object.
        if len(issue_date.split("-")) == 1:
            issue_date_dt = datetime.strptime(issue_date, "%Y")
        elif len(issue_date.split("-")) == 2:
            issue_date_dt = datetime.strptime(issue_date, "%Y-%m")
        elif len(issue_date.split("-")) == 3:
            issue_date_dt = datetime.strptime(issue_date, "%Y-%m-%d")

    if pd.isna(row["Publication date (Online)"]):
        online_date_dt = False
    else:
        online_date = row["Publication date (Online)"]
        online_date_dt = False

        if len(online_date.split("-")) == 1:
            online_date_dt = datetime.strptime(online_date, "%Y")
        elif len(online_date.split("-")) == 2:
            online_date_dt = datetime.strptime(online_date, "%Y-%m")
        elif len(online_date.split("-")) == 3:
            online_date_dt = datetime.strptime(online_date, "%Y-%m-%d")

    if issue_date_dt and online_date_dt:
        if issue_date_dt < online_date_dt:
            publication_date = issue_date
        # Always return the issue date if online date is in 2011 since our inc-
        # lusion criteria is 2012–2023 and this could be misleading.
        elif online_date.startswith("2011"):
            publication_date = issue_date
        else:
            publication_date = online_date
    elif issue_date_dt and not online_date_dt:
        publication_date = issue_date
    elif online_date_dt and not issue_date_dt:
        publication_date = online_date

    return publication_date


def clean_string(string):
    """
    Clean a string, as I saw some titles and subjects with newlines in them.
    We can't be sure if it is a CR, LF, CRLF, etc, so let's replace both in
    separate passes and then trim the double space if need be.
    """

    string = string.replace("\n", " ")
    string = string.replace("\r", " ")
    string = string.replace("  ", " ")

    return string.strip()


def normalize_doi(doi):
    """
    Try to normalize a DOI based on some cases I noticed. Return a clean
    DOI in https://doi.org/10. format, lowercased, and stripped.
    """

    if pd.isna(doi):
        return pd.NA

    # normalize DOIs like doi:10.1088/1748-9326/ac413a
    doi = doi.replace("doi:", "")

    # fix typo in DOIs like 0.1002/2014WR016668
    if doi.startswith("0."):
        doi = f"1{doi}"

    # fix typo in DOIs like http://dx.doi.org/DOI:
    doi = doi.replace("http://dx.doi.org/DOI:", "")

    # fix old dx.doi.org
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)

    # fix typo in DOI URI like https:// doi.org/10.3390/agronomy13030727
    doi = doi.replace("https:// doi.org/", "")

    # fix URLs that should be DOIs like https://www.tandfonline.com/doi/full/10.1080/23322039.2019.1640098
    doi = doi.replace("https://www.tandfonline.com/doi/full/", "")

    # fix Unicode non-printing characters like in 10.​1007/​s10113-016-0983-6
    pattern = re.compile(r"\u200B")
    match = re.findall(pattern, doi)

    if match:
        doi = re.sub(pattern, "", doi)

    # return the normalized DOI, and strip it just in case
    return f"https://doi.org/{doi.lower().strip()}"


def normalize_countries(countries):
    """
    Try to normalize country names to official names.

    Note: this is slower than country_converter's built-in pandas_convert(), but
    I can't figure out how to deal with our series mixing strings and lists.
    """
    if pd.isna(countries):
        return pd.NA

    # Don't print "Tibet not found in regex" etc
    coco_logger = coco.logging.getLogger()
    coco_logger.setLevel(logging.CRITICAL)

    # Convert to common short names
    countries_standardized = coco.convert(names=countries.split("; "), to="name_short")

    # Reset log level
    coco_logger.setLevel(logger.level)

    if isinstance(countries_standardized, str):
        if countries_standardized == "not found":
            return pd.NA
        else:
            return countries_standardized
    else:
        countries_standardized = [
            country for country in countries_standardized if country != "not found"
        ]

        return "; ".join(countries_standardized)


# Filter our abstracts so we don't accidentally distribute copyrighted material.
# The exceptions here are if the work is licensed creative commons, or if the
# abstract has been deposited by the publisher in Crossref, since they have an
# agreement allowing you to redistribute them.
#
# See: https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-metadata-license-information/
def filter_abstracts(row: pd.Series) -> str:
    # If there's no abstract we can return immediately
    if pd.isna(row["Abstract"]):
        return pd.NA

    # If the work is Creative Commons we can return immediately
    if pd.notna(row["Usage rights"]) and "CC-" in row["Usage rights"]:
        return row["Abstract"]

    # Check if the abstract is on Crossref
    try:
        request_params = {"mailto": os.environ["EMAIL"]}
    except KeyError:
        request_params = {}

    url = f"https://api.crossref.org/works/{row['DOI']}"

    r = session.get(url, params=request_params)

    # HTTP 404 here means the DOI is not registered at Crossref
    if not r.ok:
        return pd.NA

    data = r.json()

    try:
        data["message"]["abstract"]
        return row["Abstract"]
    except KeyError:
        pass

    return pd.NA
