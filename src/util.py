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
import pyalex
import requests
import requests_cache

# Create a local logger instance for this module. We don't do any configuration
# because this module might be used elsewhere that will have its own logging
# configuration.
logger = logging.getLogger(__name__)

# We must use the monkey-patching method of requests_cache instead of the more
# clean CachedSession because pyalex can't use the session manager.
requests_cache.install_cache(
    "util-cache", expire_after=timedelta(days=30), allowable_codes=(200, 404)
)

requests_cache.delete(expired=True)

cc = coco.CountryConverter()


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

        r = requests.get(url, params=request_params)
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

        r = requests.get(url, params=request_params)
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
    r = requests.get(
        f"{dspace_root}/server/api", headers={"Accept": "application/json"}
    )
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
    r = requests.get(
        f"{dspace_root}/rest/status", headers={"Accept": "application/json"}
    )
    if r.ok:
        try:
            dspace_version = r.json()["sourceVersion"]

            return dspace_version
        except KeyError:
            pass

    # Nope! Guess we have to parse the HTML Generator meta tag
    r = requests.get(dspace_root)

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
    Try to normalize country names to common short names.
    """
    if pd.isna(countries):
        return pd.NA

    # Don't print "Tibet not found in regex" etc
    coco_logger = coco.logging.getLogger()
    coco_logger.setLevel(logging.CRITICAL)

    # Convert to common short names (using a Pandas Series is 4000x faster)
    countries_standardized = cc.pandas_convert(
        series=pd.Series(countries.split("; ")), to="name_short"
    )

    # Reset log level
    coco_logger.setLevel(logger.level)

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

    r = requests.get(url, params=request_params)

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


def add_regions(countries):
    """
    Add regions for a list of countries.
    """
    if pd.isna(countries):
        return pd.NA

    # Don't print "Tibet not found in regex" etc
    coco_logger = coco.logging.getLogger()
    coco_logger.setLevel(logging.CRITICAL)

    # Convert countries to UN regions
    regions = cc.pandas_convert(series=pd.Series(countries.split("; ")), to="UNRegion")

    # Reset log level
    coco_logger.setLevel(logger.level)

    regions = [region for region in regions if region != "not found"]

    return "; ".join(regions)


def add_continents(countries):
    """
    Add continents for a list of countries.
    """
    if pd.isna(countries):
        return pd.NA

    # Don't print "Tibet not found in regex" etc
    coco_logger = coco.logging.getLogger()
    coco_logger.setLevel(logging.CRITICAL)

    # Convert to countries to continents
    continents = cc.pandas_convert(
        series=pd.Series(countries.split("; ")), to="Continent_7"
    )

    # Reset log level
    coco_logger.setLevel(logger.level)

    continents = [continent for continent in continents if continent != "not found"]

    return "; ".join(continents)


def retrieve_abstract_openalex(row: pd.Series) -> str:
    """
    Attempt to retrieve missing abstracts on OpenAlex.
    """
    # If there's already an abstract we can return immediately
    if pd.notna(row["Abstract"]):
        return row["Abstract"]

    try:
        pyalex.config.email = os.environ["EMAIL"]
    except KeyError:
        pass

    try:
        w = pyalex.Works()[row["DOI"]]
    except requests.exceptions.HTTPError:
        return pd.NA

    if not w["abstract"]:
        return pd.NA

    return w["abstract"]


def retrieve_publisher_crossref(row: pd.Series) -> str:
    """
    Attempt to retrieve missing publishers from Crossref.
    """
    # If there's already a publisher we can return immediately
    if pd.notna(row["Publisher"]):
        return row["Publisher"]

    # Check if the publisher is on Crossref
    try:
        request_params = {"mailto": os.environ["EMAIL"]}
    except KeyError:
        request_params = {}

    url = f"https://api.crossref.org/works/{row['DOI']}"

    r = requests.get(url, params=request_params)

    # HTTP 404 here means the DOI is not registered at Crossref
    if not r.ok:
        return pd.NA

    data = r.json()

    try:
        publisher = data["message"]["publisher"]
    except KeyError:
        return pd.NA

    return publisher


def retrieve_affiliations_openalex(row: pd.Series) -> str:
    """
    Attempt to retrieve missing affiliations from OpenAlex.
    """
    if pd.notna(row["Author affiliations"]):
        return row["Author affiliations"]

    try:
        pyalex.config.email = os.environ["EMAIL"]
    except KeyError:
        pass

    try:
        w = pyalex.Works()[row["DOI"]]
    except requests.exceptions.HTTPError:
        return pd.NA

    if not w["authorships"]:
        return pd.NA

    affiliations = list()

    for author in w["authorships"]:
        for affiliation in author["raw_affiliation_strings"]:
            affiliation = clean_string(affiliation)

            if affiliation not in affiliations:
                affiliations.append(affiliation)

    return "; ".join(affiliations)


def extract_missing_countries(row: pd.Series) -> str:
    """
    Attempt to extract missing countries from titles and abstracts.

    Note: this is very naive and unoptimized.
    """
    if pd.notna(row["Countries"]):
        return row["Countries"]

    # Combine title and abstract for the search space
    if pd.notna(row["Abstract"]):
        search_space = row["Title"] + row["Abstract"]
    else:
        search_space = row["Title"]

    countries = list()

    # Try short names first
    for country in cc.data.name_short.values:
        if country in search_space and country not in countries:
            countries.append(country)

    # Then try official names
    for country in cc.data.name_official.values:
        if country in search_space and country not in countries:
            countries.append(country)

    return "; ".join(countries)


def normalize_affiliations(affiliations):
    """
    Try to normalize affiliations. For now this is a manual list of replacements
    for CGIAR centers only.
    """
    if pd.isna(affiliations):
        return pd.NA

    affiliations_normalized = list()

    for affiliation in affiliations.split('; '):
        # Tease out CGIAR centers from the mess of affiliations
        affiliation = re.sub(r'^Africa Rice Center.+','Africa Rice Center', affiliation)
        affiliation = re.sub(r'^AfricaRice.+','Africa Rice Center', affiliation)

        affiliation = re.sub(r'^Alliance of Bioversity International and.+','Alliance of Bioversity International and CIAT', affiliation)

        affiliation = re.sub(r'^Bioversity International.+','Bioversity International', affiliation)

        affiliation = re.sub(r'^Cent(er|re) for International Forestry Research.+','Center for International Forestry Research', affiliation)
        affiliation = re.sub(r'^CIFOR.+','Center for International Forestry Research', affiliation)

        affiliation = re.sub(r'^International Cent(er|re) for Agricultural Research in the Dry Areas.+','International Center for Agricultural Research in the Dry Areas', affiliation)
        affiliation = re.sub(r'^ICARDA.+','International Center for Agricultural Research in the Dry Areas', affiliation)

        affiliation = re.sub(r'^International Cent(er|re) for Tropical Agriculture.+','International Center for Tropical Agriculture', affiliation)
        affiliation = re.sub(r'^Centro Internacional de Agricultura Tropical.+','International Center for Tropical Agriculture', affiliation)
        affiliation = re.sub(r'^CIAT.+','International Center for Tropical Agriculture', affiliation)

        affiliation = re.sub(r'^International Crops Research Institute for the Semi-Arid Tropics.+','International Crops Research Institute for the Semi-Arid Tropics', affiliation)
        affiliation = re.sub(r'^ICRISAT.+','International Crops Research Institute for the Semi-Arid Tropics', affiliation)

        affiliation = re.sub(r'^International Food Policy Research Institute.+','International Food Policy Research Institute', affiliation)
        affiliation = re.sub(r'^IFPRI.+','International Food Policy Research Institute', affiliation)

        affiliation = re.sub(r'^International Institute of Tropical Agriculture.+','International Institute of Tropical Agriculture', affiliation)
        affiliation = re.sub(r'^IITA.+','International Institute of Tropical Agriculture', affiliation)

        affiliation = re.sub(r'^International Livestock Research Institute.+','International Livestock Research Institute', affiliation)
        affiliation = re.sub(r'^International Livestock Research Centre.+','International Livestock Research Institute', affiliation)
        affiliation = re.sub(r'^ILRI.+','International Livestock Research Institute', affiliation)

        affiliation = re.sub(r'^International Maize and Wheat Improvement Cent(er|re).+','International Maize and Wheat Improvement Center', affiliation)
        affiliation = re.sub(r'^Centro Internacional de Mejoramiento de Ma(i|í)z y Trigo.+','International Maize and Wheat Improvement Center', affiliation)
        affiliation = re.sub(r'^CIMMYT.+','International Maize and Wheat Improvement Center', affiliation)

        affiliation = re.sub(r'^International Potato Cent(er|re).+','International Potato Center', affiliation)
        affiliation = re.sub(r'^Centro Internacional de la Papa.+','International Potato Center', affiliation)
        affiliation = re.sub(r'^CIP.+','International Potato Center', affiliation)

        affiliation = re.sub(r'^International Rice Research Institute.+','International Rice Research Institute', affiliation)
        affiliation = re.sub(r'^IRRI.+','International Rice Research Institute', affiliation)

        affiliation = re.sub(r'^International Water Management Institute.+','International Water Management Institute', affiliation)
        affiliation = re.sub(r'^IWMI.+','International Water Management Institute', affiliation)

        affiliation = re.sub(r'^World Agroforestry Cent(er|re).+','World Agroforestry', affiliation)
        affiliation = re.sub(r'^International Cent(er|re) for Research in Agroforestry.+','World Agroforestry', affiliation)
        affiliation = re.sub(r'^ICRAF.+','World Agroforestry', affiliation)

        affiliation = re.sub(r'^WorldFish.+','WorldFish', affiliation)

        if affiliation not in affiliations_normalized:
            affiliations_normalized.append(affiliation)

    return "; ".join(affiliations_normalized)
