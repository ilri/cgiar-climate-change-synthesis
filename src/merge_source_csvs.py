#!/usr/bin/env python3
#
# merge_source_csvs.py v0.0.1
#
# SPDX-License-Identifier: GPL-3.0-only
#

import logging
import warnings

import pandas as pd

from util import (
    deduplicate_subjects,
    get_access_rights,
    get_license,
    get_publication_date,
    normalize_doi,
    pdf_exists,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(message)s")

# Enable copy on write
pd.options.mode.copy_on_write = True

# Read all source CSVs into data frames. Use categorical dtype for some fields
# that have a limited number of values. Use the pyarrow dtype backend because
# pyarrow dtypes use significantly less memory than pandas default dtypes.
df_cgspace = pd.read_csv(
    "data/cgspace-filtered.csv",
    dtype={"Access rights": "category", "Usage rights": "category"},
    dtype_backend="pyarrow",
)
df_melspace = pd.read_csv(
    "data/melspace-filtered.csv",
    dtype={"Access rights": "category", "Usage rights": "category"},
    dtype_backend="pyarrow",
)
df_worldfish = pd.read_csv(
    "data/worldfish-filtered.csv",
    dtype={
        "cg.identifier.status": "category",
        "dc.rights": "category",
        "dc.date.issued": "string[pyarrow]",
    },
    usecols=[
        "dc.title",
        "dc.creator",
        "cg.contributor.affiliation",
        "dc.description.abstract",
        "cg.contributor.funder",
        "dc.date.issued",
        "dc.subject",
        "cg.subject.agrovoc",
        "dc.identifier.uri",
        "dc.identifier.doi",
        "cg.identifier.status",
        "dc.rights",
        "dc.source",
        "dc.identifier.issn",
        "dc.publisher",
    ],
    dtype_backend="pyarrow",
)
df_cifor = pd.read_csv(
    "data/cifor-filtered.csv",
    dtype={"cifor.type.oa": "category", "dc.rights": "category"},
    usecols=[
        "dc.title",
        "dc.contributor.author",
        "dc.date.issued",
        "dc.identifier.uri",
        "dc.identifier.doi",
        "dc.subject",
        "cg.subject.cifor",
        "cg.contributor.affiliation",
        "cg.contributor.center",
        "dc.description.abstract",
        "cg.contributor.donor",
        "cifor.source.title",
        "dc.identifier.issn",
        "cifor.source.volume",
        "cifor.source.numbers",
        "dc.publisher",
        "cifor.type.oa",
        "dc.rights",
        "cifor.source.page",
    ],
    dtype_backend="pyarrow",
)
df_ifpri = pd.read_csv(
    "data/ifpri-filtered.csv",
    dtype={
        "Access rights": "category",
        "Usage rights": "category",
        "Publication date": "string[pyarrow]",
    },
    usecols=[
        "Title",
        "Authors",
        "Publication date",
        "Journal",
        "Pages",
        "Publisher",
        "Abstract",
        "Funders",
        "ISSN",
        "DOI",
        "Subjects",
        "Access rights",
        "Usage rights",
        "Repository link",
    ],
    dtype_backend="pyarrow",
)
df_irri = pd.read_csv(
    "data/2023-10-16-IRRI-Climate-Change-fixed-filtered.csv",
    dtype={"date issued": "string[pyarrow]"},
    usecols=[
        "title",
        "issn",
        "first author",
        "other authors",
        "publisher",
        "journal",
        "issn",
        "date issued",
        "extent",
        "abstract",
        "subjects",
        "doi",
    ],
    dtype_backend="pyarrow",
)
df_icrisat = pd.read_csv(
    "data/icrisat-filtered.csv",
    dtype_backend="pyarrow",
)
df_cimmyt = pd.read_csv(
    "data/cimmyt-filtered.csv",
    dtype={"Publication date": "string[pyarrow]"},
    dtype_backend="pyarrow",
)

# Add source column
df_cgspace["Source"] = "CGSpace DSpace"
df_melspace["Source"] = "MELSpace DSpace"
df_worldfish["Source"] = "WorldFish DSpace"
df_cifor["Source"] = "CIFOR DSpace"
df_ifpri["Source"] = "IFPRI Library"
df_irri["Source"] = "IRRI Library"
df_icrisat["Source"] = "ICRISAT OAR"
df_cimmyt["Source"] = "CIMMYT DSpace"

# Concatenate subjects
df_worldfish["Subjects"] = (
    df_worldfish["dc.subject"]
    .fillna("MISSING DATA")
    .astype(str)
    .str.cat(
        df_worldfish["cg.subject.agrovoc"].fillna("MISSING DATA").astype(str), sep="; "
    )
)

df_worldfish["Subjects"] = df_worldfish["Subjects"].str.replace(
    r"((;\s)?MISSING DATA(;\s)?)", "", regex=True
)

df_worldfish = df_worldfish.rename(
    columns={
        "dc.title": "Title",
        "dc.creator": "Authors",
        "cg.contributor.affiliation": "Author affiliations",
        "dc.description.abstract": "Abstract",
        "cg.contributor.funder": "Funders",
        "dc.identifier.doi": "DOI",
        "cg.identifier.status": "Access rights",
        "dc.rights": "Usage rights",
        "dc.identifier.uri": "Repository link",
        "dc.date.issued": "Publication date",
        "dc.source": "Journal",
        "dc.identifier.issn": "ISSN",
        "dc.publisher": "Publisher",
    }
)

# Concatenate subjects. This is not very clever, but I can't figure out how to
# deal with missing data, so I fill it with a placeholder text I can replace.
df_cifor["dc.subject"] = (
    df_cifor["dc.subject"]
    .fillna("MISSING DATA")
    .astype(str)
    .str.cat(df_cifor["cg.subject.cifor"].fillna("MISSING DATA").astype(str), sep="; ")
)

# Ignore pandas warning about regex capture groups
warnings.simplefilter(action="ignore", category=UserWarning)

# Replace MISSING DATA
# See: https://regex101.com/r/JeGDid/1
df_cifor["dc.subject"] = df_cifor["dc.subject"].str.replace(
    r"((;\s)?MISSING DATA(;\s)?)", "", regex=True
)

# Concatenate affiliations
df_cifor["Author affiliations"] = (
    df_cifor["cg.contributor.affiliation"]
    .fillna("MISSING DATA")
    .astype(str)
    .str.cat(
        df_cifor["cg.contributor.center"].fillna("MISSING DATA").astype(str), sep="; "
    )
)

# Replace MISSING DATA
df_cifor["Author affiliations"] = df_cifor["Author affiliations"].str.replace(
    r"((;\s)?MISSING DATA(;\s)?)", "", regex=True
)

# Rename columns to match our biggest source CSV (CGSpace)
df_cifor = df_cifor.rename(
    columns={
        "dc.title": "Title",
        "dc.contributor.author": "Authors",
        "dc.description.abstract": "Abstract",
        "cg.contributor.donor": "Funders",
        "dc.identifier.doi": "DOI",
        "cifor.type.oa": "Access rights",
        "dc.rights": "Usage rights",
        "dc.identifier.uri": "Repository link",
        "dc.date.issued": "Publication date",
        "dc.subject": "Subjects",
        "cifor.source.title": "Journal",
        "dc.identifier.issn": "ISSN",
        "cifor.source.volume": "Volume",
        "cifor.source.numbers": "Issue",
        "dc.publisher": "Publisher",
        "cifor.source.page": "Pages",
    }
)

# Fix CIFOR handle links, since their Handle resolver is not working
df_cifor["Repository link"] = df_cifor["Repository link"].str.replace(
    r"^#", "https://data.cifor.org/dspace/handle", regex=True
)

# Concatenate authors since IRRI separates the first author and other authors
df_irri["Authors"] = (
    df_irri["first author"]
    .fillna("MISSING DATA")
    .astype(str)
    .str.cat(df_irri["other authors"].fillna("MISSING DATA").astype(str), sep="; ")
)

# Replace MISSING DATA
df_irri["Authors"] = df_irri["Authors"].str.replace(
    r"((;\s)?MISSING DATA(;\s)?)", "", regex=True
)

# Replace "backcrossing.;climatic change." → "backcrossing; climatic change"
df_irri["subjects"] = df_irri["subjects"].str.replace(r"\.?;", "; ", regex=True)

# Replace "spectroscopy." → "spectroscopy"
df_irri["subjects"] = df_irri["subjects"].str.replace(r"\.$", "", regex=True)

df_irri = df_irri.rename(
    columns={
        "title": "Title",
        "Authors": "Authors",
        "abstract": "Abstract",
        "doi": "DOI",
        "date issued": "Publication date",
        "journal": "Journal",
        "issn": "ISSN",
        "publisher": "Publisher",
        "subjects": "Subjects",
        "extent": "Pages",  # IRRI doesn't separate the pages
    }
)

logger.info("Merging sources...")

# Concatenate the data frames
# See: https://stackoverflow.com/a/48052579
df_final = pd.concat(
    [
        df_cgspace,
        df_melspace,
        df_worldfish,
        df_cifor,
        df_ifpri,
        df_irri,
        df_icrisat,
        df_cimmyt,
    ],
    join="outer",
)

# Normalize DOIs so we can deduplicate them
df_final["DOI"] = df_final["DOI"].apply(normalize_doi)

# Get licenses from Crossref because it's more reliable and standardized
df_final["Crossref"] = df_final["DOI"].apply(get_license)
# Fill in missing licenses from repository metadata
df_final["Usage rights"] = df_final["Crossref"].combine_first(df_final["Usage rights"])
df_final = df_final.drop("Crossref", axis="columns")
# Minor alignment for CIFOR licenses
df_final["Usage rights"] = df_final["Usage rights"].str.replace(
    "Attribution 4.0", "CC-BY-4.0"
)

# Get access rights from Unpaywall because it's more reliable and standardized
df_final["Unpaywall"] = df_final["DOI"].apply(get_access_rights)
# Fill in missing access rights from repository metadata
df_final["Access rights"] = df_final["Unpaywall"].combine_first(
    df_final["Access rights"]
)
df_final = df_final.drop("Unpaywall", axis="columns")
# Minor alignment for CIFOR and MELSpace access rights
df_final["Access rights"] = df_final["Access rights"].str.replace(
    "Closed access", "Limited Access"
)
df_final["Access rights"] = df_final["Access rights"].str.replace(
    "Gold open access", "Gold Open Access"
)
df_final["Access rights"] = df_final["Access rights"].str.replace(
    "Open access", "Open Access"
)

# Check how many rows we have total before deduplicating DOIs
total_number_records = df_final.shape[0]

# Remove duplicates using the DOI as the unique identifier. We need to use this
# instead of the much simpler drop_duplicates() because blanks are considered
# duplicates, which means we drop records that don't have DOIs!
# See: https://stackoverflow.com/questions/50154835/drop-duplicates-but-ignore-nulls
df_final = df_final[(~df_final["DOI"].duplicated()) | df_final["DOI"].isna()]

logger.info(f"Removed {total_number_records - df_final.shape[0]} duplicate DOIs")

# Check how many rows we have total before deduplicating titles
total_number_records = df_final.shape[0]

# Remove duplicates using the title as the unique identifier. This is just in
# case there are duplicate titles, as sometimes the same DOI can have a typo
# or differ in case, etc.
df_final = df_final.drop_duplicates(subset=["Title"], keep="first")

logger.info(f"Removed {total_number_records - df_final.shape[0]} duplicate titles")

###
# Normalize subjects
###

# Replace "spectroscopy;" → "spectroscopy"
df_final["Subjects"] = df_final["Subjects"].str.replace(r";\s?$", "", regex=True)

# Lower all subjects
df_final["Subjects"] = df_final["Subjects"].str.lower()

# Deduplicate subjects since we've merged various keyword and subject fields
df_final["Subjects"] = df_final["Subjects"].apply(deduplicate_subjects)

# Filter out some DOIs that we exclude from the set. For example preprints,
# book chapters, etc that have been miscataloged in a CGIAR repository).
df_dois_to_remove = pd.read_csv("data/dois-to-remove.csv")
df_final = df_final[~df_final["DOI"].isin(df_dois_to_remove["doi"])]

# Other URLs to remove
df_urls_to_remove = pd.read_csv("data/urls-to-remove.csv")
df_final = df_final[~df_final["Repository link"].isin(df_urls_to_remove["url"])]

removed = df_dois_to_remove.shape[0] + df_urls_to_remove.shape[0]
logger.info(f"Removed {removed} preprints, drafts, and book chapters")

# Write a record of items missing DOIs
df_final_missing_dois = df_final[~df_final["DOI"].str.startswith("10.", na=False)]
logger.info(
    f"Writing {df_final_missing_dois.shape[0]} records to /tmp/output-missing-dois.csv"
)
df_final_missing_dois.to_csv("/tmp/output-missing-dois.csv", index=False)

# Extract only items with DOIs, as per the inclusion criteria of the review
df_final = df_final[df_final["DOI"].str.startswith("10.", na=False)]

# Write all DOIs to text for debugging
df_final["DOI"].to_csv("/tmp/dois.txt", header=False, index=False)

# After dropping items without DOIs, check if we have the PDF
df_final["PDF"] = df_final["DOI"].apply(pdf_exists)

# Determine the publication date by getting the earlier of the issue date and
# the online date. The `axis=1` means we want to apply this function on each
# row instead of each column, so we can compare the item's dates.
df_final["Publication date"] = df_final.apply(get_publication_date, axis=1)

# Align headers with Rayyan
df_final = df_final.rename(
    columns={
        "Publication date": "Year",
        "Subjects": "Keywords",
    }
)

# Keep only the columns we want
df_final = df_final.filter(
    items=[
        "Title",
        "Authors",
        "Author affiliations",
        "Abstract",
        "Funders",
        "DOI",
        "Year",
        "Journal",
        "ISSN",
        "Volume",
        "Issue",
        "Pages",
        "Publisher",
        "Keywords",
        "Access rights",
        "Usage rights",
        "PDF",
        "Repository link",
        "Source",
    ]
)

# After normalizing to a simpler form and dropping duplicates, convert the DOIs
# back to URIs so they are easier to click in Excel or whatever
df_final["DOI"] = df_final["DOI"].str.replace(
    r"^10\.", "https://doi.org/10.", regex=True
)

# Import list of DOIs that were included in the review via Rayyan. Note that the
# DOIs here are in URI format so we don't have to convert them in a separate df.
df_dois_in_review = pd.read_csv("data/included-in-review.csv")
df_final_in_review = df_final[df_final["DOI"].isin(df_dois_in_review["doi"])]
logger.info(
    f"Writing {df_final_in_review.shape[0]} records to /tmp/output-used-in-review.csv"
)
df_final_in_review.to_csv("/tmp/output-used-in-review.csv", index=False)

# Write to a CSV without an index column
logger.info(f"Writing {df_final.shape[0]} records to /tmp/output.csv")
df_final.to_csv("/tmp/output.csv", index=False)

df_final_missing_pdfs = df_final[df_final["PDF"].isna()]
logger.info(
    f"Writing {df_final_missing_pdfs.shape[0]} records to /tmp/output-missing-pdfs.csv"
)
df_final_missing_pdfs.to_csv("/tmp/output-missing-pdfs.csv", index=False)
