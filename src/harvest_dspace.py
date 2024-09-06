#!/usr/bin/env python3
#
# harvest_dspace.py v0.0.1
#
# SPDX-License-Identifier: GPL-3.0-only
#
# ---
#
# Generic harvester for DSpace 5/6 REST API.
#
# The DSpace 5 REST API is very limited. We can't use search, the /items endpoint
# doesn't indicate the current page, number of pages, etc. Better to just get all
# the repository's handles and save them in a CSV so we can filter them later.
#
# This script was tested on DSpace 5.4, 5.10 and 6.3.
#
import argparse
import csv
import logging
import re
import signal
import sys
from datetime import timedelta
from xml.dom import minidom

from packaging import version
from requests_cache import CachedSession
from tqdm import tqdm

from util import detect_dspace_version

# Create a local logger instance
logger = logging.getLogger(__name__)

session = CachedSession(
    "requests-cache", expire_after=timedelta(days=30), allowable_codes=(200, 404)
)

# prune old cache entries
session.cache.delete(expired=True)


def signal_handler(signal, frame):
    # close output file before we exit
    args.output_file.close()

    sys.exit(1)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Harvest items from a DSpace repository using the legacy (DSpace 5/6) REST API."
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print debug messages to standard error (stderr).",
        action="store_true",
    )
    parser.add_argument(
        "-f",
        "--fields",
        help="Comma-separated list of fields to include in export, for example: dc.contributor.author,dcterms.bibliographicCitation",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        help="File to write results to (CSV).",
        required=True,
        type=argparse.FileType("w", encoding="UTF-8"),
    )
    parser.add_argument(
        "-r",
        "--dspace-root",
        required=True,
        help="URL to DSpace root.",
    )
    args = parser.parse_args()

    return args


# Returns a list of dicts
def get_item_metadata(dspace_rest_api: str, handle: str) -> list:
    r = session.get(f"{dspace_rest_api}/handle/{handle}", params={"expand": "metadata"})

    return r.json()["metadata"]


# For DSpace 5.10+ we retrieve all the Handles from the sitemap and then request
# them one by one from the /rest/handle endpoint.
def parse_sitemap(dspace_sitemap: str, dspace_rest_api: str):
    # Write the CSV header based on the user's list of metadata fields
    writer = csv.DictWriter(args.output_file, fieldnames=args.fields.split(","))
    writer.writeheader()

    # Get the main XML sitemap
    r = session.get(dspace_sitemap)
    sitemap_root = minidom.parseString(r.content)

    # Get the locations of the child sitemaps, ie:
    #
    #   <sitemapindex>
    #     <sitemap>
    #       <loc>https://data.cifor.org/dspace/xmlui/sitemap?map=0</loc>
    #       <lastmod>2023-10-08T17:00:51Z</lastmod>
    #     </sitemap>
    #   </sitemapindex>
    #
    # There can be more than one sitemap, as they are split at about 50,000 items
    for sitemap in sitemap_root.getElementsByTagName("loc"):
        sitemap_loc = sitemap.firstChild.nodeValue
        sitemap_loc_root = minidom.parseString(session.get(sitemap_loc).content)

        # Get the URLs of handles in this child sitemap, splitting the URL
        # so we can get just the handle component for each item, ie:
        #
        #   ['https://dspacetest.cgiar.org/', '10568/43178']
        #
        handles = [
            handle_loc.firstChild.nodeValue.split("handle/")[1]
            for handle_loc in sitemap_loc_root.getElementsByTagName("loc")
        ]

        for handle in tqdm(handles, desc="Harvesting"):
            logger.debug(f"Looking up {handle}")
            # Relying on undocumented "handle" REST API endpoint
            r = session.get(f"{dspace_rest_api}/handle/{handle}")

            # Skip this handle if status is not OK. Could be a restricted
            # community, collection, or item.
            if not r.ok:
                logger.debug(f"> Skipping {r.url} ({r.status_code})")

                continue

            if not r.json()["type"] == "item":
                logger.debug(f"> Skipping {r.json()['type']}")

                continue

            item_metadata_json = get_item_metadata(dspace_rest_api, handle)

            # Initialize empty dict
            row = dict()

            # Iterate over user-specified fields and extract matching metadata
            # from the item. Join multiple values with "; ".
            for field in args.fields.split(","):
                metadatum = [
                    metadatum["value"]
                    for metadatum in item_metadata_json
                    if metadatum["key"] == field
                ]
                row[field] = "; ".join(metadatum)

            writer.writerow(row)


# DSpace 5.4's REST API has numerous problems:
#   - It does not have the /rest/handle endpoint
#   - It does not indicate total number of items or pages
#   - Order of items is non-deterministic and can have pages with no items
#
# We must blindly iterate over all items and guess when we are done.
def iterate_items(dspace_root: str, dspace_rest_api: str):
    # Write the CSV header based on the user's list of metadata fields
    writer = csv.DictWriter(args.output_file, fieldnames=args.fields.split(","))
    writer.writeheader()

    r = session.get(f"{dspace_root}/discover", headers={"Accept-Language": "en"})

    if not r.ok:
        logger.error(f"> Cannot estimate number of items: {r.url}")

        sys.exit(1)

    pagination = re.findall(
        r'<p class="pagination-info">Now showing items 1-10 of \d+</p>', r.text
    )[0]
    discover_results = re.search(r"\d{3,}", pagination).group(0)
    # round the Discover results up to the nearest 1000
    num_items_estimate = round(int(discover_results), -3)

    for offset in range(0, num_items_estimate):
        params = {"limit": 1, "offset": offset}
        logger.info(f"Checking page {offset}")

        r = session.get(f"{dspace_rest_api}/items", params=params)

        if not r.ok:
            logger.debug(f"> Skipping {r.url} ({r.status_code})")

            continue

        # Some pages are blank :)
        try:
            handle = r.json()[0]["handle"]
        except IndexError:
            logger.debug(f"> Skipping empty {r.url}")

            continue

        item_id = r.json()[0]["id"]

        logger.info(f"> Looking up {handle} (id: {item_id})")
        r = session.get(
            f"{dspace_rest_api}/items/{item_id}", params={"expand": "metadata"}
        )

        if not r.ok:
            logger.debug(f"> Skipping {r.url} ({r.status_code})")

            continue

        item_metadata_json = r.json()["metadata"]

        # Initialize empty dict
        row = dict()

        # Iterate over user-specified fields and extract matching metadata
        # from the item. Join multiple values with "; ".
        for field in args.fields.split(","):
            metadatum = [
                metadatum["value"]
                for metadatum in item_metadata_json
                if metadatum["key"] == field
            ]
            row[field] = "; ".join(metadatum)

        writer.writerow(row)


def main(args):
    dspace_root = args.dspace_root
    dspace_sitemap = f"{dspace_root}/sitemap"
    dspace_rest_api = f"{dspace_root}/rest"

    dspace_version = detect_dspace_version(dspace_root)

    # Trim -SNAPSHOT if it exists because Python's version parse doesn't support
    dspace_version = dspace_version.strip("-SNAPSHOT")

    # DSpace 5.10 has the /rest/handle endpoint
    if version.parse(dspace_version) >= version.parse("5.10"):
        parse_sitemap(dspace_sitemap, dspace_rest_api)
    else:
        iterate_items(dspace_root, dspace_rest_api)


def signal_handler(signal, frame):
    sys.exit(1)


if __name__ == "__main__":
    args = parse_arguments()

    # The default log level is WARNING, but we want to set it to DEBUG or INFO
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Set the global log format since we are running interactively
    logging.basicConfig(format="[%(levelname)s] %(message)s")

    # set the signal handler for SIGINT (^C) so we can exit cleanly
    signal.signal(signal.SIGINT, signal_handler)

    main(args)
