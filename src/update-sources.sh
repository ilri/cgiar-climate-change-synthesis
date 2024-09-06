#!/usr/bin/env bash
#
# update-sources.sh v0.0.1
#
# SPDX-License-Identifier: GPL-3.0-only
#
# ---
#
# Script to harvest matching items from repositories to the extent that it is
# possible with APIs and perform secondary filtering of data sources to match
# the inclusion criteria.
#
# Note: repositories running DSpace 7 are the easiest to harvest because the
# API allows boolean searches directly, but the caveat is that the Solr/Lucene
# backend will perform stemming on search terms, returning items that don't
# exactly match the search terms. For example, if we search for "climate change"
# it will return items where an item has "climatic change". for the purposes of
# controlling inclusion criteria here I filter afterward using exact matches.
#

# Exit on first error
set -o errexit

[[ -z $VIRTUAL_ENV ]] && source .venv/bin/activate

echo "Updating CGSpace"

./scripts/harvest_cgspace.py

# Only need to filter for climate change here since our DSpace 7 harvesting
# script did some pre-processing.
csvgrep -c "Title,Subjects,Abstract" -r '[Cc]limate [Cc]hange' -a /tmp/cgspace.csv \
    | tee data/cgspace-filtered.csv \
    | xsv count

echo "Updating MELSpace"

./scripts/harvest_melspace.py

csvgrep -c "Title,Subjects,Abstract" -r '[Cc]limate [Cc]hange' -a /tmp/melspace.csv \
    | tee data/melspace-filtered.csv \
    | xsv count

echo "Updating WorldFish"

./scripts/harvest_dspace.py -r https://digitalarchive.worldfishcenter.org -f dc.title,dc.creator,cg.contributor.affiliation,dc.description.abstract,dc.date.issued,dc.subject,cg.subject.agrovoc,dc.identifier.uri,dc.identifier.doi,cg.identifier.status,dc.rights,cg.coverage.country,cg.coverage.region,dc.language,dc.source,dc.publisher,cg.contributor.funder,dc.type -o /tmp/worldfish.csv > /dev/null

csvgrep -c dc.date.issued -r '^(201[2-9]|202[0-3])' -a /tmp/worldfish.csv \
    | csvgrep -c dc.type -m 'Journal Article' \
    | csvgrep -c dc.title,dc.subject,cg.subject.agrovoc,dc.description.abstract -r '[Cc]limate [Cc]hange' -a \
    | csvgrep -c dc.language -r '[Ee]n' \
    | tee data/worldfish-filtered.csv \
    | xsv count

echo "Updating CIFOR"

./scripts/harvest_dspace.py -r https://data.cifor.org/dspace -f dc.title,dc.contributor.author,dc.type,dc.date.issued,dc.identifier.uri,dc.identifier.doi,dc.subject,cg.subject.cifor,cg.contributor.affiliation,cg.contributor.center,cg.contributor.funder,cg.coverage.region,cg.coverage.country,cifor.publication.status,dc.type.refereed,dc.description.abstract,cifor.source.title,dc.publisher,dc.type.isi,dc.language,dc.language.iso,cifor.type.oa,dc.rights,dc.format.extent,cifor.source.page -o /tmp/cifor.csv > /dev/null

csvgrep -c dc.date.issued -r '^(201[2-9]|202[0-3])' /tmp/cifor.csv \
    | csvgrep -c dc.type -m 'Journal Article' \
    | csvgrep -c dc.title,dc.subject,cg.subject.cifor,dc.description.abstract -r '[Cc]limate [Cc]hange' -a \
    | csvgrep -c dc.language.iso -m 'en' -a \
    | tee data/cifor-filtered.csv \
    | xsv count

echo "Updating CIMMYT"

./scripts/harvest_cimmyt.py

# Only need to filter for climate change here since our DSpace 7 harvesting
# script did some pre-processing.
csvgrep -c "Title,Subjects,Abstract" -r '[Cc]limate [Cc]hange' -a /tmp/cimmyt.csv \
    | tee data/cimmyt-filtered.csv \
    | xsv count

echo "Updating ICRISAT"

# JSON export from these search results:
# https://oar.icrisat.org/cgi/search/archive/advanced?order=-date%2Fcreators_name%2Ftitle&_action_search=Reorder&screen=Search&dataset=archive&exp=0%7C1%7C-date%2Fcreators_name%2Ftitle%7Carchive%7C-%7Cdate%3Adate%3AALL%3AEQ%3A2012-2023%7Ctype%3Atype%3AANY%3AEQ%3Aarticle%7C-%7Ceprint_status%3Aeprint_status%3AANY%3AEQ%3Aarchive%7Cmetadata_visibility%3Ametadata_visibility%3AANY%3AEQ%3Ashow

curl -s 'https://oar.icrisat.org/cgi/search/archive/advanced/export_icrisat_JSON.js?dataset=archive&screen=Search&_action_export=1&output=JSON&exp=0%7C1%7C-date%2Fcreators_name%2Ftitle%7Carchive%7C-%7Cdate%3Adate%3AALL%3AEQ%3A2012-2023%7Ctype%3Atype%3AANY%3AEQ%3Aarticle%7C-%7Ceprint_status%3Aeprint_status%3AANY%3AEQ%3Aarchive%7Cmetadata_visibility%3Ametadata_visibility%3AANY%3AEQ%3Ashow&n=&cache=742348' -o /tmp/icrisat.json

./scripts/icrisat_json_to_csv.py

echo "Updating IFPRI"

./scripts/harvest_ifpri.py

csvgrep -c "Publication date" -r '^(201[2-9]|202[0-3])' -a /tmp/ifpri.csv \
    | csvgrep -c Type -m 'Journal article' \
    | csvgrep -c Title,Subjects,Abstract -r '[Cc]limate [Cc]hange' -a \
    | csvgrep -c Language -m English \
    | tee data/ifpri-filtered.csv \
    | xsv count

# vim: set expandtab:ts=4:sw=4:bs=2
