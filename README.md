# CGIAR Climate Change Synthesis Scripts [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14329330.svg)](https://doi.org/10.5281/zenodo.14329330)

Code used to generate datasets for the 2024 synthesis of CGIAR work on climate change.

Items matching the inclusion criteria were retrieved from eight CGIAR institutional repositories. This Python-based extract, transform, and load (ETL) pipeline filtered, merged, and normalized the metadata to ensure consistent use of date formats, multi-value separators, and identifiers. Naive deduplication was performed using titles and DOIs. Items identified to have been included erroneously due to incorrect repository metadata (mislabeled preprints, non-English, etc) were excluded.

We used Crossref, Unpaywall, and OpenAlex to fill in gaps for missing metadata such as usage (license) and access rights, affiliations, and publishers because this information can be valuable to researchers. Minor normalization was performed on affiliations, countries, and publishers, but all other metadata was used as-is from the respective repositories. Bibliographic metadata in the CSV output is oriented towards use with the [Rayyan platform](https://www.rayyan.ai/) for systematic literature review.

See:

> Orth, Alan; Bosire, Caroline K.; Rabago, Laura; Vaidya, Shrijana; Rajbhandari, Sitashma; Pradhan, Prajal; Mukherji, Aditi, 2024, "A Comprehensive Database of CGIAR Climate-Related Journal Articles (2012–2023)", https://hdl.handle.net/20.500.11766.1/FK2/Z98CZO, MELDATA, V4


## Methodology

Search CGIAR institutional repositories to find items matching the following criteria:

- **Issue date:** 2012 to 2023
- **Output type:** Journal Article
- **Language:** English
- The words "climate change" in the title, subjects, or abstract
- DOI assigned

Repository APIs were used to perform initial searches. Due to limitations in some APIs, further filtering was carried out to ensure items matched the basic inclusion criteria. See `src/update-sources.sh`.


## Data Sources

CGIAR institutional repositories used in this dataset (sorted by total number of records):

| Name        | URL                                        | Technology | Total Records |
|-------------|--------------------------------------------|------------|---------------|
| CGSpace     | https://cgspace.cgiar.org                  | DSpace 7   | 125,945       |
| CIFOR–ICRAF | https://data.cifor.org/dspace              | DSpace 5   | 35,317        |
| IRRI        | https://library.irri.org                   | Koha       | 26,696        |
| IFPRI       | https://ebrary.ifpri.org                   | CONTENTdm  | 24,975        |
| CIMMYT      | https://repository.cimmyt.org              | DSpace 7   | 18,437        |
| MELSpace    | https://repo.mel.cgiar.org                 | DSpace 7   | 13,055        |
| WorldFish   | https://digitalarchive.worldfishcenter.org | DSpace 6   | 5,673         |
| ICRISAT     | https://oar.icrisat.org                    | EPrints    | ?             |


## Requirements

- Python >= 3.9
- UNIX-like operating system


## Usage

This project is managed using [uv](https://docs.astral.sh/uv/). You will need to install that first, or use a vanilla Python virtual environment to install the dependencies:

```console
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

Once the dependencies are installed you can run the pipeline:

```console
$ ./src/merge_source_csvs.py
```

This will use pre-harvested data from the `data` directory, as the harvest process can take many hours (up to 1 day). To update sources, use the `src/update_sources.sh` script. Caches are used where possible to speed up repeated runs.


## License
This work is licensed under the [GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html).

The license allows you to use and modify the work for personal and commercial purposes, but if you distribute the work you must provide users with a means to access the source code for the version you are distributing. Read more about the [GPLv3 at TL;DR Legal](https://tldrlegal.com/license/gnu-general-public-license-v3-(gpl-3)).
