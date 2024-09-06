# CGIAR Climate Change Synthesis

Code used to generate the dataset for the 2024 Synthesis of CGIAR work on climate change.


## Methodology

Search CGIAR institutional repositories to find items matching the following criteria:

- Issue date: 2012 to 2023
- Output type: Journal Article
- Language: English
- The words "climate change" in the title, subjects, or abstract

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

## TODO

- align with Rayyan column headers
- combine IFPRI title and subtitle?


## License
This work is licensed under the [GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html).

The license allows you to use and modify the work for personal and commercial purposes, but if you distribute the work you must provide users with a means to access the source code for the version you are distributing. Read more about the [GPLv3 at TL;DR Legal](https://tldrlegal.com/license/gnu-general-public-license-v3-(gpl-3)).
