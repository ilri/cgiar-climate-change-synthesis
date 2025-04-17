# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2025-04-17

- Updated release corresponding to v8 of the dataset:

> Orth, Alan; Bosire, Caroline K.; Rabago, Laura; Vaidya, Shrijana; Rajbhandari, Sitashma; Pradhan, Prajal; Mukherji, Aditi, 2024, "A Comprehensive Database of CGIAR Climate-Related Journal Articles (2012–2023)", https://hdl.handle.net/20.500.11766.1/FK2/Z98CZO, MELDATA, V8

### Changed

- Revise list of journal articles based on subsequent review
  - Total for dataset of original research articles: 2850→2813
    - Remove 17 with study areas in the global North
    - Remove 16 found to not be climate change or agri-food systems related
    - Remove 4 found to review, opinion, perspective, etc
  - Total for "combined" dataset (includes above, minus those that were not climate change or agri-food systems related): 3466→3450
    - Remove 16 found to not be climate change or agri-food systems related
- Remove "Original research" column for combined dataset because we can only determine that a journal article *was* original research (ie, included in the main review), not whether it *wasn't* (the combined dataset includes reviews, opinions, etc, as well as studies focusing on the global North)

### Updated

- Python dependencies

## [1.0.1] - 2025-01-22

- Updated release corresponding to v4 of the dataset:

> Orth, Alan; Bosire, Caroline K.; Rabago, Laura; Vaidya, Shrijana; Rajbhandari, Sitashma; Pradhan, Prajal; Mukherji, Aditi, 2024, "A Comprehensive Database of CGIAR Climate-Related Journal Articles (2012–2023)", https://hdl.handle.net/20.500.11766.1/FK2/Z98CZO, MELDATA, V4

### Changed

- Normalize countries to common short names instead of official names, drop unmatched countries, and de-duplicate list
- Remove "climate change" keyword since it is implied that all journal articles are climate change related and it congests tag clouds and topic lists
- Normalize publishers
- Use `pandas_convert` for country, region, and continent operations for a massive speed up

### Added

- Add column with UN regions (based on countries)
- Add column continents (based on countries)
- Retrieve missing publishers from Crossref
- Add column indicating original research (as opposed to review, synthesis, opinion, etc)
- Retrieve missing affiliations from OpenAlex (these are very messy currently, only normalizing CGIAR centers for now)
- Extract missing countries from titles and abstracts (best effort, still many missing)

## [1.0.0] - 2024-12-09

- Initial release corresponding to initial release of the dataset:

> Orth, Alan; Bosire, Caroline K.; Rabago, Laura; Vaidya, Shrijana; Rajbhandari, Sitashma; Pradhan, Prajal; Mukherji, Aditi, 2024, "A Comprehensive Database of CGIAR Climate-Related Journal Articles (2012–2023)", https://hdl.handle.net/20.500.11766.1/FK2/Z98CZO, MELDATA, V1
