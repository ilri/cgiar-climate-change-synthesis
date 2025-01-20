# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Normalize countries to common short names instead of official names, drop unmatched countries, and de-duplicate list
- Remove "climate change" keyword since it is implied that all journal articles are climate change related and it congests tag clouds and topic lists
- Normalize publishers
- Use `pandas_convert` in for country, region, and continent operations for a massive speed up

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
