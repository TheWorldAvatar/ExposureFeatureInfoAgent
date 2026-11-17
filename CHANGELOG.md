# Changelog

## 2.6.0

### Added

- Demonstration dictionary metadata endpoint, `POST /dictionary/metadata`, with dry-run previews and dataset/configuration selection. Bundled configurations cover GGVA, greenspace, NDVI, and UTCI datasets.
- Dictionary variable codes in buffer labels when dataset metadata is available.

The Compose image tag remains `2.5.0` at this commit.

## 2.5.0

### Changed

- Updated query predicates for more efficient queries.

## 2.4.1

### Added

- Column names in calculation-method metadata.

## 2.4.0

### Added

- `/gvi/feature-info-agent/get` to group supplied green-view results by year and height.

## 2.3.3

### Fixed

- Added a missing `xsd` prefix to queries.

## 2.3.2

### Changed

- Made trip information optional for the timeline route.

## 2.3.1

### Changed

- Sorted result keys at all nesting levels and retained decimal precision when present in a quantity set.

## 2.3.0

### Added

- `/timeline/getDatasets` endpoint and usage documentation.

### Changed

- Used session IDs to separate timeline stays and consolidated trajectory metadata formatting.

## 2.2.0

Development tag `2.2.0-9-query-results-via-sql-SNAPSHOT` was introduced.

### Added

- SQL alternative to the feature-info route.
- Authenticated `/timeline` route.
- Dataset-label formatting.

### Changed

- Allowed more flexible timeline lower and upper bounds, following the trip-agent's design.

## 2.1.1

### Fixed

- Excluded calculations not found in Blazegraph when database tables and the knowledge graph differ.

## 2.1.0

### Added

- Dataset filtering support.

### Changed

- Removed federated queries for non-trajectory results to address performance issues.
- Made the `NAMESPACE` setting optional.

## 2.0.0

### Changed

- Preserved time values as strings to avoid formatting issues and used time values to distinguish stays where `trip=0`.
- Further clarified trip parameter documentation while the Compose tag remained `2.0.0`.

## 1.1.0

### Changed

- Retrieved trajectory exposure results through SPARQL instead of the time-series client.
- Supported trajectories with and without trip information.
- Clarified trip parameter documentation and added logging while the Compose tag remained `1.1.0`.

## 1.0.0

- Initial implementation
