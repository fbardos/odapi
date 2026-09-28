# Motivation

This API tries to simplify the live of downstream usage by Data Analysts by unifying
and transforming various data sources.
The data provided by this API serves one of the following two use cases:

- Collect Open Data from multiple authorities and merge them together.
- Provide easy access to data, where accessability is a problem.

The main focus lies in the merging of data from different authorities and therefore,
adressing the various challenges Data Producers and Data Users face.

## Challenges Data Producer

- Cannot change the data structure once published, because would break downstram usage.
- Different data maturity level and known tools.
- Some of this unifying data work gets done on a national level
  (Bundesamt für Statistik BFS), but this cannot be done for every data topic.
- In our federal world, one authority usually not combine datasets from other
  authorities. They focus on publishing their own data.

## Challenges Data User

Datasets published by these authorities have - once published - the priority on not
changing the data schema. Otherwise, downstream data usage would break.

When Data Analysts usually work with data, they face transformation problems:

- To combine multiple cantons and municipalities, multiple datasets need to be merged.
- To combine multiple years, multiple datasets need to be merged
  (one resource per year is a common observed pattern)
- The source datasets have differrent data structures
- The source datasets have different file types
- Source datasets have rate limiting, must be obtain with multiple API page calls
- Timestamps are shown in different timezones and time formats
- Spatial data is presented in different coordinate systems and shapes
- A primary key (PK) is unknown, a assumed PK contains duplicate rows
- Data changes over time, without the possibility to restore a previous state
- Every Data Analyst needs to obtain domain knowledge and understand the differences in
  the different datasets

## Where ODAPI steps in

This API tries to close this gap and adopts to different data structures of the
published datasets and changes over the time.

This way, a unified dataset can be published, combining multiple data sources from
different authorities on cantonal and municipal level.

We want to make it possible, that authorities can easily publish new datasets, while
we make the effort to combine them with other datasets from other authorities.
