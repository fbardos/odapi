# Motivation

This API tries to simplify the live of downstream usage by Data Analysts by unifying
and transforming various data sources.
The data provided by this API serves one of the following two use cases:

- Collect Open Data from multiple authorities and merge them together.
- Provide easy access to data, where accessability is a problem.

The main focus lies in the merging of data from different authorities.
Datasets published by these authorities have - once published - the priority on not
changing the data schema. Otherwise, downstream data usage would break.

This API closes this gap and adopts to different data structures of the published
datasets and changes over the time.

This way, a unified dataset can be published, combining multiple data sources from
different authorities on cantonal and municipal level.

We want to make it possible, that authorities can easily publish new datasets, while
we make the effort to combine them with other datasets from other authorities.
