# Delta Tables Extractor

This component extracts data from Delta tables stored either in your cloud storage (AWS S3, Azure Blob Storage, Google Cloud Storage) or in Unity Catalog and loads it into Keboola.

Component supports downloading entire tables, selecting specific columns, or executing custom SQL queries to retrieve the desired data.

When loading data as Keboola Table the Full Load, Incremental Load, and Append Load modes are supported. You can also choose to store the extracted data as Parquet files in the file storage.

Complete documentation is available in the [README](https://github.com/keboola/component-delta-lake-extractor/blob/main/README.md)