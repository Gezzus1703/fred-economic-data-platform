create pipe if not exists FRED_DB.BRONZE.FRED_OBSERVATIONS_PIPE
    auto_ingest = true

as

copy into FRED_DB.BRONZE.FRED_OBSERVATIONS_RAW (
    raw_record,
    source_file,
    source_file_row_number,
    loaded_at
)

from (
    select
        $1,
        metadata$filename,
        metadata$file_row_number,
        current_timestamp()

    from @FRED_DB.BRONZE.FRED_S3_STAGE
)

file_format = (
    type = 'PARQUET'
)

pattern = '.*[.]parquet';