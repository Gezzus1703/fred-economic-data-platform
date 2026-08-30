{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[
            "series_id",
            "observation_date",
            "realtime_start",
            "realtime_end"
        ],
        on_schema_change="sync_all_columns"
    )
}}

with source_data as (

    select
        raw_record:series_id::varchar as series_id,
        try_to_date(raw_record:observation_date::varchar) as observation_date,
        try_to_decimal(
            nullif(raw_record:value::varchar, '.'),
            38,
            10
        ) as observation_value,
        try_to_date(raw_record:realtime_start::varchar) as realtime_start,
        try_to_date(raw_record:realtime_end::varchar) as realtime_end,
        try_to_timestamp_ntz(raw_record:ingested_at::varchar) as ingested_at,
        source_file,
        source_file_row_number,
        loaded_at

    from {{ source("bronze", "fred_observations_raw") }}

    {% if is_incremental() %}

        where loaded_at > (
            select coalesce(
                max(loaded_at),'1900-01-01'::timestamp_tz) 
            from {{ this }}
        )

    {% endif %}

),

deduplicated as (

    select
        *
    from source_data

    qualify row_number() over (
        partition by
            series_id,
            observation_date,
            realtime_start,
            realtime_end
        order by
            loaded_at desc,
            ingested_at desc
    ) = 1

)

select
    *
from deduplicated