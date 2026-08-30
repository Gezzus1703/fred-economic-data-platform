{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[
            "series_key",
            "date_key"
        ],
        on_schema_change="sync_all_columns"
    )
}}

with observations as (

    select *

    from {{ ref("int_fred_observations_latest") }}

    {% if is_incremental() %}

        where loaded_at > (
            select coalesce(
                max(loaded_at),
                '1900-01-01'::timestamp_tz
            )
            from {{ this }}
        )

    {% endif %}

)

select
    series.series_key,
    dates.date_key,
    observations.observation_value,
    observations.realtime_start,
    observations.realtime_end,
    observations.ingested_at,
    observations.loaded_at,
    observations.source_file,
    observations.source_file_row_number

from observations

inner join {{ ref("dim_series") }} as series
    on observations.series_id = series.series_id

inner join {{ ref("dim_date") }} as dates
    on observations.observation_date = dates.date_day