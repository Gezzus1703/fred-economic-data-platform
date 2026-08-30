select
    series_id,
    observation_date,
    observation_value,
    realtime_start,
    realtime_end,
    ingested_at,
    source_file,
    source_file_row_number,
    loaded_at

from {{ ref("stg_fred_observations") }}

qualify row_number() over (
    partition by
        series_id,
        observation_date
    order by
        realtime_start desc,
        realtime_end desc,
        loaded_at desc
) = 1