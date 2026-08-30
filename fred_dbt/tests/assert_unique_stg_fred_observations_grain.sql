select
    series_id,
    observation_date,
    realtime_start,
    realtime_end,
    count(*) as total_rows
from {{ ref("stg_fred_observations") }}

group by
    series_id,
    observation_date,
    realtime_start,
    realtime_end
    
having
    total_rows > 1