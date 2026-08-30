select
    md5(series_id) as series_key,
    series_id
from {{ ref("int_fred_observations_latest") }}
group by
    series_id