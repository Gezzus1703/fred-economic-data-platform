select
    series_key,
    date_key,
    count(*) as total_rows
from {{ ref("fact_economic_observations") }}

group by
    series_key,
    date_key
    
having total_rows > 1