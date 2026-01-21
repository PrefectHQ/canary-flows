select
    id,
    name,
    value,
    value * 2 as doubled_value
from {{ ref('canary_data') }}
