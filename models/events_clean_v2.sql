{% set date = var("date", none) %}
select distinct
    user_id,
    "timestamp",
    type_id,
    {{ updated_at() }},
    "date"
from
    {{ ref("events_prep") }}
where
{% if is_incremental() %}
    {% if date %}
        "date" = date '{{ date }}'
    {% else %}
        "date" > (select max("date") from {{ this }})
    {% endif %}
{% else %}
    "date" < date '2023-08-01'
{% endif %}