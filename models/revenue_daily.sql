select
    "date",
    sum(price_rub) as revenue_rub,
    {{ updated_at() }}
from {{ ref("trips_prep") }}
{% if is_incremental() %}
    where "date" >= (select max("date") - interval '2' day from {{ this }})
{% else %}
    where 1=1
{% endif %}
group by 
    "date", 
    now() at time zone 'utc'
order by "date"