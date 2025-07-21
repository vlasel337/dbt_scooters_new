select
    t1.*,
    t2.sex,
    extract(year from t1.started_at) - extract(year from t2.birth_date) as age
from {{ ref("trips_prep") }} t1
left join {{ source("scooters_raw","users")}} t2
    on t1.user_id = t2.id
{% if is_incremental() %}
    where t1.id > (select max(id) from {{ this }})
    order by t1.id
    limit 75000
{% else %}
    where t1.id <= 75000
{% endif %}
