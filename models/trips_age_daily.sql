with date_age_cte as (
    select
        t1.*,
        extract(year from t1.started_at) - extract(year from t2.birth_date) as age
    from {{ ref("trips_prep") }} as t1
    inner join {{ source("scooters_raw", "users") }} as t2
        on t1.user_id = t2.id
)
select
    "date",
    age,
    count(*) as trips,
    sum(price_rub) as revenue_rub
from date_age_cte
group by
    "date",
    age