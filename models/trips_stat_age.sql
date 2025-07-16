with date_age_cte as (
    select
        date(t1.started_at) as date,
        extract(year from t1.started_at) - extract(year from t2.birth_date) as age
    from scooters_raw.trips as t1
    inner join scooters_raw.users t2
        on t1.user_id = t2.id
),
count_cte as (
    select
        date,
        age,
        count(*) as trips
    from date_age_cte
    group by
        date,
        age
)
select
    age,
    avg(trips) as avg_trips
from count_cte
group by age
order by age
